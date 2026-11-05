//! Proves the Polymarket adapter feeds the same pipeline and produces the same kind of
//! stream as the Kalshi path.
//!
//! This is the payoff of Extension 3: nothing below the adapter changes. The test drives
//! real Polymarket frames through [`polymarket::decode`] into the shared [`Pipeline`] and
//! reads the SBE file back with the challenge's own reader.
//!
//! The specific trap it guards against: Polymarket publishes the *resting size* at a
//! level, so two updates of 300 then 200 mean "300, then 200", not "300, then 500".

use std::time::Duration;

use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use engineering_challenge::book::SCALE;
use engineering_challenge::pipeline::{BookEvent, Pipeline};
use engineering_challenge::polymarket;
use engineering_challenge::sink::SbeFileSink;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
use tokio::sync::{mpsc, oneshot};

const CHUNK: usize = 64 << 20;
const ASSET: &str =
    "107505882767731489358349912513945399560393482969656700824895970500493757150417";

fn at() -> DateTime<Utc> {
    DateTime::from_timestamp_micros(1_782_753_357_000_000).unwrap()
}

#[tokio::test]
async fn polymarket_frames_stream_through_the_shared_pipeline() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("polymarket.sbe");

    let sink = SbeFileSink::create(&path, CHUNK, CHUNK / 8)?;
    let pipeline = Pipeline::new(
        Box::new(sink),
        Duration::from_secs(3_600),
        Duration::from_millis(50),
    );
    let (tx, rx) = mpsc::channel::<BookEvent>(1024);
    let handle = tokio::spawn(async move { pipeline.run(rx).await });

    // Frames as Polymarket actually sends them, in order.
    let frames = [
        r#"{
            "event_type": "book",
            "asset_id": "107505882767731489358349912513945399560393482969656700824895970500493757150417",
            "bids": [{"price": "0.48", "size": "100"}],
            "asks": [{"price": "0.52", "size": "250"}],
            "timestamp": "1782753357257"
        }"#,
        r#"{
            "event_type": "price_change",
            "asset_id": "107505882767731489358349912513945399560393482969656700824895970500493757150417",
            "changes": [{"price": "0.48", "size": "300", "side": "BUY"}],
            "timestamp": "1782753357260"
        }"#,
        r#"{
            "event_type": "price_change",
            "asset_id": "107505882767731489358349912513945399560393482969656700824895970500493757150417",
            "changes": [{"price": "0.48", "size": "200", "side": "BUY"}],
            "timestamp": "1782753357261"
        }"#,
        // Removing the ask entirely.
        r#"{
            "event_type": "price_change",
            "asset_id": "10750588276773148935834991251394539956039396869656700824985970500493757150417",
            "changes": []
        }"#,
        r#"{
            "event_type": "price_change",
            "asset_id": "107505882767731489358349912513945399560393482969656700824895970500493757150417",
            "changes": [{"price": "0.52", "size": "0", "side": "SELL"}],
            "timestamp": "1782753357262"
        }"#,
        // Ignored by the adapter, and must not disturb the stream.
        r#"{"event_type": "last_trade_price", "asset_id": "x", "price": "0.50", "size": "5"}"#,
        // A duplicate of the last real change from a second socket.
    ];

    for text in frames {
        for event in polymarket::to_events(&polymarket::decode(text)?, 0, at()) {
            tx.send(event).await?;
        }
    }
    // Redundant socket replaying the same exchange clock for the same level.
    let duplicate = r#"{
        "event_type": "price_change",
        "asset_id": "107505882767731489358349912513945399560393482969656700824895970500493757150417",
        "changes": [{"price": "0.52", "size": "0", "side": "SELL"}],
        "timestamp": "1782753357262"
    }"#;
    for event in polymarket::to_events(&polymarket::decode(duplicate)?, 1, at()) {
        tx.send(event).await?;
    }

    let (done_tx, done_rx) = oneshot::channel();
    tx.send(BookEvent::Shutdown(done_tx)).await?;
    done_rx.await?;
    handle.await?;

    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path)?;
    let mut snapshots = 0;
    let mut updates = Vec::new();
    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(snapshot) => {
                let snapshot = SnapshotDomain::try_from_decoder(snapshot)?;
                assert_eq!(snapshot.symbol, ASSET);
                snapshots += 1;
            }
            AnyMessage::PriceLevelUpdate(update) => {
                updates.push(PriceLevelUpdateDomain::try_from_decoder(update)?);
            }
            _ => {}
        }
    }

    assert_eq!(snapshots, 2, "startup snapshot plus the shutdown snapshot");

    let quantities: Vec<i64> = updates.iter().map(|update| update.quantity).collect();
    assert_eq!(
        quantities,
        vec![300 * SCALE, 200 * SCALE, 0],
        "resting size is reported as it stands, never accumulated"
    );

    let sides: Vec<Side> = updates.iter().map(|update| update.side).collect();
    assert_eq!(sides, vec![Side::Buy, Side::Buy, Side::Sell]);

    let prices: Vec<i64> = updates.iter().map(|update| update.price_level).collect();
    assert_eq!(prices, vec![480_000, 480_000, 520_000]);

    Ok(())
}
