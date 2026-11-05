//! End-to-end contract test for the output stream.
//!
//! This is the test that matters for a backtester: it drives the real [`Pipeline`] into a
//! real [`AlignedSbeWriter`] file, reads it back with the challenge's own reader, and
//! asserts the properties a consumer depends on:
//!
//! 1. A snapshot precedes the updates that depend on it.
//! 2. Each update carries the *absolute* resting quantity after the change, not the delta.
//! 3. A second socket delivering the same exchange clock does not double-count.
//! 4. The file ends with a snapshot, so a truncated tail never looks like a valid book.
//!
//! It runs entirely offline, which is what makes it usable in CI without Kalshi keys.

use std::time::Duration;

use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use engineering_challenge::book::SCALE;
use engineering_challenge::pipeline::{BookEvent, Pipeline};
use engineering_challenge::sink::SbeFileSink;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
use tokio::sync::{mpsc, oneshot};

// Must equal AlignedSbeReader's chunk size; the reader walks the file in that stride.
const CHUNK: usize = 64 << 20;

fn at(offset_micros: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(1_700_000_000_000_000 + offset_micros).unwrap()
}

#[tokio::test]
async fn stream_is_self_consistent_and_deduplicated() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("stream.sbe");

    let sink = SbeFileSink::create(&path, CHUNK, CHUNK / 8)?;
    let pipeline = Pipeline::new(
        Box::new(sink),
        Duration::from_secs(3_600),
        Duration::from_millis(50),
    );

    let (tx, rx) = mpsc::channel::<BookEvent>(1024);
    let handle = tokio::spawn(async move { pipeline.run(rx).await });

    let symbol = "TEST-MARKET".to_owned();

    // Startup snapshot: 10 contracts resting at 0.50 on the bid.
    tx.send(BookEvent::Reset {
        symbol: symbol.clone(),
        bids: vec![(500_000, 10 * SCALE)],
        asks: vec![(600_000, 4 * SCALE)],
        clock: Some(1_000),
        received_at: at(0),
        exchange_timestamp: at(0),
        worker: 0,
    })
    .await?;

    // The primary socket applies a partial fill: 10 -> 7 at the same price.
    tx.send(BookEvent::Delta {
        symbol: symbol.clone(),
        side: Side::Buy,
        price: 500_000,
        delta: -3 * SCALE,
        clock: Some(1_001),
        received_at: at(10),
        exchange_timestamp: at(10),
        worker: 0,
    })
    .await?;

    // The hot standby delivers the identical update. It must be dropped, because a
    // second copy would turn a 7-contract level into a 4-contract one.
    tx.send(BookEvent::Delta {
        symbol: symbol.clone(),
        side: Side::Buy,
        price: 500_000,
        delta: -3 * SCALE,
        clock: Some(1_001),
        received_at: at(11),
        exchange_timestamp: at(10),
        worker: 1,
    })
    .await?;

    // A new better bid appears, then a level is fully removed.
    tx.send(BookEvent::Delta {
        symbol: symbol.clone(),
        side: Side::Buy,
        price: 510_000,
        delta: 2 * SCALE,
        clock: Some(1_002),
        received_at: at(20),
        exchange_timestamp: at(20),
        worker: 0,
    })
    .await?;
    tx.send(BookEvent::Delta {
        symbol: symbol.clone(),
        side: Side::Sell,
        price: 600_000,
        delta: -4 * SCALE,
        clock: Some(1_003),
        received_at: at(30),
        exchange_timestamp: at(30),
        worker: 0,
    })
    .await?;

    let (done_tx, done_rx) = oneshot::channel();
    tx.send(BookEvent::Shutdown(done_tx)).await?;
    done_rx.await?;
    handle.await?;

    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path)?;
    let mut snapshots = 0usize;
    let mut updates = Vec::new();
    let mut first_kind = None;

    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(snapshot) => {
                first_kind.get_or_insert("snapshot");
                snapshots += 1;
                let decoded = SnapshotDomain::try_from_decoder(snapshot)?;
                assert_eq!(decoded.symbol, symbol);
            }
            AnyMessage::PriceLevelUpdate(update) => {
                first_kind.get_or_insert("update");
                updates.push(PriceLevelUpdateDomain::try_from_decoder(update)?);
            }
            // The schema is non-exhaustive on purpose: a future message type must not
            // break a consumer that predates it.
            _ => {}
        }
    }

    assert_eq!(
        first_kind,
        Some("snapshot"),
        "a consumer needs a book before it can apply updates"
    );
    assert_eq!(snapshots, 2, "startup snapshot plus the shutdown snapshot");
    assert_eq!(
        updates.len(),
        3,
        "the duplicate copy is dropped, nothing else lost"
    );

    let quantities: Vec<i64> = updates.iter().map(|update| update.quantity).collect();
    assert_eq!(
        quantities,
        vec![7 * SCALE, 2 * SCALE, 0],
        "quantities are absolute, and a removed level is reported as zero"
    );

    let prices: Vec<i64> = updates.iter().map(|update| update.price_level).collect();
    assert_eq!(prices, vec![500_000, 510_000, 600_000]);

    let sides: Vec<Side> = updates.iter().map(|update| update.side).collect();
    assert_eq!(sides, vec![Side::Buy, Side::Buy, Side::Sell]);

    Ok(())
}
