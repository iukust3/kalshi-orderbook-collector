//! Polymarket market-stream adapter.
//!
//! Extension 3 asks for the exchange-specific part to be isolated so the rest of the
//! service does not change. This module is that boundary: it turns Polymarket frames
//! into the same [`BookEvent`]s the Kalshi feed produces, so the pipeline, the redundancy
//! logic, the snapshot cadence and the SBE writer are shared unchanged.
//!
//! Two Polymarket behaviours force real differences, and both are handled here rather
//! than leaking upstream:
//!
//! * **It publishes resting size, not a signed change.** `price_change` carries the size
//!   now at that level, with `0` meaning removed. Converting that to a delta would need
//!   the current book, which lives in the pipeline, so the adapter emits
//!   [`BookEvent::SetLevel`] and the book sets the level instead of adding to it.
//! * **It has two envelope shapes in the wild.** The market stream is documented with a
//!   flat `{event_type, asset_id, ...}` shape and also with the newer
//!   `{topic, type, payload}` shape used by the unified SDK. Both are decoded here,
//!   because a collector that dies on the other shape is a collector that silently stops.
//!
//! Prices and sizes arrive as decimal strings (`"0.08"`, `"33343.4"`); they are parsed
//! into the same 1e-6 fixed point as everything else. No floating point touches money.

use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::Value;
use tracing::warn;

use crate::Side;
use crate::kalshi::protocol::parse_fixed_6;
use crate::pipeline::{BookEvent, WorkerId};

/// One decoded frame, normalised across both envelope shapes.
#[derive(Debug, Clone, PartialEq)]
pub enum Normalized {
    /// Full book for one asset. Authoritative for everything at or below `timestamp`.
    Book {
        asset: String,
        bids: Vec<(i64, i64)>,
        asks: Vec<(i64, i64)>,
        timestamp_ms: Option<i64>,
    },
    /// Per-level absolute sizes for one or more assets.
    Changes {
        changes: Vec<LevelChange>,
        timestamp_ms: Option<i64>,
    },
    /// The market is finished; its book is no longer meaningful.
    Resolved { asset: String },
    /// A market opened. The caller decides whether to subscribe.
    NewMarket { assets: Vec<String> },
    /// Known but nothing to apply (last trade price, tick size, best bid/ask).
    Ignored,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LevelChange {
    pub asset: String,
    pub side: Side,
    pub price: i64,
    pub size: i64,
}

#[derive(Debug, Deserialize)]
struct RawFrame {
    event_type: String,
    #[serde(default)]
    msg: Option<Value>,
    #[serde(flatten)]
    rest: Value,
}

/// Decodes one websocket text frame. Unknown types return [`Normalized::Ignored`] rather
/// than an error: the market stream is additive, and refusing an unknown frame would turn
/// a new event type into an outage.
pub fn decode(text: &str) -> anyhow::Result<Vec<Normalized>> {
    let value: Value = serde_json::from_str(text)?;

    // The market stream also sends a bare array of book frames on subscribe.
    if let Some(array) = value.as_array() {
        let mut out = Vec::with_capacity(array.len());
        for item in array {
            out.push(decode_value(item)?);
        }
        return Ok(out);
    }
    Ok(vec![decode_value(&value)?])
}

fn decode_value(value: &Value) -> anyhow::Result<Normalized> {
    // Newer envelope: {topic, type, payload}.
    if value.get("topic").is_some() {
        let kind = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let payload = value.get("payload").cloned().unwrap_or(Value::Null);
        return decode_kind(&kind, &payload);
    }

    // Flat envelope: {event_type, ...}.
    let frame: RawFrame = serde_json::from_value(value.clone())?;
    let merged = match frame.msg {
        Some(msg) => {
            let mut base = frame.rest.clone();
            if let (Some(base), Some(msg)) = (base.as_object_mut(), msg.as_object()) {
                for (key, item) in msg {
                    base.insert(key.clone(), item.clone());
                }
            }
            base
        }
        None => frame.rest.clone(),
    };
    decode_kind(&frame.event_type, &merged)
}

fn decode_kind(kind: &str, payload: &Value) -> anyhow::Result<Normalized> {
    match kind {
        "book" => Ok(decode_book(payload)),
        "price_change" => Ok(Normalized::Changes {
            changes: decode_changes(payload),
            timestamp_ms: timestamp_of(payload),
        }),
        "market_resolved" => Ok(Normalized::Resolved {
            asset: winning_asset(payload),
        }),
        "new_market" => Ok(Normalized::NewMarket {
            // Both spellings appear: the SDK envelope uses `assetIds`, the flat frame
            // uses `asset_ids`, and older frames carry `clobTokenIds`.
            assets: string_list(
                payload
                    .get("assetIds")
                    .or_else(|| payload.get("asset_ids"))
                    .or_else(|| payload.get("clobTokenIds")),
            ),
        }),
        // last_trade_price, tick_size_change, best_bid_ask, pong, and anything newer.
        _ => Ok(Normalized::Ignored),
    }
}

fn decode_book(payload: &Value) -> Normalized {
    // In the SDK envelope one book frame covers one asset; the flat envelope identifies
    // it as `asset_id`.
    let asset = asset_of(payload).unwrap_or_default();
    Normalized::Book {
        asset,
        bids: levels(payload.get("bids")),
        asks: levels(payload.get("asks")),
        timestamp_ms: timestamp_of(payload),
    }
}

fn decode_changes(payload: &Value) -> Vec<LevelChange> {
    let default_asset = asset_of(payload);
    let items = payload
        .get("priceChanges")
        .or_else(|| payload.get("changes"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let Some(price) = item
            .get("price")
            .and_then(Value::as_str)
            .and_then(parse_fixed_6)
        else {
            warn!(?item, "polymarket: unparsable price in price_change");
            continue;
        };
        let size = item
            .get("size")
            .and_then(Value::as_str)
            .and_then(parse_fixed_6)
            .unwrap_or(0);
        let side = match item
            .get("side")
            .and_then(Value::as_str)
            .unwrap_or("BUY")
            .to_ascii_uppercase()
            .as_str()
        {
            "SELL" => Side::Sell,
            _ => Side::Buy,
        };
        let asset = asset_of(&item).or_else(|| default_asset.clone());
        let Some(asset) = asset else {
            warn!("polymarket: price_change without an asset id");
            continue;
        };
        out.push(LevelChange {
            asset,
            side,
            price,
            size,
        });
    }
    out
}

fn levels(value: Option<&Value>) -> Vec<(i64, i64)> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|level| {
                    let price = level.get("price")?.as_str().and_then(parse_fixed_6)?;
                    let size = level.get("size")?.as_str().and_then(parse_fixed_6)?;
                    Some((price, size))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn asset_of(value: &Value) -> Option<String> {
    value
        .get("asset_id")
        .or_else(|| value.get("assetId"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn winning_asset(payload: &Value) -> String {
    payload
        .get("winningAssetId")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| asset_of(payload))
        .unwrap_or_default()
}

fn string_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

/// Polymarket sends millisecond timestamps as decimal strings.
fn timestamp_of(payload: &Value) -> Option<i64> {
    match payload.get("timestamp") {
        Some(Value::String(text)) => text.parse::<i64>().ok(),
        Some(Value::Number(number)) => number.as_i64(),
        _ => None,
    }
}

/// Converts decoded frames into pipeline events.
///
/// `worker` stamps the events for the cross-socket dedupe rule, and `received_at` is the
/// local instant, kept separate from the exchange timestamp so observable latency stays
/// measurable downstream.
pub fn to_events(
    frames: &[Normalized],
    worker: WorkerId,
    received_at: DateTime<Utc>,
) -> Vec<BookEvent> {
    let mut events = Vec::new();
    for frame in frames {
        match frame {
            Normalized::Book {
                asset,
                bids,
                asks,
                timestamp_ms,
            } => {
                if asset.is_empty() {
                    continue;
                }
                events.push(BookEvent::Reset {
                    symbol: asset.clone(),
                    bids: bids.clone(),
                    asks: asks.clone(),
                    clock: clock_of(*timestamp_ms),
                    received_at,
                    exchange_timestamp: exchange_time(*timestamp_ms, received_at),
                    worker,
                });
            }
            Normalized::Changes {
                changes,
                timestamp_ms,
            } => {
                for change in changes {
                    events.push(BookEvent::SetLevel {
                        symbol: change.asset.clone(),
                        side: change.side,
                        price: change.price,
                        quantity: change.size,
                        clock: clock_of(*timestamp_ms),
                        received_at,
                        exchange_timestamp: exchange_time(*timestamp_ms, received_at),
                        worker,
                    });
                }
            }
            Normalized::Resolved { asset } => {
                if !asset.is_empty() {
                    events.push(BookEvent::Drop {
                        symbol: asset.clone(),
                    });
                }
            }
            Normalized::NewMarket { assets } => {
                // Nothing to apply: a new market's book arrives as its own `book` frame
                // once subscribed. Surfaced so the caller can decide to subscribe.
                let _ = assets;
            }
            Normalized::Ignored => {}
        }
    }
    events
}

fn clock_of(timestamp_ms: Option<i64>) -> Option<u64> {
    timestamp_ms.and_then(|value| u64::try_from(value).ok())
}

fn exchange_time(timestamp_ms: Option<i64>, fallback: DateTime<Utc>) -> DateTime<Utc> {
    timestamp_ms
        .and_then(|millis| Utc.timestamp_millis_opt(millis).single())
        .unwrap_or(fallback)
}

/// Subscription frame for a set of asset ids.
pub fn subscribe_frame(assets: &[String]) -> String {
    serde_json::json!({ "assets_ids": assets, "type": "market" }).to_string()
}

/// Polymarket keeps a connection alive with a literal `PING` frame.
pub fn heartbeat_frame() -> &'static str {
    "PING"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::SCALE;

    fn at() -> DateTime<Utc> {
        DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap()
    }

    #[test]
    fn flat_book_frame_becomes_a_reset() {
        let text = r#"{
            "event_type": "book",
            "asset_id": "107505882767731489358349912513945399560393482969656700824895970500493757150417",
            "bids": [{"price": "0.08", "size": "33343.4"}],
            "asks": [{"price": "0.09", "size": "163939.58"}],
            "timestamp": "1782753357257"
        }"#;
        let frames = decode(text).unwrap();
        let events = to_events(&frames, 2, at());
        assert_eq!(events.len(), 1);
        match &events[0] {
            BookEvent::Reset {
                bids,
                asks,
                clock,
                symbol,
                ..
            } => {
                assert_eq!(
                    symbol,
                    "107505882767731489358349912513945399560393482969656700824895970500493757150417"
                );
                assert_eq!(bids[0], (80_000, 33_343_400_000));
                assert_eq!(asks[0], (90_000, 163_939_580_000));
                assert_eq!(*clock, Some(1_782_753_357_257));
            }
            other => panic!("expected a reset, got {other:?}"),
        }
    }

    #[test]
    fn sdk_envelope_is_decoded_too() {
        let text = r#"{
            "topic": "market",
            "type": "book",
            "payload": {
                "conditionId": "0x747dc809",
                "assetId": "999",
                "bids": [{"price": "0.42", "size": "10"}],
                "asks": [],
                "timestamp": "1782753357257"
            }
        }"#;
        let events = to_events(&decode(text).unwrap(), 0, at());
        match &events[0] {
            BookEvent::Reset { symbol, bids, .. } => {
                assert_eq!(symbol, "999");
                assert_eq!(bids[0], (420_000, 10 * SCALE));
            }
            other => panic!("expected a reset, got {other:?}"),
        }
    }

    #[test]
    fn price_change_is_an_absolute_level_not_a_delta() {
        // Polymarket publishes the size now resting at the level. A collector that added
        // these together would report 300 then 300 again instead of 300 then 200.
        let text = r#"{
            "event_type": "price_change",
            "asset_id": "42",
            "changes": [{"price": "0.50", "size": "300", "side": "BUY"}],
            "timestamp": "1782753357257"
        }"#;
        let events = to_events(&decode(text).unwrap(), 0, at());
        match &events[0] {
            BookEvent::SetLevel {
                quantity,
                side,
                price,
                ..
            } => {
                assert_eq!(*quantity, 300 * SCALE);
                assert_eq!(*side, Side::Buy);
                assert_eq!(*price, 500_000);
            }
            other => panic!("expected an absolute level, got {other:?}"),
        }
    }

    #[test]
    fn sdk_price_change_carries_one_asset_per_change() {
        let text = r#"{
            "topic": "market",
            "type": "price_change",
            "payload": {
                "conditionId": "0xabc",
                "priceChanges": [
                    {"assetId": "111", "price": "0.31", "size": "5", "side": "SELL"},
                    {"assetId": "222", "price": "0.62", "size": "0", "side": "BUY"}
                ],
                "timestamp": "1782753357000"
            }
        }"#;
        let events = to_events(&decode(text).unwrap(), 1, at());
        assert_eq!(events.len(), 2);
        let symbols: Vec<&str> = events
            .iter()
            .map(|event| match event {
                BookEvent::SetLevel { symbol, .. } => symbol.as_str(),
                _ => panic!("unexpected event"),
            })
            .collect();
        assert_eq!(symbols, vec!["111", "222"]);
    }

    #[test]
    fn array_of_books_on_subscribe_decodes() {
        let text = r#"[{"event_type":"book","asset_id":"1","bids":[],"asks":[]},
                       {"event_type":"book","asset_id":"2","bids":[],"asks":[]}]"#;
        let frames = decode(text).unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(to_events(&frames, 0, at()).len(), 2);
    }

    #[test]
    fn unknown_and_irrelevant_events_are_ignored_not_fatal() {
        for text in [
            r#"{"event_type":"last_trade_price","asset_id":"1","price":"0.5","size":"1"}"#,
            r#"{"event_type":"tick_size_change","asset_id":"1","new_tick_size":"0.001"}"#,
            r#"{"event_type":"pong"}"#,
            r#"{"topic":"market","type":"best_bid_ask","payload":{"assetId":"1"}}"#,
            r#"{"event_type":"something_invented_next_year"}"#,
        ] {
            let frames = decode(text).expect("decoding must not fail");
            assert!(to_events(&frames, 0, at()).is_empty(), "{text}");
        }
    }

    #[test]
    fn resolution_drops_the_market() {
        let text = r#"{"event_type":"market_resolved","asset_id":"77","winning_asset_id":"77"}"#;
        let events = to_events(&decode(text).unwrap(), 0, at());
        match &events[0] {
            BookEvent::Drop { symbol } => assert_eq!(symbol, "77"),
            other => panic!("expected a drop, got {other:?}"),
        }
    }

    #[test]
    fn new_market_exposes_its_assets_without_touching_the_book() {
        let text = r#"{"event_type":"new_market","asset_ids":["1","2"],"question":"Q?"}"#;
        let frames = decode(text).unwrap();
        match &frames[0] {
            Normalized::NewMarket { assets } => {
                assert_eq!(assets, &["1".to_owned(), "2".to_owned()])
            }
            other => panic!("unexpected frame {other:?}"),
        }
        assert!(to_events(&frames, 0, at()).is_empty());
    }

    #[test]
    fn subscribe_frame_uses_polymarkets_own_field_name() {
        // `assets_ids` (their spelling), not `asset_ids`: the venue rejects the latter
        // silently, which reads as "subscribed but silent" at the application layer.
        let frame = subscribe_frame(&["1".to_owned(), "2".to_owned()]);
        assert!(frame.contains("\"assets_ids\""));
        assert!(frame.contains("\"type\":\"market\""));
        assert_eq!(heartbeat_frame(), "PING");
    }
}
