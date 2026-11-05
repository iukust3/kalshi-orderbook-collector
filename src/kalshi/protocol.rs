//! Kalshi wire protocol: message envelopes, payloads and outbound commands.
//!
//! Kalshi's websocket frames all share `type`, `sid`, `seq`, `sending_ts_ms` and a
//! channel-specific `msg`. Rather than an internally tagged enum (which fights the
//! `msg` field name and the per-channel payload shapes), the envelope is parsed once
//! and the payload is decoded only for the types this collector consumes. Unknown
//! types are ignored, which keeps the collector working when Kalshi adds a channel or
//! a field.

use serde::Deserialize;
use serde_json::{Value, json};

/// Kalshi expresses prices and sizes as decimal strings in dollars/shares
/// (`"0.0800"`, `"-54.00"`). Everything downstream is fixed point at 1e-6, so this is
/// the only place decimals are parsed, and it is done without floating point.
pub fn parse_fixed_6(raw: &str) -> Option<i64> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let (negative, body) = match raw.as_bytes()[0] {
        b'-' => (true, &raw[1..]),
        b'+' => (false, &raw[1..]),
        _ => (false, raw),
    };
    let (whole, frac) = match body.split_once('.') {
        Some((whole, frac)) => (whole, frac),
        None => (body, ""),
    };
    if whole.is_empty() && frac.is_empty() {
        return None;
    }
    if !whole.chars().all(|c| c.is_ascii_digit()) || !frac.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    if frac.len() > 6 {
        return None;
    }
    let whole: i64 = whole.parse().ok()?;
    let mut frac_padded = frac.to_owned();
    while frac_padded.len() < 6 {
        frac_padded.push('0');
    }
    let frac: i64 = frac_padded.parse().ok()?;
    let value = whole.checked_mul(1_000_000)?.checked_add(frac)?;
    Some(if negative { -value } else { value })
}

#[derive(Debug, Deserialize)]
pub struct Envelope {
    #[serde(rename = "type")]
    pub kind: String,
    /// Echo of the outbound command id, which is how a `subscribed` reply is attributed
    /// to the batch that asked for it.
    pub id: Option<u64>,
    pub sid: Option<i64>,
    pub seq: Option<i64>,
    #[serde(rename = "sending_ts_ms")]
    pub sending_ts_ms: Option<i64>,
    #[serde(default)]
    pub msg: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct OrderbookSnapshotPayload {
    pub market_ticker: String,
    /// Resting bids for YES, as `[price_dollars, quantity]`.
    #[serde(default)]
    pub yes_dollars_fp: Option<Vec<(String, String)>>,
    /// Resting bids for NO. A NO bid at `q` is a YES ask at `1 - q`.
    #[serde(default)]
    pub no_dollars_fp: Option<Vec<(String, String)>>,
    pub clock: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct OrderbookDeltaPayload {
    pub market_ticker: String,
    pub price_dollars: String,
    pub delta_fp: String,
    pub side: String,
    pub clock: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct SubscribedPayload {
    pub channel: String,
    pub sid: i64,
}

#[derive(Debug, Deserialize)]
pub struct ErrorPayload {
    pub code: i64,
    pub msg: String,
}

#[derive(Debug, Deserialize)]
pub struct MarketLifecyclePayload {
    pub market_ticker: String,
    pub event_type: String,
}

// ---- outbound -----------------------------------------------------------------

pub fn subscribe(id: u64, channel: &str, tickers: &[String]) -> String {
    json!({
        "id": id,
        "cmd": "subscribe",
        "params": { "channels": [channel], "market_tickers": tickers }
    })
    .to_string()
}

/// Subscribes to a whole channel with no ticker filter. The lifecycle channel uses
/// this: asking for every market change is exactly how new symbols are discovered,
/// and a ticker list would defeat the purpose.
pub fn subscribe_channel(channel: &str) -> String {
    json!({
        "id": 1,
        "cmd": "subscribe",
        "params": { "channels": [channel] }
    })
    .to_string()
}

/// Removes a subscription by `sid`. Used on shutdown and when a market is delisted, so
/// the exchange frees the slot instead of counting us against the limit until timeout.
pub fn unsubscribe(id: u64, sids: &[i64]) -> String {
    json!({
        "id": id,
        "cmd": "unsubscribe",
        "params": { "sids": sids }
    })
    .to_string()
}

pub fn update_subscription(id: u64, sid: i64, tickers: &[String], action: &str) -> String {
    json!({
        "id": id,
        "cmd": "update_subscription",
        "params": { "sid": sid, "market_tickers": tickers, "action": action }
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_dollar_strings_without_float_error() {
        assert_eq!(parse_fixed_6("0.0800"), Some(80_000));
        assert_eq!(parse_fixed_6("1"), Some(1_000_000));
        assert_eq!(parse_fixed_6("0.123456"), Some(123_456));
        assert_eq!(parse_fixed_6("-54.00"), Some(-54_000_000));
        assert_eq!(parse_fixed_6("300.00"), Some(300_000_000));
        assert_eq!(parse_fixed_6("0.000001"), Some(1));
        assert_eq!(parse_fixed_6(""), None);
        assert_eq!(parse_fixed_6("abc"), None);
        assert_eq!(parse_fixed_6("0.1234567"), None);
    }

    #[test]
    fn envelope_tolerates_unknown_types() {
        let raw = r#"{"type":"ok","id":3,"sending_ts_ms":1,"msg":[{"channel":"orderbook_delta","sid":1}]}"#;
        let envelope: Envelope = serde_json::from_str(raw).unwrap();
        assert_eq!(envelope.kind, "ok");
    }

    #[test]
    fn snapshot_payload_parses_level_pairs() {
        let raw = r#"{
            "market_ticker": "FED-23DEC-T3.00",
            "clock": 4182057,
            "yes_dollars_fp": [["0.0800","300.00"],["0.2200","333.00"]],
            "no_dollars_fp": [["0.5400","20.00"]]
        }"#;
        let payload: OrderbookSnapshotPayload = serde_json::from_str(raw).unwrap();
        let yes = payload.yes_dollars_fp.unwrap();
        assert_eq!(yes.len(), 2);
        assert_eq!(parse_fixed_6(&yes[1].0), Some(220_000));
        assert_eq!(payload.clock, Some(4_182_057));
    }
}
