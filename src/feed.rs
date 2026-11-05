//! One websocket connection.
//!
//! A `Worker` owns a single Kalshi connection and nothing else, which keeps the failure
//! domain small. It knows how to subscribe, how to keep its subscription list across
//! reconnects, and how to detect that it missed messages. Decisions about *which*
//! socket should be live, and about redundancy, live in [`crate::supervisor`].
//!
//! Two design points worth calling out:
//!
//! * **Backpressure, never drop.** Book events go to the pipeline over a large bounded
//!   channel and the worker awaits on it. If the writer falls behind, the socket reader
//!   slows instead of losing data. That is safe for timing because every message carries
//!   the exchange's own timestamp, so a delayed message is still correctly stamped;
//!   only its arrival is late.
//! * **Sequence tracking per subscription.** Kalshi increments `seq` per sid, so a jump
//!   proves data loss on that subscription. The worker reports the gap instead of
//!   pretending the book is fine; the supervisor repairs it with a fresh snapshot.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use crate::Side;
use chrono::{DateTime, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tracing::{debug, info, warn};

use crate::book::SCALE;
use crate::config::KalshiEnv;
use crate::kalshi::auth::KalshiSigner;
use crate::kalshi::protocol::{
    self as proto, Envelope, ErrorPayload, MarketLifecyclePayload, OrderbookDeltaPayload,
    OrderbookSnapshotPayload, SubscribedPayload, parse_fixed_6,
};
use crate::pipeline::{BookEvent, WorkerId};

const ORDERBOOK_CHANNEL: &str = "orderbook_delta";
const LIFECYCLE_CHANNEL: &str = "market_lifecycle_v2";

type Socket = WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerMode {
    /// Subscribes to `orderbook_delta` for a shard's symbols and feeds the pipeline.
    Orderbook,
    /// Subscribes to `market_lifecycle_v2` and only reports symbol lifecycle changes.
    Lifecycle,
}

#[derive(Debug)]
pub enum WorkerNotice {
    Gap {
        worker: WorkerId,
        sid: i64,
        expected: i64,
        got: i64,
    },
    ExchangeError {
        worker: WorkerId,
        code: i64,
        message: String,
    },
    Lifecycle {
        ticker: String,
        event_type: String,
    },
}

#[derive(Debug)]
pub enum WorkerCommand {
    /// Add markets to this connection.
    Subscribe(Vec<String>),
    /// Remove markets from this connection.
    Unsubscribe(Vec<String>),
    /// Ask the exchange to resend snapshots without changing the subscription.
    Snapshot(Vec<String>),
    /// Reconnect on purpose (proactive recycle).
    Recycle,
    Shutdown,
}

#[derive(Debug, Default)]
pub struct WorkerStats {
    pub connected: AtomicBool,
    /// Unix millis of the last frame of any kind.
    pub last_message_ms: AtomicI64,
    pub messages: AtomicU64,
    pub orderbook_messages: AtomicU64,
    pub connects: AtomicU64,
    /// Only the socket currently trusted to write to the sink.
    pub is_source: AtomicBool,
}

impl WorkerStats {
    pub fn touch(&self) {
        self.last_message_ms
            .store(Utc::now().timestamp_millis(), Ordering::Relaxed);
        self.messages.fetch_add(1, Ordering::Relaxed);
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    /// `None` means the socket has never delivered a frame, which counts as infinitely
    /// stale for failover purposes.
    pub fn millis_since_message(&self) -> Option<i64> {
        let last = self.last_message_ms.load(Ordering::Relaxed);
        if last == 0 {
            None
        } else {
            Some(Utc::now().timestamp_millis() - last)
        }
    }

    pub fn set_source(&self, source: bool) {
        self.is_source.store(source, Ordering::Relaxed);
    }

    pub fn is_source(&self) -> bool {
        self.is_source.load(Ordering::Relaxed)
    }
}

pub struct WorkerHandle {
    pub id: WorkerId,
    pub mode: WorkerMode,
    pub stats: Arc<WorkerStats>,
    commands: mpsc::Sender<WorkerCommand>,
    task: JoinHandle<()>,
}

impl WorkerHandle {
    pub async fn send(&self, command: WorkerCommand) {
        if self.commands.send(command).await.is_err() {
            debug!(worker = self.id, "worker command channel closed");
        }
    }

    pub async fn shutdown(self) {
        let _ = self.commands.send(WorkerCommand::Shutdown).await;
        if tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .is_err()
        {
            debug!(worker = self.id, "worker did not stop within 5s");
        }
    }
}

pub struct WorkerSpec {
    pub id: WorkerId,
    pub mode: WorkerMode,
    pub symbols: Vec<String>,
    pub subscribe_batch: usize,
    pub env: KalshiEnv,
    /// Starts as the authoritative source for its shard (the primary socket).
    pub source: bool,
}

pub fn spawn(
    spec: WorkerSpec,
    signer: Arc<KalshiSigner>,
    events: mpsc::Sender<BookEvent>,
    notices: mpsc::Sender<WorkerNotice>,
    shutdown: watch::Receiver<bool>,
) -> WorkerHandle {
    let (cmd_tx, cmd_rx) = mpsc::channel(64);
    let stats = Arc::new(WorkerStats::default());
    stats.set_source(spec.source);
    let id = spec.id;
    let mode = spec.mode;
    let shared = stats.clone();
    let task = tokio::spawn(async move {
        run(spec, signer, shared, events, notices, cmd_rx, shutdown).await;
        info!(worker = id, "worker exited");
    });
    WorkerHandle {
        id,
        mode,
        stats,
        commands: cmd_tx,
        task,
    }
}

async fn run(
    spec: WorkerSpec,
    signer: Arc<KalshiSigner>,
    stats: Arc<WorkerStats>,
    events: mpsc::Sender<BookEvent>,
    notices: mpsc::Sender<WorkerNotice>,
    mut commands: mpsc::Receiver<WorkerCommand>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut subscriptions: Vec<String> = spec.symbols.clone();
    let mut backoff = Duration::from_millis(500);

    loop {
        if *shutdown.borrow() {
            break;
        }

        match connect(&signer, spec.env).await {
            Ok(mut socket) => {
                backoff = Duration::from_millis(500);
                stats.connected.store(true, Ordering::Relaxed);
                stats.connects.fetch_add(1, Ordering::Relaxed);
                stats.touch();
                info!(worker = spec.id, mode = ?spec.mode, subscriptions = subscriptions.len(), "connected");

                if let Err(err) = subscribe_all(&mut socket, &spec, &subscriptions).await {
                    warn!(worker = spec.id, error = %err, "subscribe failed");
                }

                let reason = read_loop(
                    &mut socket,
                    &spec,
                    &stats,
                    &mut subscriptions,
                    &mut commands,
                    &mut shutdown,
                    &events,
                    &notices,
                )
                .await;

                stats.connected.store(false, Ordering::Relaxed);
                // A polite close lets the exchange release the subscription immediately
                // instead of waiting for a timeout.
                let _ = socket.close(None).await;
                info!(worker = spec.id, reason = %reason, "disconnected");
            }
            Err(err) => {
                warn!(worker = spec.id, error = %err, "connect failed");
            }
        }

        if *shutdown.borrow() {
            break;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(15));
    }

    stats.connected.store(false, Ordering::Relaxed);
}

async fn connect(signer: &KalshiSigner, env: KalshiEnv) -> anyhow::Result<Socket> {
    let mut request = env.ws_url().into_client_request()?;
    for (name, value) in signer.headers("GET", env.ws_path())? {
        request.headers_mut().insert(
            HeaderName::from_bytes(name.as_bytes())?,
            HeaderValue::from_str(&value)?,
        );
    }
    let (socket, _response) = tokio_tungstenite::connect_async(request).await?;
    Ok(socket)
}

fn chunks(symbols: &[String], size: usize) -> impl Iterator<Item = &[String]> {
    symbols.chunks(size.max(1))
}

/// Sends the initial subscription for every symbol this worker should hold.
async fn subscribe_all(
    socket: &mut Socket,
    spec: &WorkerSpec,
    symbols: &[String],
) -> anyhow::Result<()> {
    match spec.mode {
        WorkerMode::Lifecycle => {
            // No ticker filter: the lifecycle channel streams every market change, which
            // is exactly what new-symbol discovery needs.
            let command = proto::subscribe_channel(LIFECYCLE_CHANNEL);
            socket.send(Message::Text(command)).await?;
        }
        WorkerMode::Orderbook => {
            for (index, chunk) in chunks(symbols, spec.subscribe_batch).enumerate() {
                let command = proto::subscribe(1_000 + index as u64, ORDERBOOK_CHANNEL, chunk);
                socket.send(Message::Text(command)).await?;
                // Spread the subscribe burst so a large shard does not look like a
                // rate-limit attack.
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    Ok(())
}

#[derive(Default)]
struct SubscriptionMap {
    sid_of_symbol: HashMap<String, i64>,
    symbols_of_sid: HashMap<i64, Vec<String>>,
    seq_of_sid: HashMap<i64, i64>,
    /// Outbound command id -> the symbols it asked for, so the `subscribed` reply can be
    /// attributed to the right batch instead of guessing.
    pending: HashMap<u64, Vec<String>>,
}

#[allow(clippy::too_many_arguments)]
async fn read_loop(
    socket: &mut Socket,
    spec: &WorkerSpec,
    stats: &WorkerStats,
    subscriptions: &mut Vec<String>,
    commands: &mut mpsc::Receiver<WorkerCommand>,
    shutdown: &mut watch::Receiver<bool>,
    events: &mpsc::Sender<BookEvent>,
    notices: &mpsc::Sender<WorkerNotice>,
) -> String {
    let mut map = SubscriptionMap::default();
    // The ids used by `subscribe_all` are 1000.. so command ids never collide with them.
    for (index, chunk) in chunks(&spec.symbols, spec.subscribe_batch).enumerate() {
        map.pending.insert(1_000 + index as u64, chunk.to_vec());
    }
    let mut next_id: u64 = 1;

    loop {
        let step = tokio::select! {
            frame = socket.next() => match frame {
                Some(Ok(message)) => Step::Frame(message),
                Some(Err(WsError::Protocol(_))) | Some(Err(WsError::AlreadyClosed)) => {
                    return "protocol closed".to_owned();
                }
                Some(Err(err)) => return format!("socket error: {err}"),
                None => return "stream ended".to_owned(),
            },
            command = commands.recv() => match command {
                Some(command) => Step::Command(command),
                None => return "command channel closed".to_owned(),
            },
            _ = shutdown.changed() => Step::Shutdown,
        };

        match step {
            Step::Shutdown => {
                // One best-effort unsubscribe, then leave: the process is stopping and a
                // clean close is what actually releases the subscription.
                let sids: Vec<i64> = map.symbols_of_sid.keys().copied().collect();
                if !sids.is_empty() {
                    let command = proto::unsubscribe(next_id, &sids);
                    if socket.send(Message::Text(command)).await.is_err() {
                        debug!(worker = spec.id, "unsubscribe on shutdown failed");
                    }
                }
                return "shutdown".to_owned();
            }
            Step::Frame(Message::Text(text)) => {
                if let Err(err) = handle_text(spec, stats, &text, &mut map, events, notices).await {
                    return format!("protocol error: {err}");
                }
            }
            Step::Frame(Message::Ping(payload)) => {
                // tungstenite also queues an automatic Pong; replying explicitly keeps
                // the behaviour obvious and survives library changes.
                if socket.send(Message::Pong(payload)).await.is_err() {
                    return "pong send failed".to_owned();
                }
            }
            Step::Frame(Message::Close(frame)) => {
                return format!("closed by exchange: {frame:?}");
            }
            Step::Frame(_) => {}
            Step::Command(WorkerCommand::Shutdown) => {
                let sids: Vec<i64> = map.symbols_of_sid.keys().copied().collect();
                if !sids.is_empty() {
                    let command = proto::unsubscribe(next_id, &sids);
                    let _ = socket.send(Message::Text(command)).await;
                }
                return "shutdown".to_owned();
            }
            Step::Command(WorkerCommand::Recycle) => return "recycle requested".to_owned(),
            Step::Command(WorkerCommand::Subscribe(symbols)) => {
                let fresh: Vec<String> = symbols
                    .into_iter()
                    .filter(|symbol| !map.sid_of_symbol.contains_key(symbol))
                    .filter(|symbol| !map.pending.values().any(|held| held.contains(symbol)))
                    .collect();
                if fresh.is_empty() {
                    continue;
                }
                let mut by_sid: HashMap<i64, Vec<String>> = HashMap::new();
                let mut unsubscribed = Vec::new();
                for symbol in &fresh {
                    match map.sid_of_symbol.get(symbol).copied() {
                        Some(sid) => by_sid.entry(sid).or_default().push(symbol.clone()),
                        // A symbol we have never subscribed has no sid yet, so it goes in
                        // a fresh subscribe. Adding to an existing sid would silently skip
                        // the snapshot the new market needs.
                        None => unsubscribed.push(symbol.clone()),
                    }
                }
                for chunk in chunks(&unsubscribed, spec.subscribe_batch) {
                    let id = next_id;
                    next_id += 1;
                    map.pending.insert(id, chunk.to_vec());
                    let command = proto::subscribe(id, ORDERBOOK_CHANNEL, chunk);
                    if socket.send(Message::Text(command)).await.is_err() {
                        return "subscribe send failed".to_owned();
                    }
                    for symbol in chunk {
                        if !subscriptions.contains(symbol) {
                            subscriptions.push(symbol.clone());
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                for (sid, chunk) in by_sid {
                    let id = next_id;
                    next_id += 1;
                    map.pending.insert(id, chunk.clone());
                    let command = proto::update_subscription(id, sid, &chunk, "add_markets");
                    if socket.send(Message::Text(command)).await.is_err() {
                        return "add_markets send failed".to_owned();
                    }
                }
            }
            Step::Command(WorkerCommand::Unsubscribe(symbols)) => {
                let mut by_sid: HashMap<i64, Vec<String>> = HashMap::new();
                for symbol in &symbols {
                    if let Some(sid) = map.sid_of_symbol.remove(symbol) {
                        by_sid.entry(sid).or_default().push(symbol.clone());
                    }
                }
                for (sid, tickers) in by_sid {
                    let command =
                        proto::update_subscription(next_id, sid, &tickers, "delete_markets");
                    next_id += 1;
                    if socket.send(Message::Text(command)).await.is_err() {
                        return "unsubscribe send failed".to_owned();
                    }
                    if let Some(held) = map.symbols_of_sid.get_mut(&sid) {
                        held.retain(|symbol| !tickers.contains(symbol));
                    }
                }
                subscriptions.retain(|symbol| !symbols.contains(symbol));
            }
            Step::Command(WorkerCommand::Snapshot(symbols)) => {
                let mut by_sid: HashMap<i64, Vec<String>> = HashMap::new();
                for symbol in &symbols {
                    if let Some(sid) = map.sid_of_symbol.get(symbol) {
                        by_sid.entry(*sid).or_default().push(symbol.clone());
                    }
                }
                for (sid, tickers) in by_sid {
                    for chunk in chunks(&tickers, spec.subscribe_batch) {
                        let command =
                            proto::update_subscription(next_id, sid, chunk, "get_snapshot");
                        next_id += 1;
                        if socket.send(Message::Text(command)).await.is_err() {
                            return "snapshot request failed".to_owned();
                        }
                    }
                }
            }
        }
    }
}

enum Step {
    Frame(Message),
    Command(WorkerCommand),
    Shutdown,
}

async fn handle_text(
    spec: &WorkerSpec,
    stats: &WorkerStats,
    text: &str,
    map: &mut SubscriptionMap,
    events: &mpsc::Sender<BookEvent>,
    notices: &mpsc::Sender<WorkerNotice>,
) -> anyhow::Result<()> {
    stats.touch();
    let envelope: Envelope = serde_json::from_str(text)?;
    let received_at = Utc::now();
    let source = stats.is_source();

    match envelope.kind.as_str() {
        "subscribed" => {
            let Some(payload) = envelope.msg.clone() else {
                return Ok(());
            };
            let payload: SubscribedPayload = serde_json::from_value(payload)?;
            let symbols = envelope
                .id
                .and_then(|id| map.pending.remove(&id))
                .unwrap_or_default();
            for symbol in &symbols {
                map.sid_of_symbol.insert(symbol.clone(), payload.sid);
            }
            map.symbols_of_sid.insert(payload.sid, symbols);
        }
        "orderbook_snapshot" => {
            let Some(payload) = envelope.msg.clone() else {
                return Ok(());
            };
            let payload: OrderbookSnapshotPayload = serde_json::from_value(payload)?;
            stats.orderbook_messages.fetch_add(1, Ordering::Relaxed);
            if let (Some(seq), Some(sid)) = (envelope.seq, envelope.sid) {
                note_seq(spec, sid, seq, &mut map.seq_of_sid, notices).await;
            }
            if spec.mode != WorkerMode::Orderbook || !source {
                return Ok(());
            }
            let (bids, asks) = snapshot_levels(&payload);
            events
                .send(BookEvent::Reset {
                    symbol: payload.market_ticker,
                    bids,
                    asks,
                    clock: payload.clock,
                    received_at,
                    exchange_timestamp: exchange_timestamp(envelope.sending_ts_ms, None),
                    worker: spec.id,
                })
                .await?;
        }
        "orderbook_delta" => {
            let Some(payload) = envelope.msg.clone() else {
                return Ok(());
            };
            let payload: OrderbookDeltaPayload = serde_json::from_value(payload)?;
            stats.orderbook_messages.fetch_add(1, Ordering::Relaxed);
            if let (Some(seq), Some(sid)) = (envelope.seq, envelope.sid) {
                note_seq(spec, sid, seq, &mut map.seq_of_sid, notices).await;
            }
            if spec.mode != WorkerMode::Orderbook || !source {
                return Ok(());
            }
            let Some((side, price, delta)) = convert_delta(&payload) else {
                warn!(worker = spec.id, ticker = %payload.market_ticker, "unparsable delta");
                return Ok(());
            };
            events
                .send(BookEvent::Delta {
                    symbol: payload.market_ticker,
                    side,
                    price,
                    delta,
                    clock: payload.clock,
                    received_at,
                    exchange_timestamp: exchange_timestamp(envelope.sending_ts_ms, None),
                    worker: spec.id,
                })
                .await?;
        }
        "market_lifecycle_v2" => {
            if let Some(Ok(payload)) = envelope
                .msg
                .clone()
                .map(serde_json::from_value::<MarketLifecyclePayload>)
            {
                let _ = notices
                    .send(WorkerNotice::Lifecycle {
                        ticker: payload.market_ticker,
                        event_type: payload.event_type,
                    })
                    .await;
            }
        }
        "error" => {
            if let Some(Ok(payload)) = envelope
                .msg
                .clone()
                .map(serde_json::from_value::<ErrorPayload>)
            {
                warn!(worker = spec.id, code = payload.code, msg = %payload.msg, "exchange error");
                let _ = notices
                    .send(WorkerNotice::ExchangeError {
                        worker: spec.id,
                        code: payload.code,
                        message: payload.msg,
                    })
                    .await;
            }
        }
        _ => {}
    }

    Ok(())
}

async fn note_seq(
    spec: &WorkerSpec,
    sid: i64,
    seq: i64,
    seq_of_sid: &mut HashMap<i64, i64>,
    notices: &mpsc::Sender<WorkerNotice>,
) {
    if let Some(previous) = seq_of_sid.insert(sid, seq) {
        let expected = previous + 1;
        if seq != expected {
            let _ = notices
                .send(WorkerNotice::Gap {
                    worker: spec.id,
                    sid,
                    expected,
                    got: seq,
                })
                .await;
        }
    }
}

/// Kalshi publishes YES bids and NO bids. A NO bid at `q` is exactly a YES ask at
/// `1 - q` for the same size, so the two arrays become the two sides of one
/// YES-denominated book. Keeping the venue's raw NO prices would produce a book whose
/// two sides live on different scales.
/// `(bids, asks)` in exchange fixed point, both normalised to YES-price space.
pub type Levels = (Vec<(i64, i64)>, Vec<(i64, i64)>);

pub fn snapshot_levels(payload: &OrderbookSnapshotPayload) -> Levels {
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    for (price, quantity) in payload.yes_dollars_fp.iter().flatten() {
        if let (Some(price), Some(quantity)) = (parse_fixed_6(price), parse_fixed_6(quantity)) {
            bids.push((price, quantity));
        }
    }
    for (price, quantity) in payload.no_dollars_fp.iter().flatten() {
        if let (Some(price), Some(quantity)) = (parse_fixed_6(price), parse_fixed_6(quantity)) {
            asks.push((SCALE - price, quantity));
        }
    }
    (bids, asks)
}

pub fn convert_delta(payload: &OrderbookDeltaPayload) -> Option<(Side, i64, i64)> {
    let price = parse_fixed_6(&payload.price_dollars)?;
    let delta = parse_fixed_6(&payload.delta_fp)?;
    match payload.side.as_str() {
        "yes" => Some((Side::Buy, price, delta)),
        "no" => Some((Side::Sell, SCALE - price, delta)),
        _ => None,
    }
}

fn exchange_timestamp(sending_ts_ms: Option<i64>, payload_ts_ms: Option<i64>) -> DateTime<Utc> {
    let millis = payload_ts_ms
        .or(sending_ts_ms)
        .unwrap_or_else(|| Utc::now().timestamp_millis());
    Utc.timestamp_millis_opt(millis)
        .single()
        .unwrap_or_else(Utc::now)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_bids_become_yes_asks_at_complement_price() {
        let payload = OrderbookSnapshotPayload {
            market_ticker: "MKT".to_owned(),
            yes_dollars_fp: Some(vec![("0.4000".to_owned(), "10.00".to_owned())]),
            no_dollars_fp: Some(vec![("0.3000".to_owned(), "5.00".to_owned())]),
            clock: Some(1),
        };
        let (bids, asks) = snapshot_levels(&payload);
        assert_eq!(bids, vec![(400_000, 10 * SCALE)]);
        assert_eq!(asks, vec![(700_000, 5 * SCALE)]);
    }

    #[test]
    fn delta_sides_map_to_book_sides() {
        let yes = OrderbookDeltaPayload {
            market_ticker: "MKT".to_owned(),
            price_dollars: "0.9600".to_owned(),
            delta_fp: "-54.00".to_owned(),
            side: "yes".to_owned(),
            clock: None,
        };
        assert_eq!(convert_delta(&yes), Some((Side::Buy, 960_000, -54 * SCALE)));

        let no = OrderbookDeltaPayload {
            market_ticker: "MKT".to_owned(),
            price_dollars: "0.2500".to_owned(),
            delta_fp: "3.00".to_owned(),
            side: "no".to_owned(),
            clock: None,
        };
        assert_eq!(convert_delta(&no), Some((Side::Sell, 750_000, 3 * SCALE)));
    }
}
