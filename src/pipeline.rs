//! The book actor: the single owner of all order book state and of the output sink.
//!
//! Everything upstream (REST catalog, websocket workers, the lifecycle connection) only
//! produces [`BookEvent`]s. One task consumes them in arrival order, owns every
//! [`OrderBook`], and is the only thing that writes to the [`Sink`]. That gives three
//! properties for free:
//!
//! * **Total order.** Messages reach the file in exactly the order the book changed, so
//!   a replay never sees a delta before the snapshot it depends on.
//! * **No interleaving.** Two sockets can hand over a shard without ever writing
//!   concurrently.
//! * **Cross-socket deduplication.** Each market carries an exchange `clock` that is
//!   identical on every connection, so the actor can drop anything it has already
//!   applied. This is what makes redundant sockets safe rather than double-counting.

use std::collections::HashMap;
use std::time::Duration;

use crate::Side;
use crate::util::dtos::challenge_orderbook::PriceLevelUpdateDomain;
use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use crate::book::OrderBook;
use crate::sink::{FlushMode, Outbound, Sink};

/// Where an event came from, for logging and for the "snapshot on source switch" rule.
pub type WorkerId = u64;

#[derive(Debug)]
pub enum BookEvent {
    /// Authoritative replacement of a market's whole book.
    Reset {
        symbol: String,
        bids: Vec<(i64, i64)>,
        asks: Vec<(i64, i64)>,
        clock: Option<u64>,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
        worker: WorkerId,
    },
    /// A signed size change at one price level.
    Delta {
        symbol: String,
        side: Side,
        price: i64,
        delta: i64,
        clock: Option<u64>,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
        worker: WorkerId,
    },
    /// One price level set to an absolute resting size, for venues that publish size
    /// rather than a signed change (Polymarket). Zero means the level is gone.
    SetLevel {
        symbol: String,
        side: Side,
        price: i64,
        quantity: i64,
        clock: Option<u64>,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
        worker: WorkerId,
    },
    /// A market is no longer trading; stop tracking it.
    Drop {
        symbol: String,
    },
    /// Force buffered bytes out (used on shutdown).
    Flush,
    Shutdown(oneshot::Sender<()>),
}

/// The exchange `clock` is monotonic per market and shared across connections, but
/// several deltas from one exchange action can carry the *same* clock. Equality alone is
/// therefore not proof of a duplicate: two deltas with the same clock from the same
/// socket are two real changes, while the same clock arriving from a *different* socket
/// is the redundant copy. The floor keeps both halves of that fact, plus whether it came
/// from a snapshot (which summarises every change at or below its clock).
#[derive(Debug, Clone, Copy)]
struct ClockFloor {
    clock: u64,
    worker: WorkerId,
    from_snapshot: bool,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PipelineStats {
    pub snapshots_written: u64,
    pub updates_written: u64,
    pub stale_dropped: u64,
    pub unknown_symbol_dropped: u64,
    pub markets_tracked: usize,
}

pub struct Pipeline {
    books: HashMap<String, OrderBook>,
    floors: HashMap<String, ClockFloor>,
    sink: Box<dyn Sink>,
    stats: PipelineStats,
    snapshot_interval: Duration,
    flush_interval: Duration,
}

impl Pipeline {
    pub fn new(sink: Box<dyn Sink>, snapshot_interval: Duration, flush_interval: Duration) -> Self {
        Self {
            books: HashMap::new(),
            floors: HashMap::new(),
            sink,
            stats: PipelineStats::default(),
            snapshot_interval,
            flush_interval,
        }
    }

    /// Runs until a [`BookEvent::Shutdown`] arrives and the sink is flushed.
    pub async fn run(mut self, mut rx: mpsc::Receiver<BookEvent>) {
        let mut flush_tick = tokio::time::interval(self.flush_interval);
        let mut snapshot_tick = tokio::time::interval(self.snapshot_interval);
        flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        snapshot_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut report_tick = tokio::time::interval(Duration::from_secs(30));
        report_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                event = rx.recv() => {
                    let Some(event) = event else { break };
                    match event {
                        BookEvent::Reset { .. }
                        | BookEvent::Delta { .. }
                        | BookEvent::SetLevel { .. }
                        | BookEvent::Drop { .. } => {
                            self.handle(event);
                        }
                        BookEvent::Flush => {
                            if let Err(err) = self.sink.flush(FlushMode::Force) {
                                warn!(error = %err, "flush failed");
                            }
                        }
                        BookEvent::Shutdown(done) => {
                            self.write_snapshots_all();
                            if let Err(err) = self.sink.flush(FlushMode::Force) {
                                warn!(error = %err, "final flush failed");
                            }
                            let _ = done.send(());
                            break;
                        }
                    }
                }
                _ = flush_tick.tick() => {
                    if let Err(err) = self.sink.flush(FlushMode::IfWorthIt) {
                        warn!(error = %err, "scheduled flush failed");
                    }
                }
                _ = snapshot_tick.tick() => {
                    // A periodic full snapshot bounds recovery cost for a consumer that
                    // starts mid-stream or whose own state drifted.
                    self.write_snapshots_all();
                }
                _ = report_tick.tick() => {
                    info!(
                        markets = self.stats.markets_tracked,
                        snapshots = self.stats.snapshots_written,
                        updates = self.stats.updates_written,
                        stale_dropped = self.stats.stale_dropped,
                        unknown_dropped = self.stats.unknown_symbol_dropped,
                        "pipeline"
                    );
                }
            }
        }

        info!(
            snapshots = self.stats.snapshots_written,
            updates = self.stats.updates_written,
            "pipeline stopped"
        );
    }

    fn handle(&mut self, event: BookEvent) {
        match event {
            BookEvent::Reset {
                symbol,
                bids,
                asks,
                clock,
                received_at,
                exchange_timestamp,
                worker,
            } => {
                // Scope the book borrow: the sink write happens while the book is
                // borrowed, so anything touching `self` again must wait until it ends.
                let snapshot = {
                    let book = self.books.entry(symbol.clone()).or_default();
                    book.reset(bids, asks);
                    book.snapshot(&symbol, received_at, exchange_timestamp)
                };
                self.stats.markets_tracked = self.books.len();
                if let Some(clock) = clock {
                    self.floors.insert(
                        symbol.clone(),
                        ClockFloor {
                            clock,
                            worker,
                            from_snapshot: true,
                        },
                    );
                }
                debug!(symbol = %symbol, worker, "snapshot");
                if let Err(err) = self.sink.write(Outbound::Snapshot(Box::new(snapshot))) {
                    warn!(error = %err, "snapshot write failed");
                } else {
                    self.stats.snapshots_written += 1;
                }
            }
            BookEvent::Delta {
                symbol,
                side,
                price,
                delta,
                clock,
                received_at,
                exchange_timestamp,
                worker,
            } => {
                if self.is_stale(&symbol, clock, worker) {
                    return;
                }
                let Some(book) = self.books.get_mut(&symbol) else {
                    // We only track markets we are subscribed to; anything else is a
                    // late message from a shard we already dropped.
                    self.stats.unknown_symbol_dropped += 1;
                    return;
                };
                let quantity = book.apply(side, price, delta);
                self.write_level(
                    symbol,
                    side,
                    price,
                    quantity,
                    clock,
                    received_at,
                    exchange_timestamp,
                    worker,
                );
            }
            BookEvent::SetLevel {
                symbol,
                side,
                price,
                quantity,
                clock,
                received_at,
                exchange_timestamp,
                worker,
            } => {
                if self.is_stale(&symbol, clock, worker) {
                    return;
                }
                let Some(book) = self.books.get_mut(&symbol) else {
                    self.stats.unknown_symbol_dropped += 1;
                    return;
                };
                let resting = book.set_level(side, price, quantity);
                self.write_level(
                    symbol,
                    side,
                    price,
                    resting,
                    clock,
                    received_at,
                    exchange_timestamp,
                    worker,
                );
            }
            BookEvent::Drop { symbol } => {
                if self.books.remove(&symbol).is_some() {
                    self.floors.remove(&symbol);
                    self.stats.markets_tracked = self.books.len();
                    debug!(symbol = %symbol, "dropped");
                }
            }
            BookEvent::Flush | BookEvent::Shutdown(_) => {}
        }
    }

    /// True when this message must be dropped as already applied.
    ///
    /// An unknown symbol is not stale; the caller decides that separately, because a
    /// late message for a dropped market and a duplicate for a tracked one are different
    /// problems with different counters.
    fn is_stale(&mut self, symbol: &str, clock: Option<u64>, worker: WorkerId) -> bool {
        let (Some(clock), Some(floor)) = (clock, self.floors.get(symbol)) else {
            return false;
        };
        let older = clock < floor.clock;
        let same_action_and_socket =
            clock == floor.clock && !floor.from_snapshot && floor.worker == worker;
        if older || (clock == floor.clock && !same_action_and_socket) {
            self.stats.stale_dropped += 1;
            return true;
        }
        false
    }

    #[allow(clippy::too_many_arguments)]
    fn write_level(
        &mut self,
        symbol: String,
        side: Side,
        price: i64,
        quantity: i64,
        clock: Option<u64>,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
        worker: WorkerId,
    ) {
        if let Some(clock) = clock {
            self.floors.insert(
                symbol.clone(),
                ClockFloor {
                    clock,
                    worker,
                    from_snapshot: false,
                },
            );
        }
        debug!(symbol = %symbol, worker, "level");
        let update = PriceLevelUpdateDomain {
            backtest_timestamp: received_at,
            exchange_timestamp,
            price_level: price,
            quantity,
            side,
            symbol,
        };
        if let Err(err) = self.sink.write(Outbound::Update(update)) {
            warn!(error = %err, "update write failed");
        } else {
            self.stats.updates_written += 1;
        }
    }

    fn write_snapshots_all(&mut self) {
        let now = Utc::now();
        let mut snapshots = Vec::with_capacity(self.books.len());
        for (symbol, book) in &self.books {
            // The exchange timestamp is unknown at snapshot-on-timer time; reusing the
            // emit instant keeps the field monotonic and lets consumers tell a
            // collector-generated periodic snapshot from an exchange one by clock.
            snapshots.push(book.snapshot(symbol, now, now));
        }
        for snapshot in snapshots {
            if let Err(err) = self.sink.write(Outbound::Snapshot(Box::new(snapshot))) {
                warn!(error = %err, "periodic snapshot write failed");
                return;
            }
            self.stats.snapshots_written += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::SCALE;
    use crate::sink::RecordingSink;

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap()
    }

    fn pipeline(sink: Box<dyn Sink>) -> Pipeline {
        Pipeline::new(sink, Duration::from_secs(60), Duration::from_secs(1))
    }

    #[test]
    fn delta_writes_absolute_quantity() {
        let mut pipeline = pipeline(Box::new(RecordingSink::default()));
        pipeline.handle(BookEvent::Reset {
            symbol: "MKT".to_owned(),
            bids: vec![(500_000, 10 * SCALE)],
            asks: vec![],
            clock: Some(100),
            received_at: now(),
            exchange_timestamp: now(),
            worker: 1,
        });
        pipeline.handle(BookEvent::Delta {
            symbol: "MKT".to_owned(),
            side: Side::Buy,
            price: 500_000,
            delta: -4 * SCALE,
            clock: Some(101),
            received_at: now(),
            exchange_timestamp: now(),
            worker: 1,
        });
        assert_eq!(pipeline.stats.updates_written, 1);
        assert_eq!(pipeline.stats.snapshots_written, 1);
    }

    #[test]
    fn duplicate_clock_from_standby_is_dropped() {
        let mut pipeline = pipeline(Box::new(RecordingSink::default()));
        pipeline.handle(BookEvent::Reset {
            symbol: "MKT".to_owned(),
            bids: vec![(500_000, 10 * SCALE)],
            asks: vec![],
            clock: Some(100),
            received_at: now(),
            exchange_timestamp: now(),
            worker: 1,
        });
        for (worker, clock) in [(1, 101), (2, 101)] {
            pipeline.handle(BookEvent::Delta {
                symbol: "MKT".to_owned(),
                side: Side::Buy,
                price: 500_000,
                delta: -SCALE,
                clock: Some(clock),
                received_at: now(),
                exchange_timestamp: now(),
                worker,
            });
        }
        assert_eq!(pipeline.stats.updates_written, 1);
        assert_eq!(pipeline.stats.stale_dropped, 1);
    }

    #[test]
    fn deltas_sharing_one_clock_are_all_applied() {
        let mut pipeline = pipeline(Box::new(RecordingSink::default()));
        pipeline.handle(BookEvent::Reset {
            symbol: "MKT".to_owned(),
            bids: vec![(500_000, 10 * SCALE)],
            asks: vec![],
            clock: Some(100),
            received_at: now(),
            exchange_timestamp: now(),
            worker: 1,
        });
        for price in [490_000, 480_000] {
            pipeline.handle(BookEvent::Delta {
                symbol: "MKT".to_owned(),
                side: Side::Buy,
                price,
                delta: SCALE,
                clock: Some(105),
                received_at: now(),
                exchange_timestamp: now(),
                worker: 1,
            });
        }
        assert_eq!(pipeline.stats.updates_written, 2);
    }

    #[test]
    fn delta_equal_to_snapshot_clock_is_dropped() {
        let mut pipeline = pipeline(Box::new(RecordingSink::default()));
        pipeline.handle(BookEvent::Reset {
            symbol: "MKT".to_owned(),
            bids: vec![(500_000, 10 * SCALE)],
            asks: vec![],
            clock: Some(200),
            received_at: now(),
            exchange_timestamp: now(),
            worker: 1,
        });
        pipeline.handle(BookEvent::Delta {
            symbol: "MKT".to_owned(),
            side: Side::Buy,
            price: 500_000,
            delta: SCALE,
            clock: Some(200),
            received_at: now(),
            exchange_timestamp: now(),
            worker: 1,
        });
        assert_eq!(pipeline.stats.updates_written, 0);
        assert_eq!(pipeline.stats.stale_dropped, 1);
    }
}
