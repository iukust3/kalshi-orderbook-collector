//! Orchestration: which symbols exist, which socket owns them, and what happens when a
//! socket fails.
//!
//! The collector's correctness rests on a simple split. A [`feed::spawn`]ed worker owns
//! one connection and knows nothing about the rest of the world. The [`Supervisor`] owns
//! the *global* decisions:
//!
//! * **Sharding.** Every open market is assigned to exactly one shard. A shard is one
//!   primary socket plus an optional hot standby, so one connection never has to carry
//!   the whole exchange and a shard failure costs a fraction of the stream.
//! * **Hot standby.** The standby subscribes to the same markets and validates its own
//!   sequence numbers but is not a *source*: its frames never reach the sink. Only the
//!   source writes, which is why two live sockets cannot double-count liquidity. On
//!   failover the standby is promoted and asked for a fresh snapshot, so the book is
//!   rebuilt from an authoritative point instead of trusting deltas it may have missed.
//! * **Stale detection.** "Connected" is not "working". Kalshi can leave a socket open
//!   while silently dropping a subscription, so the supervisor compares wall-clock
//!   freshness between the two sockets and switches when the source goes quiet while the
//!   standby is still receiving.
//! * **Lifecycle reconciliation.** New markets are discovered two ways, because neither
//!   alone is timely: the `market_lifecycle_v2` stream is instant but can miss events
//!   across a reconnect, and the REST catalog is complete but up to
//!   [`Config::catalog_refresh`] stale. The union of both is applied idempotently.
//! * **Repair.** A sequence gap reported by a worker is not a log line, it is data loss
//!   in a running book. The supervisor answers it with a snapshot request for the
//!   affected shard, which resets that book and re-establishes the clock floor.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::catalog::Catalog;
use crate::config::Config;
use crate::feed::{self, WorkerCommand, WorkerHandle, WorkerMode, WorkerNotice, WorkerSpec};
use crate::kalshi::auth::KalshiSigner;
use crate::pipeline::BookEvent;

/// One primary socket and its standby, plus the markets they own.
struct Shard {
    id: usize,
    symbols: Vec<String>,
    primary: WorkerHandle,
    standby: Option<WorkerHandle>,
    next_recycle: Instant,
}

impl Shard {
    fn handles(&self) -> impl Iterator<Item = &WorkerHandle> {
        std::iter::once(&self.primary).chain(self.standby.iter())
    }

    fn handle(&self, index: usize) -> Option<&WorkerHandle> {
        self.handles().nth(index)
    }

    /// Index of the socket currently trusted to write: 0 is the primary, 1 the standby.
    fn source_index(&self) -> Option<usize> {
        self.handles().position(|handle| handle.stats.is_source())
    }

    fn source(&self) -> Option<&WorkerHandle> {
        self.handles().find(|handle| handle.stats.is_source())
    }
}

pub struct Supervisor {
    cfg: Config,
    catalog: Catalog,
    events: mpsc::Sender<BookEvent>,
    shutdown_tx: watch::Sender<bool>,
    shards: Vec<Shard>,
    lifecycle: WorkerHandle,
    /// Reverse index so a notice about one ticker does not require scanning every shard.
    shard_of: HashMap<String, usize>,
}

impl Supervisor {
    /// Builds the shard layout from the live catalog and starts every socket.
    pub async fn start(
        cfg: Config,
        signer: Arc<KalshiSigner>,
        events: mpsc::Sender<BookEvent>,
        notices_tx: mpsc::Sender<WorkerNotice>,
        shutdown_tx: watch::Sender<bool>,
    ) -> anyhow::Result<Self> {
        let catalog = Catalog::new(cfg.env, signer.clone())?;
        let symbols = catalog.open_symbols().await?;
        info!(
            symbols = symbols.len(),
            shards = cfg.sockets,
            redundancy = cfg.redundancy,
            "starting supervisor"
        );

        // Round-robin rather than contiguous ranges: markets that share an event tend to
        // be listed together, and round-robin spreads those correlated books across
        // sockets instead of loading them all onto one.
        let mut buckets: Vec<Vec<String>> = vec![Vec::new(); cfg.sockets.max(1)];
        let shard_count = buckets.len();
        for (index, symbol) in symbols.into_iter().enumerate() {
            buckets[index % shard_count].push(symbol);
        }

        let mut shards = Vec::with_capacity(buckets.len());
        let mut shard_of = HashMap::new();

        for (id, mut bucket) in buckets.into_iter().enumerate() {
            bucket.sort();
            for symbol in &bucket {
                shard_of.insert(symbol.clone(), id);
            }
            let primary = spawn_worker(
                &cfg,
                &signer,
                &events,
                &notices_tx,
                &shutdown_tx,
                WorkerSpec {
                    id: (id as u64) * 2,
                    mode: WorkerMode::Orderbook,
                    symbols: bucket.clone(),
                    subscribe_batch: cfg.subscribe_batch,
                    env: cfg.env,
                    source: true,
                },
            );
            let standby = cfg.redundancy.then(|| {
                spawn_worker(
                    &cfg,
                    &signer,
                    &events,
                    &notices_tx,
                    &shutdown_tx,
                    WorkerSpec {
                        id: (id as u64) * 2 + 1,
                        mode: WorkerMode::Orderbook,
                        symbols: bucket.clone(),
                        subscribe_batch: cfg.subscribe_batch,
                        env: cfg.env,
                        source: false,
                    },
                )
            });
            shards.push(Shard {
                id,
                symbols: bucket,
                primary,
                standby,
                next_recycle: Instant::now() + cfg.recycle_after,
            });
        }

        // One dedicated lifecycle connection. It holds no books; it only reports when the
        // universe changes, which keeps discovery off the orderbook sockets.
        let lifecycle = spawn_worker(
            &cfg,
            &signer,
            &events,
            &notices_tx,
            &shutdown_tx,
            WorkerSpec {
                id: u64::MAX,
                mode: WorkerMode::Lifecycle,
                symbols: Vec::new(),
                subscribe_batch: cfg.subscribe_batch,
                env: cfg.env,
                source: true,
            },
        );

        Ok(Self {
            cfg,
            catalog,
            events,
            shutdown_tx,
            shards,
            lifecycle,
            shard_of,
        })
    }

    /// Runs until the process is asked to stop.
    pub async fn run(mut self, notices_rx: &mut mpsc::Receiver<WorkerNotice>) {
        let mut monitor = tokio::time::interval(Duration::from_secs(1));
        monitor.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut catalog_tick = tokio::time::interval(self.cfg.catalog_refresh);
        catalog_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                notice = notices_rx.recv() => {
                    let Some(notice) = notice else { break };
                    self.handle_notice(notice).await;
                }
                _ = monitor.tick() => {
                    self.monitor_sockets().await;
                }
                _ = catalog_tick.tick() => {
                    self.refresh_catalog().await;
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }

        self.stop().await;
    }

    async fn handle_notice(&mut self, notice: WorkerNotice) {
        match notice {
            WorkerNotice::Gap {
                worker,
                sid,
                expected,
                got,
            } => {
                warn!(worker, sid, expected, got, "sequence gap, resyncing shard");
                self.resync_worker_shard(worker).await;
            }
            WorkerNotice::ExchangeError {
                worker,
                code,
                message,
            } => {
                // Kalshi returns per-command errors, including subscription-limit
                // violations. Neither is fatal: the socket recovers or the reconnect
                // loop replaces it, and a shard that keeps failing is a visible log line
                // rather than silent data loss.
                warn!(worker, code, message = %message, "exchange error");
            }
            WorkerNotice::Lifecycle { ticker, event_type } => {
                self.apply_lifecycle(&ticker, &event_type).await;
            }
        }
    }

    /// Promotes a standby when the source is dead or silent, and recycles sockets on a
    /// timer so cumulative exchange-side bugs cannot outlive a restart.
    async fn monitor_sockets(&mut self) {
        for shard in &mut self.shards {
            let stale = self.cfg.stale_after;

            let source_healthy = shard
                .source()
                .map(|handle| healthy(&handle.stats, stale))
                .unwrap_or(false);

            if !source_healthy {
                // Pick the healthiest socket rather than blindly the other one: a single
                // healthy primary should not be replaced by a dead standby.
                let best = shard
                    .handles()
                    .enumerate()
                    .filter(|(_, handle)| healthy(&handle.stats, stale))
                    .min_by_key(|(_, handle)| {
                        handle.stats.millis_since_message().unwrap_or(i64::MAX)
                    })
                    .map(|(index, _)| index);

                match best {
                    Some(index) => Self::switch_source(shard, index).await,
                    None => debug!(shard = shard.id, "no healthy socket, waiting for reconnect"),
                }
            }

            if Instant::now() >= shard.next_recycle {
                // Recycle the socket that is *not* serving, so the live stream never has
                // to pause for a routine restart. Under sustained failover the roles
                // alternate, so both sockets still get recycled.
                let victim = match shard.source_index() {
                    Some(0) => shard.standby.as_ref(),
                    Some(_) => Some(&shard.primary),
                    None => Some(&shard.primary),
                };
                if let Some(victim) = victim {
                    info!(shard = shard.id, worker = victim.id, "proactive recycle");
                    victim.send(WorkerCommand::Recycle).await;
                }
                shard.next_recycle = Instant::now() + self.cfg.recycle_after;
            }
        }
    }

    /// Makes `index` the writing socket and asks it for authoritative snapshots.
    async fn switch_source(shard: &mut Shard, index: usize) {
        let Some(target) = shard.handle(index) else {
            return;
        };
        let target_id = target.id;
        for (position, handle) in shard.handles().enumerate() {
            handle.stats.set_source(position == index);
        }
        info!(
            shard = shard.id,
            worker = target_id,
            "switched source socket, requesting snapshots"
        );
        // A promoted socket may have been idle long after the sink's book was built, so
        // its next delta would apply to a book it never contributed to. Snapshots reset
        // the book and set the clock floor, which makes the handover lossless.
        target
            .send(WorkerCommand::Snapshot(shard.symbols.clone()))
            .await;
    }

    /// Reacts to `market_lifecycle_v2`.
    async fn apply_lifecycle(&mut self, ticker: &str, event_type: &str) {
        match event_type {
            "market_activated" | "market_created" => {
                if !self.shard_of.contains_key(ticker) {
                    self.add_symbol(ticker.to_owned()).await;
                }
            }
            "market_deactivated" | "market_settled" | "market_determined" => {
                self.remove_symbol(ticker).await;
            }
            // `market_closed` only stops new orders; the book still has resting liquidity
            // and the exchange keeps publishing deltas until settlement, so it is not a
            // reason to drop the market.
            _ => {}
        }
    }

    async fn add_symbol(&mut self, ticker: String) {
        // Least-loaded shard, so a burst of new listings cannot pile onto one socket.
        let Some(index) = self
            .shards
            .iter()
            .enumerate()
            .min_by_key(|(_, shard)| shard.symbols.len())
            .map(|(index, _)| index)
        else {
            return;
        };
        let shard = &mut self.shards[index];
        shard.symbols.push(ticker.clone());
        shard.symbols.sort();
        self.shard_of.insert(ticker.clone(), index);
        for handle in shard.handles() {
            handle
                .send(WorkerCommand::Subscribe(vec![ticker.clone()]))
                .await;
        }
        info!(symbol = %ticker, shard = index, "added market");
    }

    async fn remove_symbol(&mut self, ticker: &str) {
        let Some(index) = self.shard_of.remove(ticker) else {
            return;
        };
        let shard = &mut self.shards[index];
        shard.symbols.retain(|symbol| symbol != ticker);
        for handle in shard.handles() {
            handle
                .send(WorkerCommand::Unsubscribe(vec![ticker.to_owned()]))
                .await;
        }
        // Dropping the book matters as much as unsubscribing: a settled market's levels
        // are not truth any more, and keeping them inflates every later snapshot.
        let _ = self
            .events
            .send(BookEvent::Drop {
                symbol: ticker.to_owned(),
            })
            .await;
        info!(symbol = %ticker, "removed market");
    }

    /// Reconciles the universe against the REST catalog. Idempotent by construction:
    /// adding a known ticker or removing an unknown one is a no-op.
    async fn refresh_catalog(&mut self) {
        let symbols = match self.catalog.open_symbols().await {
            Ok(symbols) => symbols,
            Err(err) => {
                warn!(error = %err, "catalog refresh failed, keeping current universe");
                return;
            }
        };

        let wanted: HashSet<String> = symbols.into_iter().collect();
        let known: HashSet<String> = self.shard_of.keys().cloned().collect();

        for ticker in wanted.difference(&known) {
            self.add_symbol(ticker.clone()).await;
        }
        for ticker in known.difference(&wanted) {
            self.remove_symbol(ticker).await;
        }
    }

    /// A worker reporting a gap means its shard's books are missing messages. Ask the
    /// serving socket for fresh snapshots of that shard.
    async fn resync_worker_shard(&mut self, worker: u64) {
        let shard = self
            .shards
            .iter_mut()
            .find(|shard| shard.handles().any(|handle| handle.id == worker));
        let Some(shard) = shard else { return };
        if let Some(source) = shard.source() {
            source
                .send(WorkerCommand::Snapshot(shard.symbols.clone()))
                .await;
        }
    }

    async fn stop(mut self) {
        info!(shards = self.shards.len(), "shutting down sockets");
        let _ = self.shutdown_tx.send(true);
        for shard in std::mem::take(&mut self.shards) {
            shard.primary.shutdown().await;
            if let Some(standby) = shard.standby {
                standby.shutdown().await;
            }
        }
        self.lifecycle.shutdown().await;
    }
}

fn spawn_worker(
    cfg: &Config,
    signer: &Arc<KalshiSigner>,
    events: &mpsc::Sender<BookEvent>,
    notices: &mpsc::Sender<WorkerNotice>,
    shutdown: &watch::Sender<bool>,
    spec: WorkerSpec,
) -> WorkerHandle {
    let _ = cfg;
    feed::spawn(
        spec,
        signer.clone(),
        events.clone(),
        notices.clone(),
        shutdown.subscribe(),
    )
}

/// A socket is usable when it is connected and has produced a frame recently. Both
/// halves matter: a socket that never connects is obvious, but one that connects and
/// then goes quiet is the failure mode that silently corrupts an order book.
fn healthy(stats: &feed::WorkerStats, stale_after: Duration) -> bool {
    if !stats.is_connected() {
        return false;
    }
    match stats.millis_since_message() {
        Some(millis) => millis >= 0 && Duration::from_millis(millis as u64) <= stale_after,
        None => false,
    }
}

/// Startup summary, logged once so an operator can see the shard shape without reading
/// the code.
pub fn describe_layout(cfg: &Config) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    map.insert("env".to_owned(), format!("{:?}", cfg.env));
    map.insert("shards".to_owned(), cfg.sockets.to_string());
    map.insert("redundancy".to_owned(), cfg.redundancy.to_string());
    map.insert(
        "subscribe_batch".to_owned(),
        cfg.subscribe_batch.to_string(),
    );
    map.insert(
        "snapshot_interval_secs".to_owned(),
        cfg.snapshot_interval.as_secs().to_string(),
    );
    map
}
