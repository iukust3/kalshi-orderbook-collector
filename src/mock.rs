//! Synthetic traffic for `MOCK=1`.
//!
//! Two reasons this exists. First, the output contract can be validated with no
//! credentials and no network, which is how the SBE stream is exercised in CI. Second,
//! it lets the hot-standby path be tested honestly: the mock has no second socket, so a
//! run with `MOCK=1` proves the writer, the ordering and the dedupe, while a run against
//! Kalshi proves the failover.
//!
//! The generated stream is deliberately shaped like the real one: an initial snapshot per
//! market, then signed deltas that add, resize and remove levels, so a replay sees levels
//! appear and disappear rather than a monotonic book.

use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use tokio::sync::{mpsc, watch};
use tracing::info;

use crate::book::SCALE;
use crate::pipeline::BookEvent;

/// Deterministic, dependency-free pseudo random source. A fixed seed keeps mock runs
/// reproducible, which matters when comparing two runs of the writer.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*: adequate for traffic shaping, and stable across platforms.
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 { 0 } else { self.next() % bound }
    }
}

pub async fn run(
    symbols: usize,
    events: mpsc::Sender<BookEvent>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut rng = Rng(0x5EED_1234_ABCD_0001);
    let tickers: Vec<String> = (0..symbols.max(1))
        .map(|index| format!("MOCK-{index:03}"))
        .collect();
    info!(symbols = tickers.len(), "mock traffic enabled");

    // Initial snapshots, so every market starts from a complete book.
    for ticker in &tickers {
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        for level in 0..5u64 {
            let price = (500_000 - level as i64 * 10_000).max(1);
            bids.push((price, ((level + 1) * 100) as i64 * SCALE / 100));
            asks.push((price + 10_000, ((level + 1) * 100) as i64 * SCALE / 100));
        }
        let now = Utc::now();
        events
            .send(BookEvent::Reset {
                symbol: ticker.clone(),
                bids,
                asks,
                clock: Some(0),
                received_at: now,
                exchange_timestamp: now,
                worker: u64::MAX,
            })
            .await
            .context("mock pipeline closed during snapshot")?;
    }

    let mut tick = tokio::time::interval(Duration::from_millis(50));
    let mut clock = 1u64;

    loop {
        // Same stop path as a live run, so `MOCK=1` exercises the drain behaviour.
        tokio::select! {
            _ = tick.tick() => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("mock traffic stopping");
                    return Ok(());
                }
            }
        }

        let ticker = &tickers[rng.below(tickers.len() as u64) as usize];
        let price = 500_000 - (rng.below(5) as i64) * 10_000;
        let side = if rng.below(2) == 0 {
            crate::Side::Buy
        } else {
            crate::Side::Sell
        };
        let delta = ((rng.below(300) as i64) - 150) * SCALE / 100;
        let now = Utc::now();
        clock += 1;

        events
            .send(BookEvent::Delta {
                symbol: ticker.clone(),
                side,
                price,
                delta,
                clock: Some(clock),
                received_at: now,
                exchange_timestamp: now,
                worker: u64::MAX,
            })
            .await
            .context("mock pipeline closed during delta")?;
    }
}
