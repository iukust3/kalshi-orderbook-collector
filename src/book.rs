//! In-memory order book for a single instrument, kept in exchange fixed-point.
//!
//! The book is the collector's authoritative view of each market, and it exists for
//! one reason: a [`PriceLevelUpdate`](crate::util::dtos::challenge_orderbook::PriceLevelUpdateDomain)
//! carries the *absolute* quantity at a price level after the change, not the source
//! delta. Applying the delta here is what lets the sink emit a self-consistent stream
//! that a backtester can replay without knowing the venue's protocol.

use std::collections::BTreeMap;

use crate::Side;
use crate::util::dtos::challenge_orderbook::{
    SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};
use chrono::{DateTime, Utc};

/// The schema's fixed-point scale: 1_000_000 == 1 USDC per share.
pub const SCALE: i64 = 1_000_000;

/// One side of one instrument: price level -> absolute resting quantity.
type Levels = BTreeMap<i64, i64>;

#[derive(Debug, Default, Clone)]
pub struct OrderBook {
    bids: Levels,
    asks: Levels,
}

impl OrderBook {
    /// Replaces the whole book. Used for startup and for every resync: an exchange
    /// snapshot is authoritative for every change up to its own clock.
    pub fn reset(
        &mut self,
        bids: impl IntoIterator<Item = (i64, i64)>,
        asks: impl IntoIterator<Item = (i64, i64)>,
    ) {
        self.bids = bids.into_iter().filter(|(_, qty)| *qty > 0).collect();
        self.asks = asks.into_iter().filter(|(_, qty)| *qty > 0).collect();
    }

    /// Applies a signed size change to one level and returns the resulting absolute
    /// quantity, with `0` meaning the level no longer exists.
    pub fn apply(&mut self, side: Side, price: i64, delta: i64) -> i64 {
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let current = levels.get(&price).copied().unwrap_or(0);
        let next = current.saturating_add(delta);
        if next <= 0 {
            // A level can go negative if we started from an incomplete book; clamp to
            // "gone" rather than inventing negative liquidity.
            levels.remove(&price);
            0
        } else {
            levels.insert(price, next);
            next
        }
    }

    /// Sets one level to an absolute size and returns the quantity now resting there.
    ///
    /// Some venues publish resting size rather than a signed change (Polymarket does).
    /// Treating that as a delta would double-count every update, so the two shapes are
    /// separate calls and the pipeline keeps them separate all the way to the sink.
    pub fn set_level(&mut self, side: Side, price: i64, quantity: i64) -> i64 {
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if quantity <= 0 {
            levels.remove(&price);
            0
        } else {
            levels.insert(price, quantity);
            quantity
        }
    }

    pub fn depth(&self) -> (usize, usize) {
        (self.bids.len(), self.asks.len())
    }

    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    pub fn snapshot(
        &self,
        symbol: &str,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    ) -> SnapshotDomain {
        SnapshotDomain {
            received_at,
            exchange_timestamp,
            bids: self
                .bids
                .iter()
                .map(|(price, qty)| SnapshotBidsEntryDomain {
                    price_level: *price,
                    quantity: *qty,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(price, qty)| SnapshotAsksEntryDomain {
                    price_level: *price,
                    quantity: *qty,
                })
                .collect(),
            symbol: symbol.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_adds_then_removes_level() {
        let mut book = OrderBook::default();
        assert_eq!(book.apply(Side::Buy, 500_000, 300 * SCALE), 300 * SCALE);
        assert_eq!(book.depth(), (1, 0));
        assert_eq!(book.apply(Side::Buy, 500_000, -300 * SCALE), 0);
        assert!(book.is_empty());
    }

    #[test]
    fn negative_level_is_dropped_not_stored() {
        let mut book = OrderBook::default();
        book.apply(Side::Sell, 400_000, 10);
        assert_eq!(book.apply(Side::Sell, 400_000, -50), 0);
        assert!(book.is_empty());
    }

    #[test]
    fn snapshot_keeps_sides_separate() {
        let mut book = OrderBook::default();
        book.reset([(480_000, 5)], [(520_000, 7)]);
        let now = Utc::now();
        let snap = book.snapshot("MKT", now, now);
        assert_eq!(snap.bids.len(), 1);
        assert_eq!(snap.asks.len(), 1);
        assert_eq!(snap.bids[0].price_level, 480_000);
        assert_eq!(snap.asks[0].quantity, 7);
    }
}
