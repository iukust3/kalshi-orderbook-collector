//! Orderbook collector for Kalshi.
//!
//! The crate is split so that each module has one reason to change:
//!
//! * [`book`] is pure state: an instrument's price levels, in exchange fixed point.
//! * [`pipeline`] is the single owner of every book and the only writer to the sink.
//! * [`feed`] owns one websocket connection and its subscription bookkeeping.
//! * [`supervisor`] decides shard membership, redundancy and repair.
//! * [`catalog`] discovers the symbol universe from the exchange's REST API.
//! * [`sink`] is the SBE output, behind a trait so tests can record instead of write.
//! * [`config`] turns environment variables into one validated struct.
//! * [`mock`] produces synthetic traffic so the stream can be exercised with no keys.

pub mod book;
pub mod catalog;
pub mod config;
pub mod feed;
pub mod kalshi;
pub mod mock;
pub mod pipeline;
pub mod polymarket;
pub mod sink;
pub mod supervisor;
pub mod util;

mod side;
pub use side::{InvalidSide, Side};
