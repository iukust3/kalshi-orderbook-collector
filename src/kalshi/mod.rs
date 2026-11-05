//! Kalshi-specific wire details: request signing and the websocket protocol.
//!
//! Everything venue-specific is confined to this module tree, so supporting a second
//! venue (see the Polymarket adapter in the write-up) means adding a sibling module
//! rather than editing the collector.

pub mod auth;
pub mod protocol;
