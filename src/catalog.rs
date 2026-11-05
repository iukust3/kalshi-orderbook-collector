//! The symbol universe.
//!
//! A collector that only subscribes to a hand-written list of tickers is stale the moment
//! Kalshi lists a new market, and it keeps paying for markets that have settled. So the
//! universe is derived from the exchange itself: `GET /markets` filtered to open markets,
//! paginated until the cursor is exhausted.
//!
//! The REST call is signed with the same RSA key as the websocket handshake, because
//! `GET /markets` is cheap but rate limited per key and an unsigned request gets a 401 on
//! the authenticated tier.
//!
//! Nothing here is authoritative about *state*: the catalog only answers "which tickers
//! exist". Books come from the websocket, and reconciliation between the two happens in
//! [`crate::supervisor`].

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use serde::Deserialize;
use tracing::{debug, warn};

use crate::config::KalshiEnv;
use crate::kalshi::auth::KalshiSigner;

/// Signed path, without the host. Kalshi signs the endpoint path, not the full URL.
const MARKETS_PATH: &str = "/trade-api/v2/markets";

/// Kalshi caps `limit` at 1000. Using the maximum keeps the cursor loop short, which
/// matters because every page is a rate-limited request.
const PAGE_LIMIT: usize = 1000;

#[derive(Debug, Deserialize)]
struct MarketsResponse {
    #[serde(default)]
    markets: Vec<Market>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Market {
    ticker: String,
    #[serde(default)]
    status: Option<String>,
}

pub struct Catalog {
    client: reqwest::Client,
    env: KalshiEnv,
    signer: Arc<KalshiSigner>,
}

impl Catalog {
    pub fn new(env: KalshiEnv, signer: Arc<KalshiSigner>) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .user_agent("voluntas-orderbook-collector/1.0")
            .build()
            .context("cannot build HTTP client")?;
        Ok(Self {
            client,
            env,
            signer,
        })
    }

    /// Every open ticker, sorted so that shard assignment is deterministic across
    /// restarts. A stable order means a restarted collector rebuilds the same shards
    /// instead of reshuffling every market onto a different socket.
    pub async fn open_symbols(&self) -> anyhow::Result<Vec<String>> {
        let mut symbols = Vec::new();
        let mut cursor: Option<String> = None;
        let mut pages = 0usize;

        loop {
            let mut url = format!(
                "{}/markets?status=open&limit={PAGE_LIMIT}",
                self.env.rest_base()
            );
            if let Some(cursor) = &cursor {
                url.push_str("&cursor=");
                url.push_str(cursor);
            }

            let mut request = self.client.get(&url);
            for (name, value) in self.signer.headers("GET", MARKETS_PATH)? {
                request = request.header(name, value);
            }

            let response = request
                .send()
                .await
                .with_context(|| format!("GET {url} failed"))?;
            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                anyhow::bail!("GET {url} returned {status}: {body}");
            }

            let page: MarketsResponse = response
                .json()
                .await
                .with_context(|| format!("GET {url} returned an unexpected body"))?;

            for market in page.markets {
                // `status=open` is a server-side filter, but Kalshi can return a market
                // that settled between the filter and the page read. Trust the field.
                if market.status.as_deref() == Some("open") || market.status.is_none() {
                    symbols.push(market.ticker);
                }
            }

            pages += 1;
            match page.cursor {
                Some(next) if !next.is_empty() => cursor = Some(next),
                _ => break,
            }
        }

        symbols.sort();
        symbols.dedup();
        debug!(pages, symbols = symbols.len(), "catalog fetched");
        if symbols.is_empty() {
            warn!("catalog returned no open markets");
        }
        Ok(symbols)
    }
}
