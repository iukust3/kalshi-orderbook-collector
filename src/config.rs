//! Runtime configuration, entirely from environment variables so the same binary
//! runs against Kalshi production, the Kalshi demo environment, or a file sink in
//! tests without a rebuild.

use std::{env, path::PathBuf, time::Duration};

/// Kalshi venue pair (REST + websocket) for one of the two public environments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KalshiEnv {
    Prod,
    Demo,
}

impl KalshiEnv {
    pub fn rest_base(self) -> &'static str {
        match self {
            Self::Prod => "https://api.elections.kalshi.com/trade-api/v2",
            // Kalshi's recommended demo root; `demo-api.kalshi.co` also works.
            Self::Demo => "https://external-api.demo.kalshi.co/trade-api/v2",
        }
    }

    pub fn ws_url(self) -> &'static str {
        match self {
            Self::Prod => "wss://api.elections.kalshi.com/trade-api/ws/v2",
            Self::Demo => "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2",
        }
    }

    /// Request path used when signing. It is part of the signed payload, so it has
    /// to match the websocket endpoint exactly.
    pub fn ws_path(self) -> &'static str {
        "/trade-api/ws/v2"
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub env: KalshiEnv,
    pub api_key_id: Option<String>,
    pub private_key_path: Option<PathBuf>,

    /// Where the SBE stream goes. `/dev/stdout` is the challenge's required sink;
    /// a real path is accepted so the same code can write to disk.
    pub output_path: PathBuf,
    /// Fixed chunk size of the aligned output file. Also the worst-case padding
    /// written per flush, so it trades snapshot depth for flush granularity.
    pub chunk_bytes: usize,
    /// How often the pipeline considers pushing buffered messages to the sink. A flush
    /// pads the current chunk, so it is only worth doing once enough is buffered.
    pub flush_interval: Duration,
    /// Minimum buffered bytes before a scheduled flush is worth its padding cost.
    pub flush_min_bytes: usize,
    /// Full snapshot cadence, per the challenge spec.
    pub snapshot_interval: Duration,

    /// Number of orderbook shards. Each shard owns roughly
    /// `symbols / sockets` markets across one primary and one standby socket.
    pub sockets: usize,
    /// Keep a hot standby socket per shard and switch source on failure.
    pub redundancy: bool,
    /// Markets per `subscribe` command; Kalshi rate limits per message.
    pub subscribe_batch: usize,
    /// A source socket with no traffic for this long, while its standby is
    /// receiving, is considered silently dead.
    pub stale_after: Duration,
    /// Proactively rebuild a socket after this long to shake off cumulative
    /// exchange-side bugs.
    pub recycle_after: Duration,
    /// How often the REST catalog is polled for symbol additions and removals.
    pub catalog_refresh: Duration,
    /// Bounded event channel between socket workers and the book actor.
    pub event_buffer: usize,
    /// Demo mode: emit synthetic traffic instead of connecting, so the output
    /// format can be validated with no credentials.
    pub mock: bool,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let env_name = env::var("KALSHI_ENV").unwrap_or_else(|_| "prod".to_owned());
        let env = match env_name.to_ascii_lowercase().as_str() {
            "prod" | "production" => KalshiEnv::Prod,
            "demo" => KalshiEnv::Demo,
            other => anyhow::bail!("KALSHI_ENV must be 'prod' or 'demo', got {other:?}"),
        };

        let mock = flag("MOCK");

        Ok(Self {
            env,
            api_key_id: env::var("KALSHI_API_KEY_ID").ok().filter(|v| !v.is_empty()),
            private_key_path: env::var("KALSHI_PRIVATE_KEY_PATH")
                .ok()
                .filter(|v| !v.is_empty())
                .map(PathBuf::from),
            output_path: env::var("OUTPUT_PATH")
                .unwrap_or_else(|_| "/dev/stdout".to_owned())
                .into(),
            // Must match AlignedSbeReader's chunk size (64 MiB), otherwise a reader
            // walks the file in the wrong stride and sees zeros where messages are.
            chunk_bytes: number("OUTPUT_CHUNK_BYTES", 64 << 20)?,
            flush_interval: Duration::from_millis(number("OUTPUT_FLUSH_MS", 1000)? as u64),
            flush_min_bytes: number("OUTPUT_FLUSH_MIN_BYTES", 8 << 20)?,
            snapshot_interval: Duration::from_secs(number("SNAPSHOT_INTERVAL_SECS", 60)? as u64),
            sockets: number("SOCKETS", 4)?.max(1),
            redundancy: !flag("NO_REDUNDANCY"),
            subscribe_batch: number("SUBSCRIBE_BATCH", 100)?.max(1),
            stale_after: Duration::from_secs(number("STALE_SECS", 15)? as u64),
            recycle_after: Duration::from_secs(number("RECYCLE_SECS", 1800)? as u64),
            catalog_refresh: Duration::from_secs(number("CATALOG_REFRESH_SECS", 60)? as u64),
            event_buffer: number("EVENT_BUFFER", 262_144)?,
            mock,
        })
    }

    /// Fails fast with an actionable message instead of a confusing auth error.
    pub fn require_credentials(&self) -> anyhow::Result<(String, PathBuf)> {
        match (&self.api_key_id, &self.private_key_path) {
            (Some(id), Some(path)) => Ok((id.clone(), path.clone())),
            _ => anyhow::bail!(
                "missing Kalshi credentials: set KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH \
                 (or run with MOCK=1 to exercise the writer without a venue connection)"
            ),
        }
    }
}

fn number(key: &str, default: usize) -> anyhow::Result<usize> {
    match env::var(key) {
        Ok(raw) if !raw.trim().is_empty() => raw
            .trim()
            .parse::<usize>()
            .map_err(|e| anyhow::anyhow!("{key} must be a positive integer: {e}")),
        _ => Ok(default),
    }
}

fn flag(key: &str) -> bool {
    matches!(
        env::var(key).map(|v| v.to_ascii_lowercase()),
        Ok(v) if v == "1" || v == "true" || v == "yes"
    )
}
