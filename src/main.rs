//! Entry point.
//!
//! The binary is a thin composition root: it builds the sink, starts the book actor, and
//! chooses between a live Kalshi run and the offline mock. All of the interesting
//! behaviour lives in the modules it wires together:
//!
//! ```text
//! catalog (REST)  ─┐
//!                  ├─> supervisor ─> feed workers (N shards x primary+standby)
//! lifecycle (ws)  ─┘                        │
//!                                           v
//!                                    pipeline (single owner of state)
//!                                           │
//!                                           v
//!                                     SBE file / stdout
//! ```
//!
//! Configuration is entirely environment driven (see [`engineering_challenge::config`]),
//! so the same binary runs against production, the demo environment, or the mock without
//! a rebuild. `RUST_LOG=debug` turns on per-market tracing.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use engineering_challenge::config::Config;
use engineering_challenge::kalshi::auth::KalshiSigner;
use engineering_challenge::pipeline::{BookEvent, Pipeline};
use engineering_challenge::sink::SbeFileSink;
use engineering_challenge::supervisor::{self, Supervisor};
use engineering_challenge::{feed, mock};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let cfg = Config::from_env()?;

    for (key, value) in supervisor::describe_layout(&cfg) {
        info!(setting = %key, value = %value, "layout");
    }

    let sink = SbeFileSink::create(&cfg.output_path, cfg.chunk_bytes, cfg.flush_min_bytes)
        .with_context(|| format!("cannot open output at {}", cfg.output_path.display()))?;
    let pipeline = Pipeline::new(Box::new(sink), cfg.snapshot_interval, cfg.flush_interval);

    // Every worker writes into this one channel, which is what gives the stream a total
    // order. The buffer is large on purpose: a full buffer slows the socket readers
    // instead of dropping market data.
    let (events_tx, events_rx) = mpsc::channel::<BookEvent>(cfg.event_buffer);
    let (notices_tx, mut notices_rx) = mpsc::channel::<feed::WorkerNotice>(4096);
    let (shutdown_tx, _) = watch::channel(false);

    let pipeline_task = tokio::spawn(async move { pipeline.run(events_rx).await });

    let supervisor_task = if cfg.mock {
        info!("MOCK=1: generating synthetic traffic, no venue connection");
        let mock_events = events_tx.clone();
        let mock_shutdown = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            mock::run(8, mock_events, mock_shutdown).await
        }))
    } else {
        let (api_key_id, private_key_path) = cfg.require_credentials()?;
        let signer = Arc::new(
            KalshiSigner::from_file(api_key_id, &private_key_path)
                .context("cannot load the Kalshi API key")?,
        );
        let supervisor = Supervisor::start(
            cfg.clone(),
            signer,
            events_tx.clone(),
            notices_tx.clone(),
            shutdown_tx.clone(),
        )
        .await?;
        Some(tokio::spawn(async move {
            supervisor.run(&mut notices_rx).await;
            Ok(())
        }))
    };

    wait_for_stop_signal().await;
    info!("stop requested");

    // Order matters on the way out: stop producing first, then drain and flush the sink,
    // so the file ends with a complete final snapshot rather than a truncated one.
    let _ = shutdown_tx.send(true);
    if let Some(task) = supervisor_task {
        match tokio::time::timeout(Duration::from_secs(15), task).await {
            Ok(Ok(Ok(()))) => info!("producers stopped"),
            Ok(Ok(Err(err))) => warn!(error = %err, "producer ended with an error"),
            Ok(Err(err)) => warn!(error = %err, "producer task panicked"),
            Err(_) => warn!("producer did not stop within 15s"),
        }
    }

    let (drained_tx, drained_rx) = oneshot::channel();
    events_tx.send(BookEvent::Shutdown(drained_tx)).await.ok();
    let _ = tokio::time::timeout(Duration::from_secs(15), drained_rx).await;
    match tokio::time::timeout(Duration::from_secs(15), pipeline_task).await {
        Ok(Ok(())) => info!("sink flushed, exiting"),
        Ok(Err(err)) => warn!(error = %err, "pipeline task panicked"),
        Err(_) => warn!("pipeline did not stop within 15s"),
    }

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,engineering_challenge=info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

/// Ctrl-C, or SIGTERM on unix, so the container is stopped by the same drain path a
/// human interrupt uses.
async fn wait_for_stop_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(signal) => signal,
            Err(err) => {
                warn!(error = %err, "SIGTERM handler unavailable, waiting for ctrl-c only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
