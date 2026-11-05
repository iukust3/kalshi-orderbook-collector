//! Output side of the collector.
//!
//! The challenge fixes the wire format, so this module is deliberately thin: the only
//! decisions it owns are *where* bytes go and *how often* they are pushed. Everything
//! interesting (state, ordering, dedupe) lives in [`crate::pipeline`].
//!
//! [`Sink`] is a trait so the pipeline can be exercised end to end without touching a
//! real file, and so a future consumer (Kafka, S3, a test fixture) is a new impl rather
//! than a refactor.

use crate::util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain};
use anyhow::Context;

/// A message the collector emits, in the order it must appear in the stream.
#[derive(Debug)]
pub enum Outbound {
    Snapshot(Box<SnapshotDomain>),
    Update(PriceLevelUpdateDomain),
}

impl Outbound {
    pub fn is_snapshot(&self) -> bool {
        matches!(self, Self::Snapshot(_))
    }
}

/// How urgently the caller wants bytes out.
///
/// The wire format is chunk aligned, so a flush pads the current chunk to its full size.
/// That makes an eager flush expensive and a lazy one lossy, which is why the decision is
/// explicit rather than implicit in the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushMode {
    /// Flush only if enough is buffered for the padding to be worth it.
    IfWorthIt,
    /// Flush regardless: shutdown, and any request that must be observable now.
    Force,
}

pub trait Sink: Send {
    fn write(&mut self, message: Outbound) -> anyhow::Result<()>;
    /// Push buffered bytes out according to `mode`.
    fn flush(&mut self, mode: FlushMode) -> anyhow::Result<()>;
}

/// Writes FIX/SBE messages through the challenge's [`AlignedSbeWriter`].
///
/// The writer pads every chunk to a fixed size, which is what allows a reader to seek
/// and pre-fetch without a length prefixed framing layer. That padding is also the
/// reason [`crate::config::Config::chunk_bytes`] is not the template's 64 MiB: a
/// smaller chunk bounds the bytes wasted per flush, and flushes decide how stale the
/// stream is. 256 KiB keeps ~16k price levels per snapshot while costing at most
/// 512 KiB/s of padding at a 500 ms flush.
///
/// [`AlignedSbeWriter`]: crate::util::AlignedSbeWriter
pub struct SbeFileSink {
    writer: crate::util::AlignedSbeWriter,
    written: u64,
    min_flush_bytes: usize,
}

impl SbeFileSink {
    pub fn create(
        path: impl AsRef<std::path::Path>,
        chunk_bytes: usize,
        min_flush_bytes: usize,
    ) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let writer = crate::util::AlignedSbeWriter::create(path, chunk_bytes)
            .with_context(|| format!("cannot open output sink at {}", path.display()))?;
        Ok(Self {
            writer,
            written: 0,
            // A threshold smaller than one message would flush constantly; clamping keeps
            // the policy meaningful whatever an operator sets.
            min_flush_bytes: min_flush_bytes.min(chunk_bytes).max(1),
        })
    }

    pub fn messages_written(&self) -> u64 {
        self.written
    }
}

impl Sink for SbeFileSink {
    fn write(&mut self, message: Outbound) -> anyhow::Result<()> {
        match message {
            Outbound::Snapshot(snapshot) => self.writer.write_msg(snapshot.as_ref())?,
            Outbound::Update(update) => self.writer.write_msg(&update)?,
        }
        self.written += 1;
        Ok(())
    }

    fn flush(&mut self, mode: FlushMode) -> anyhow::Result<()> {
        let worth_it = self.writer.buffered_len() >= self.min_flush_bytes;
        if mode == FlushMode::Force || worth_it {
            self.writer.flush()?;
        }
        Ok(())
    }
}

/// In-memory sink used by tests and by `--self-test` to assert on the exact stream.
#[derive(Debug, Default)]
pub struct RecordingSink {
    pub messages: Vec<Outbound>,
    pub flushes: usize,
}

impl Sink for RecordingSink {
    fn write(&mut self, message: Outbound) -> anyhow::Result<()> {
        self.messages.push(message);
        Ok(())
    }

    fn flush(&mut self, _mode: FlushMode) -> anyhow::Result<()> {
        self.flushes += 1;
        Ok(())
    }
}
