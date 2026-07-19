//! Where captured WAL frames go.
//!
//! Capture is only half the story. Frames accumulate in memory at roughly 1.5x the data
//! volume — measured at 15 MB for 20,000 rows — so without something consuming them,
//! turning capture on makes the memory budget strictly worse. This module is the consumer
//! boundary.
//!
//! # How backpressure works
//!
//! The writer thread drains its shard's capture **after every batch**, inside the same loop
//! that commits. So retained memory is bounded by one batch, not by the write rate. If a
//! sink cannot keep up, the drain slows the writer thread, the bounded write queue fills
//! behind it, and callers get [`crate::Error::WriterBusy`]. Backpressure therefore travels
//! back to clients through the mechanism that already exists, rather than through a second
//! one invented for replication.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::shard::ShardId;
use crate::vfs::CommittedTxn;

/// A destination for committed frames.
///
/// Implementations must be cheap: this is called on the writer thread, between
/// transactions. Slow work here directly reduces write throughput — which is the intended
/// backpressure, but it means a network sink should hand off to its own thread rather than
/// block on a round trip.
pub trait FrameSink: Send + Sync {
    /// Accept committed transactions for one shard, in commit order.
    ///
    /// Returning `Err` means the sink is unhealthy. The writer treats that as fatal for the
    /// node: the local database has committed data the sink never received, which is
    /// precisely the divergence physical replication exists to prevent.
    fn accept(&self, shard: ShardId, txns: Vec<CommittedTxn>) -> crate::Result<()>;
}

/// Discards everything, counting as it goes.
///
/// For a single-node deployment that has capture enabled only to measure its cost. Not a
/// replication target — it is exactly the thing that loses data silently, so it is named to
/// make that obvious at the call site.
#[derive(Debug, Default)]
pub struct NullSink {
    txns: AtomicU64,
    frames: AtomicU64,
    bytes: AtomicU64,
}

impl NullSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn counts(&self) -> (u64, u64, u64) {
        (
            self.txns.load(Ordering::Relaxed),
            self.frames.load(Ordering::Relaxed),
            self.bytes.load(Ordering::Relaxed),
        )
    }
}

impl FrameSink for NullSink {
    fn accept(&self, _shard: ShardId, txns: Vec<CommittedTxn>) -> crate::Result<()> {
        let frames: u64 = txns.iter().map(|t| t.frames.len() as u64).sum();
        let bytes: u64 = txns
            .iter()
            .flat_map(|t| t.frames.iter())
            .map(|f| f.data.len() as u64)
            .sum();
        self.txns.fetch_add(txns.len() as u64, Ordering::Relaxed);
        self.frames.fetch_add(frames, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }
}

/// Retains everything, per shard. For tests and for reconstructing a follower locally.
///
/// Unbounded by construction — only for test workloads whose size is known.
#[derive(Debug, Default)]
pub struct MemorySink {
    inner: Mutex<std::collections::BTreeMap<ShardId, Vec<CommittedTxn>>>,
}

impl MemorySink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn take(&self, shard: ShardId) -> Vec<CommittedTxn> {
        self.inner
            .lock()
            .expect("sink mutex")
            .remove(&shard)
            .unwrap_or_default()
    }

    pub fn txn_count(&self, shard: ShardId) -> usize {
        self.inner
            .lock()
            .expect("sink mutex")
            .get(&shard)
            .map(|v| v.len())
            .unwrap_or(0)
    }

    pub fn shards(&self) -> Vec<ShardId> {
        self.inner
            .lock()
            .expect("sink mutex")
            .keys()
            .copied()
            .collect()
    }
}

impl FrameSink for MemorySink {
    fn accept(&self, shard: ShardId, txns: Vec<CommittedTxn>) -> crate::Result<()> {
        self.inner
            .lock()
            .expect("sink mutex")
            .entry(shard)
            .or_default()
            .extend(txns);
        Ok(())
    }
}
