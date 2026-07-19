//! A follower that pulls from a primary over the network.
//!
//! # Pull, not push
//!
//! The follower asks for the position it needs. A pushing primary has to track what every
//! follower has, and a follower that misses a push has no way to say so — it simply falls
//! behind silently. Pulling makes the follower's position the single source of truth about
//! its own progress, and a missed round trip costs nothing but a retry.
//!
//! # Bootstrap is a normal outcome, not a failure
//!
//! Retention is bounded, so a follower that falls far enough behind will be told
//! `NeedsBootstrap`. That is the design working: the primary refuses to serve a stream with
//! a hole in it, and the follower takes a snapshot instead. It is counted so that a follower
//! bootstrapping *repeatedly* — which means retention is too small for the write rate — is
//! visible rather than merely slow.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::error::{Error, Result};
use crate::replication::bootstrap::{SnapshotId, SnapshotTransfer};
use crate::replication::{Follower, StreamTxn};
use crate::shard::ShardId;

use super::client::Client;
use super::protocol::{Request, Response};

#[derive(Debug, Clone)]
pub struct ReplicaConfig {
    pub primary_addr: String,
    /// Shards to follow.
    pub shards: Vec<ShardId>,
    /// How long to wait when there is nothing new, before asking again.
    pub poll_interval: Duration,
    /// Transactions per subscription response.
    pub batch: u32,
    /// Bytes per snapshot chunk.
    pub snapshot_chunk: usize,
    /// Where partial snapshot transfers are staged.
    pub staging_dir: std::path::PathBuf,
}

#[derive(Debug, Default)]
struct Counters {
    applied_txns: AtomicU64,
    bootstraps: AtomicU64,
    polls: AtomicU64,
    errors: AtomicU64,
    up_to_date: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaStats {
    pub applied_txns: u64,
    /// Bootstraps performed. Repeated bootstraps mean retention is too small for the write
    /// rate — a tuning signal, not a fault.
    pub bootstraps: u64,
    pub polls: u64,
    pub up_to_date: u64,
    pub errors: u64,
}

pub struct Replica {
    cfg: ReplicaConfig,
    follower: Arc<Follower>,
    counters: Arc<Counters>,
    stop: Arc<AtomicBool>,
}

impl Replica {
    pub fn new(cfg: ReplicaConfig, follower: Arc<Follower>) -> Self {
        Self {
            cfg,
            follower,
            counters: Arc::new(Counters::default()),
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn stats(&self) -> ReplicaStats {
        ReplicaStats {
            applied_txns: self.counters.applied_txns.load(Ordering::Relaxed),
            bootstraps: self.counters.bootstraps.load(Ordering::Relaxed),
            polls: self.counters.polls.load(Ordering::Relaxed),
            up_to_date: self.counters.up_to_date.load(Ordering::Relaxed),
            errors: self.counters.errors.load(Ordering::Relaxed),
        }
    }

    pub fn stop_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.stop)
    }

    pub fn follower(&self) -> &Arc<Follower> {
        &self.follower
    }

    /// Catch every shard up once, then return.
    ///
    /// Separate from [`Self::run`] so callers — and tests — can drive replication a step at
    /// a time rather than only as an endless loop.
    pub fn sync_once(&self) -> Result<()> {
        let mut client = Client::connect(&self.cfg.primary_addr)?;
        for shard in self.cfg.shards.clone() {
            self.sync_shard(&mut client, shard)?;
        }
        Ok(())
    }

    /// Follow until stopped.
    pub fn run(&self) -> Result<()> {
        while !self.stop.load(Ordering::Relaxed) {
            match self.sync_once() {
                Ok(()) => {}
                Err(e) => {
                    self.counters.errors.fetch_add(1, Ordering::Relaxed);
                    // A primary that is down or restarting is expected, not exceptional; the
                    // loop keeps trying rather than exiting and needing supervision.
                    tracing::warn!(error = %e, "replication cycle failed; will retry");
                }
            }
            std::thread::sleep(self.cfg.poll_interval);
        }
        Ok(())
    }

    fn sync_shard(&self, client: &mut Client, shard: ShardId) -> Result<()> {
        loop {
            if self.stop.load(Ordering::Relaxed) {
                return Ok(());
            }

            // Claim *this follower's* epoch, not the primary's. Asking the primary what
            // epoch to claim and then claiming it would make the primary's epoch check
            // vacuous — it would be comparing its own answer against itself, and a follower
            // holding a copy from an older generation would be fed a newer generation's
            // frames as though they continued its own. A fresh follower has epoch 0, which
            // claims nothing and lets the primary decide.
            let pos = self.follower.position(shard);
            let from = pos.applied_lsn + 1;

            self.counters.polls.fetch_add(1, Ordering::Relaxed);
            match client.subscribe(shard.0, pos.epoch, from, self.cfg.batch)? {
                Response::Frames { txns, .. } if txns.is_empty() => return Ok(()),
                // The epoch recorded is the one the frames actually came from, not the one
                // this follower asked with — those differ for a fresh follower, and
                // recording the request's epoch would stamp a copy with a generation it
                // does not belong to.
                Response::Frames { epoch, txns, .. } => {
                    let n = txns.len();
                    self.apply(shard, epoch, txns)?;
                    self.counters
                        .applied_txns
                        .fetch_add(n as u64, Ordering::Relaxed);
                    // A full batch probably means there is more; keep going rather than
                    // waiting a poll interval per batch while behind.
                    if n < self.cfg.batch as usize {
                        return Ok(());
                    }
                }
                Response::UpToDate { .. } => {
                    self.counters.up_to_date.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                Response::NeedsBootstrap { reason, .. } => {
                    tracing::info!(%shard, reason, "bootstrapping");
                    self.bootstrap(client, shard)?;
                    self.counters.bootstraps.fetch_add(1, Ordering::Relaxed);
                    // Loop again to pick up whatever arrived during the copy.
                }
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected response to subscribe: {other:?}"
                    )));
                }
            }
        }
    }

    fn apply(&self, shard: ShardId, epoch: u64, txns: Vec<StreamTxn>) -> Result<()> {
        // The follower refuses a non-contiguous batch, so a gap surfaces here rather than
        // being written into the database.
        self.follower.apply(shard, epoch, &txns)
    }

    /// Copy a shard from the primary and install it.
    ///
    /// The transfer is resumable, so an interruption mid-copy continues rather than
    /// restarting — and the snapshot's identity is carried through, so a resume against a
    /// *different* snapshot is discarded rather than spliced.
    fn bootstrap(&self, client: &mut Client, shard: ShardId) -> Result<()> {
        let (epoch, lsn, total) = client.snapshot_begin(shard.0)?;

        let id = SnapshotId {
            shard,
            epoch,
            lsn,
            total_bytes: total,
        };
        let result = (|| -> Result<()> {
            let mut transfer = SnapshotTransfer::begin(&self.cfg.staging_dir, id)?;
            while !transfer.is_complete() {
                let chunk =
                    client.snapshot_read(shard.0, transfer.offset(), self.cfg.snapshot_chunk)?;
                if chunk.is_empty() {
                    return Err(Error::Protocol(format!(
                        "the primary stopped sending at {} of {total} bytes",
                        transfer.offset()
                    )));
                }
                transfer.write_chunk(&chunk)?;
            }
            transfer.finish(&self.follower)
        })();

        // Release the freeze whatever happened, or the primary's checkpointing stays
        // suspended and its WAL grows without bound.
        let released = client.snapshot_end(shard.0);
        result?;
        released?;
        Ok(())
    }
}

impl Client {
    pub(crate) fn subscribe(
        &mut self,
        shard: u32,
        epoch: u64,
        from_lsn: u64,
        max_txns: u32,
    ) -> Result<Response> {
        self.request(Request::Subscribe {
            shard,
            epoch,
            from_lsn,
            max_txns,
        })
    }

    /// Freeze a shard on the primary. Returns `(epoch, lsn, total_bytes)`.
    pub(crate) fn snapshot_begin(&mut self, shard: u32) -> Result<(u64, u64, u64)> {
        match self.request(Request::SnapshotBegin { shard })? {
            Response::SnapshotInfo {
                epoch,
                lsn,
                total_bytes,
                ..
            } => Ok((epoch, lsn, total_bytes)),
            other => Err(Error::Protocol(format!(
                "unexpected response to snapshot_begin: {other:?}"
            ))),
        }
    }

    pub(crate) fn snapshot_read(&mut self, shard: u32, offset: u64, len: usize) -> Result<Vec<u8>> {
        match self.request(Request::SnapshotRead {
            shard,
            offset,
            len: len as u32,
        })? {
            Response::SnapshotChunk { data } => Ok(data),
            other => Err(Error::Protocol(format!(
                "unexpected response to snapshot_read: {other:?}"
            ))),
        }
    }

    pub(crate) fn snapshot_end(&mut self, shard: u32) -> Result<()> {
        match self.request(Request::SnapshotEnd { shard })? {
            Response::Ok => Ok(()),
            other => Err(Error::Protocol(format!(
                "unexpected response to snapshot_end: {other:?}"
            ))),
        }
    }
}
