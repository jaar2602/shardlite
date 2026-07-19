//! Handing a shard's file between the replication path and SQLite.
//!
//! # Why this is a type and not four lines at the call site
//!
//! Promotion and demotion are each a short sequence whose **order is the whole correctness
//! argument**. Written inline they look trivial and are easy to get subtly wrong, and the
//! failure is silent:
//!
//! - Demoting must **close the write gate first**, then hand the file over. The other order
//!   leaves the writer committing SQL into a file the replication path has begun rewriting.
//! - Promoting must **wait for the pull loop to actually stop**, not merely ask it to.
//!   `stop()` sets a flag; the loop may be inside `apply` writing raw pages. Opening a
//!   connection then gives SQLite a file that changes underneath it.
//! - Marking a shard followed must happen **before** quiescing, and quiescing must happen at
//!   all: it is the surviving open handle, not the mark, that corrupts.
//!
//! # What this deliberately does not claim
//!
//! Nothing here can *verify* that the replication path has let go — a follower mid-apply
//! holds no lock SQLite can see. The waiting is what establishes it, which is why the wait
//! is bounded and reported rather than assumed.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::net::Replica;
use crate::shard::{ShardId, ShardManager};

use super::fence::Fence;
use super::term::Term;

/// Owns the handover of a node's shards between following and leading.
pub struct Promotion {
    manager: Arc<ShardManager>,
    replica: Arc<Replica>,
    fence: Arc<Fence>,
    shards: Vec<ShardId>,
    /// How long to wait for the pull loop to come to rest before giving up.
    settle: Duration,
}

impl Promotion {
    pub fn new(
        manager: Arc<ShardManager>,
        replica: Arc<Replica>,
        fence: Arc<Fence>,
        shards: Vec<ShardId>,
    ) -> Self {
        Self {
            manager,
            replica,
            fence,
            shards,
            settle: Duration::from_secs(5),
        }
    }

    pub fn with_settle_timeout(mut self, settle: Duration) -> Self {
        self.settle = settle;
        self
    }

    /// Give every shard to the replication path. The node becomes a follower.
    ///
    /// Safe to call when already following — it is the state this drives towards, not a
    /// transition, so a node that steps down twice does not need to care.
    pub fn follow(&self) -> Result<()> {
        // Writes stop first. Everything after this takes time, and for all of it the file is
        // about to belong to someone else.
        self.fence
            .close("this node is following these shards, not leading them");

        for &shard in &self.shards {
            // Marks followed, then closes every cached connection. Both, in that order.
            self.manager.follow(shard)?;
        }

        self.replica.stop_handle().store(false, Ordering::Relaxed);
        tracing::info!(shards = self.shards.len(), "following");
        Ok(())
    }

    /// Take every shard back from the replication path. The node becomes the leader.
    ///
    /// Blocks until the pull loop has come to rest, because a follower still inside `apply`
    /// is writing pages that any connection opened now would be caching stale copies of.
    pub fn promote(&self, term: Term) -> Result<()> {
        self.replica.stop_handle().store(true, Ordering::Relaxed);

        let deadline = Instant::now() + self.settle;
        while self.replica.is_running() {
            if Instant::now() >= deadline {
                // Refuse rather than promote optimistically. Opening SQLite on a file the
                // replication path may still be rewriting is silent corruption, and a failed
                // promotion is merely an outage.
                return Err(Error::PromotionBlocked {
                    waited: format!("{:?}", self.settle),
                });
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        for &shard in &self.shards {
            self.manager.lead(shard);
        }

        // Last: the gate opens only once every file is genuinely this node's.
        self.fence.open(term);
        tracing::info!(term, shards = self.shards.len(), "promoted to leader");
        Ok(())
    }

    pub fn shards(&self) -> &[ShardId] {
        &self.shards
    }
}

impl std::fmt::Debug for Promotion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Promotion")
            .field("shards", &self.shards)
            .field("settle", &self.settle)
            .finish_non_exhaustive()
    }
}
