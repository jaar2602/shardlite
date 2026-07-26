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
        // Connect the replication path to the reader coordination. Without this, applies
        // neither exclude readers nor invalidate their cached connections, and a reader on a
        // followed shard is served from a page cache frozen at whenever it first connected —
        // silently, and forever.
        replica
            .follower()
            .coordinate_with(Arc::clone(manager.modes()));
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

    /// Move to leading exactly `lead` and following everything else.
    ///
    /// The ordering is the correctness argument, and it is not the obvious one:
    ///
    /// 1. **Close gates for shards being taken away, first.** Everything after this takes
    ///    time, and for all of it those files are about to belong to another node.
    /// 2. Hand those files to the replication path — mark followed, then quiesce. It is the
    ///    surviving connection, not the mark, that corrupts.
    /// 3. **Bring the pull loop to rest** before touching anything being gained. A loop
    ///    inside `apply` is writing raw pages; opening SQLite on that file is exactly the
    ///    silent corruption the ownership invariant exists to prevent.
    /// 4. Take ownership of gained shards and stop following them.
    /// 5. **Only now** open their gates. A gate opened before step 4 would let this node
    ///    write a file the replication path still owns.
    ///
    /// Doing 5 before 3 is the mistake that looks harmless and is not.
    pub fn apply(&self, lead: &[ShardId], term: Term) -> Result<()> {
        let want: std::collections::BTreeSet<ShardId> = lead.iter().copied().collect();
        let held: std::collections::BTreeSet<ShardId> =
            self.fence.led_shards().into_iter().collect();

        let losing: Vec<ShardId> = held.difference(&want).copied().collect();
        let gaining: Vec<ShardId> = want.difference(&held).copied().collect();
        if losing.is_empty() && gaining.is_empty() {
            return Ok(());
        }

        // 1 and 2: stop writing what is leaving, then hand the files over.
        if !losing.is_empty() {
            let keep: Vec<ShardId> = held.intersection(&want).copied().collect();
            self.fence.open_for(&keep, term);
            for &shard in &losing {
                self.manager.follow(shard)?;
                self.replica.follow(shard);
            }
        }

        if gaining.is_empty() {
            return Ok(());
        }

        // 3: the loop must be holding no file before anything is taken over.
        self.replica.pause();
        let settled = self.replica.wait_idle(self.settle);
        if !settled {
            self.replica.resume();
            return Err(Error::PromotionBlocked {
                waited: format!("{:?}", self.settle),
            });
        }

        // 4: ownership moves while the loop is standing still.
        for &shard in &gaining {
            self.replica.unfollow(shard);
            self.manager.lead(shard);
        }
        self.replica.resume();

        // 5: and only now may this node write them.
        self.fence.open_for(lead, term);
        tracing::info!(
            term,
            gained = gaining.len(),
            lost = losing.len(),
            "placement applied"
        );
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
        self.fence.open_for(&self.shards, term);
        tracing::info!(term, shards = self.shards.len(), "promoted to leader");
        Ok(())
    }

    pub fn shards(&self) -> &[ShardId] {
        &self.shards
    }

    /// Fence one source shard, drain its writer queue, and return its exact final `(epoch, LSN)`.
    ///
    /// The gate closes before the queue barrier. Anything already inside SQLite finishes; queued
    /// work that has not started observes the closed gate. Once `drain_writes` returns no later
    /// commit can advance this position.
    pub fn fence_for_transfer(&self, shard: ShardId) -> Result<(u64, u64)> {
        if !self.shards.contains(&shard) || !self.fence.is_open(shard) {
            return Err(Error::ClusterConfig(format!(
                "cannot fence {shard} for transfer: this node is not its writable source"
            )));
        }
        self.fence
            .close_shard(shard, "catalog transfer reached its fencing barrier");
        self.manager.drain_writes(shard)?;
        let epoch = self.manager.epoch().ok_or_else(|| {
            Error::ClusterConfig(format!(
                "cannot transfer {shard} without frame capture and a stream epoch"
            ))
        })?;
        Ok((epoch, self.manager.last_lsn(shard)))
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
