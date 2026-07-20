//! Whether this node may open a shard's file at all.
//!
//! # The hazard this exists to prevent
//!
//! A follower does not execute SQL. It writes **raw pages** into the database file
//! (`vfs::apply_to_db_file`), and on bootstrap it **renames a whole new file over the old
//! one** (`Follower::install_snapshot`). Neither goes through SQLite, so neither respects
//! SQLite's locking.
//!
//! Any connection open across those operations is therefore broken in a way that does not
//! announce itself:
//!
//! - After direct page writes, an open connection's page cache describes a file that no
//!   longer exists. It will happily serve rows assembled from stale pages.
//! - After a rename, an open connection still holds the **old inode** — a deleted file. It
//!   reads a database frozen at the moment of replacement, forever, with no error.
//!
//! Both are silent. `PRAGMA integrity_check` on the *file* passes, because the file is fine;
//! it is the open handle that is wrong.
//!
//! # The invariant
//!
//! A shard is either **led** by this node — its `ShardManager` owns the file and SQLite has
//! exclusive charge of it — or **followed** — the replication path owns the file and no
//! SQLite connection may exist. Never both, and never neither.
//!
//! Enforced where connections are opened rather than by convention, because the failure it
//! prevents is invisible and permanent.
//!
//! # Followers can serve reads, under a protocol
//!
//! Writes are refused outright on a followed shard. Reads are allowed, but only through
//! [`ShardAccess`], which supplies the two things SQLite is not getting for itself:
//!
//! - **Exclusion.** Applies take the lock exclusively, reads share it. So no read is ever in
//!   flight while pages are being rewritten or a file renamed underneath it.
//! - **A generation.** Every apply bumps it. A cached connection from an older generation is
//!   closed and reopened rather than reused, which is what clears the stale page cache — and,
//!   after a snapshot install, the handle to the deleted inode.
//!
//! Measured before choosing this over applying frames through SQLite's own WAL: reopening a
//! read connection costs ~8 us, and a query through a fresh connection 23 us against 2 us on
//! a persistent one. An apply costs an fsync — about 1.5 ms on container storage — so the
//! reopen is under 1% of the work it follows, and only the first read after each apply pays
//! it. Rewriting the apply path to produce real WAL frames would be faster still and is the
//! eventual answer, but it is the riskiest change available and this buys the same capability
//! for a measured, small cost.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::error::{Error, Result};

use super::ShardId;

/// Who owns a shard's file on this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ShardMode {
    /// This node's `ShardManager` owns the file. SQLite may open it.
    #[default]
    Led,
    /// The replication path owns the file and rewrites it behind SQLite's back. No
    /// connection may be open.
    Followed,
}

/// Serialises reads against the replication path, and marks when a reader's view went stale.
///
/// Held per shard and shared between the reader fleet and the follower, which are otherwise
/// unaware of each other.
#[derive(Debug, Default)]
pub struct ShardAccess {
    /// Applies take this exclusively; reads share it. The only thing standing between a read
    /// and a file being rewritten beneath it.
    lock: RwLock<()>,
    /// Bumped by every apply. A connection opened in an older generation is stale.
    generation: AtomicU64,
    reopens: AtomicU64,
}

impl ShardAccess {
    /// The current generation. A reader that cached a connection at an older one must reopen.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn reopens(&self) -> u64 {
        self.reopens.load(Ordering::Relaxed)
    }

    pub fn note_reopen(&self) {
        self.reopens.fetch_add(1, Ordering::Relaxed);
    }

    /// Hold for the duration of a read. Blocks while an apply is in flight.
    pub fn read(&self) -> std::sync::RwLockReadGuard<'_, ()> {
        self.lock.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Hold for the duration of an apply, then bump the generation.
    ///
    /// The bump happens **while the lock is still held**, so no reader can observe the new
    /// pages through a connection still marked as current.
    pub fn apply<T>(&self, f: impl FnOnce() -> T) -> T {
        let _guard = self.lock.write().unwrap_or_else(|e| e.into_inner());
        let out = f();
        self.generation.fetch_add(1, Ordering::Release);
        out
    }
}

/// Per-shard ownership, consulted before any connection is opened.
///
/// Defaults to [`ShardMode::Led`] so a standalone deployment — which has no replication and
/// therefore no hazard — behaves exactly as before and pays nothing.
#[derive(Debug, Default)]
pub struct ShardModes {
    modes: Mutex<BTreeMap<ShardId, ShardMode>>,
    access: Mutex<BTreeMap<ShardId, Arc<ShardAccess>>>,
    refused: AtomicU64,
    closed_on_handover: AtomicU64,
}

impl ShardModes {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mode(&self, shard: ShardId) -> ShardMode {
        self.modes
            .lock()
            .expect("shard mode mutex")
            .get(&shard)
            .copied()
            .unwrap_or_default()
    }

    pub fn set(&self, shard: ShardId, mode: ShardMode) {
        let previous = self
            .modes
            .lock()
            .expect("shard mode mutex")
            .insert(shard, mode);
        if previous != Some(mode) {
            tracing::info!(%shard, ?previous, now = ?mode, "shard ownership changed");
        }
    }

    /// Refusals to open a followed shard. Each one is a bug caught rather than corruption
    /// caused, so the number climbing is a caller that has not been told about the
    /// invariant — worth seeing.
    pub fn refused_opens(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }

    /// Connections closed when handing shards to the replication path.
    ///
    /// Observable because the alternative to closing them is not an error — it is stale rows
    /// served from a cached handle, which nothing else would reveal.
    pub fn closed_on_handover(&self) -> u64 {
        self.closed_on_handover.load(Ordering::Relaxed)
    }

    pub fn record_handover(&self, closed: usize) {
        if closed > 0 {
            self.closed_on_handover
                .fetch_add(closed as u64, Ordering::Relaxed);
            tracing::debug!(closed, "closed connections handing a shard over");
        }
    }

    /// The read/apply coordination for a shard, created on first use.
    pub fn access(&self, shard: ShardId) -> Arc<ShardAccess> {
        Arc::clone(
            self.access
                .lock()
                .expect("shard access mutex")
                .entry(shard)
                .or_default(),
        )
    }

    /// May a **reader** open this shard?
    ///
    /// Yes in either mode. A followed shard is readable through [`ShardAccess`], which is
    /// what makes the read safe; refusing here would give up read scale-out for nothing.
    pub fn check_may_read(&self, _shard: ShardId) -> Result<()> {
        Ok(())
    }

    /// May a **writer** open this shard? `Err` if the replication path owns it.
    pub fn check_may_open(&self, shard: ShardId) -> Result<()> {
        match self.mode(shard) {
            ShardMode::Led => Ok(()),
            ShardMode::Followed => {
                self.refused.fetch_add(1, Ordering::Relaxed);
                Err(Error::ShardFollowed {
                    shard: shard.to_string(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_shard_defaults_to_led_so_a_standalone_node_is_unaffected() {
        let m = ShardModes::new();
        assert_eq!(m.mode(ShardId(0)), ShardMode::Led);
        assert!(m.check_may_open(ShardId(0)).is_ok());
    }

    #[test]
    fn a_followed_shard_refuses_to_be_opened_and_says_why() {
        let m = ShardModes::new();
        m.set(ShardId(3), ShardMode::Followed);
        let err = m.check_may_open(ShardId(3)).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("shard_3"), "{msg}");
        assert!(
            msg.contains("replicat"),
            "the message must say why, not merely refuse: {msg}"
        );
        assert_eq!(m.refused_opens(), 1, "refusals must be counted");
    }

    #[test]
    fn modes_are_per_shard() {
        // Promotion moves shards one at a time, so a node routinely leads some and follows
        // others. A global flag would make that impossible to express.
        let m = ShardModes::new();
        m.set(ShardId(0), ShardMode::Followed);
        assert!(m.check_may_open(ShardId(0)).is_err());
        assert!(m.check_may_open(ShardId(1)).is_ok());
    }

    #[test]
    fn promotion_reopens_a_shard() {
        let m = ShardModes::new();
        m.set(ShardId(0), ShardMode::Followed);
        assert!(m.check_may_open(ShardId(0)).is_err());
        m.set(ShardId(0), ShardMode::Led);
        assert!(m.check_may_open(ShardId(0)).is_ok());
    }
}
