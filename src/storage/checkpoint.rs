//! WAL checkpointing, run by the writer thread between transactions.
//!
//! `wal_autocheckpoint` is disabled on the writer connection because autocheckpoint fires
//! *inside* `COMMIT`, injecting an unpredictable multi-hundred-millisecond stall into the
//! write path. Checkpointing is instead explicit and happens between batches, where it
//! delays nothing that is mid-transaction.
//!
//! # The starvation problem
//!
//! A checkpoint cannot copy WAL frames past the end mark of any active reader. If readers
//! overlap continuously so that one is always active, no checkpoint ever completes and
//! **the WAL grows without bound** — which on a small disk ends as `SQLITE_FULL`, i.e. a
//! machine fault that aborts the batch in flight.
//!
//! Two defences: reader transactions are capped by a deadline (see [`super::reader`]), and
//! the ladder below escalates to a blocking `TRUNCATE` when passive attempts keep failing.
//!
//! # Why file size is only a gate
//!
//! A completed `PASSIVE` checkpoint **resets the WAL for reuse but does not shrink the
//! file** — it stays at its high-water mark. So the file size is an upper bound on live
//! WAL content, useful as a cheap "definitely nothing to do" gate, but the authoritative
//! measurement is the page count the checkpoint pragma returns. Only `TRUNCATE` actually
//! shrinks the file.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::Connection;

use crate::config::CheckpointConfig;

/// What a single checkpoint attempt did. Returned for observability and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointOutcome {
    /// WAL is below the soft limit; nothing worth doing.
    Skipped,
    Passive {
        /// Total frames in the WAL.
        log_pages: i64,
        /// Frames copied back into the database file.
        checkpointed: i64,
        /// SQLite could not get the lock at all.
        busy: bool,
    },
    /// Escalated: blocks readers briefly, but reclaims the file.
    Truncated { before_bytes: u64, after_bytes: u64 },
    /// The attempt failed. Not fatal on its own — the next one may succeed.
    Failed(String),
}

#[derive(Debug, Default)]
pub struct Counters {
    pub passive: AtomicU64,
    pub truncated: AtomicU64,
    pub stalls: AtomicU64,
    pub failures: AtomicU64,
    pub wal_bytes: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointStats {
    pub passive: u64,
    pub truncated: u64,
    pub stalls: u64,
    pub failures: u64,
    /// WAL file size at the last check.
    pub wal_bytes: u64,
}

pub struct Checkpointer {
    cfg: CheckpointConfig,
    wal_path: PathBuf,
    counters: Arc<Counters>,
    /// Consecutive passive attempts that failed to fully drain the WAL.
    consecutive_stalls: u32,
    batches_since_check: u64,
    /// Multiplier on `check_every_batches`, doubled while stalling.
    stall_backoff: u64,
}

impl Checkpointer {
    pub fn new(db_path: &Path, cfg: CheckpointConfig, counters: Arc<Counters>) -> Self {
        Self {
            cfg,
            wal_path: wal_path_for(db_path),
            counters,
            consecutive_stalls: 0,
            batches_since_check: 0,
            stall_backoff: 1,
        }
    }

    /// Call after each committed batch, from the writer thread.
    ///
    /// Cheap in the common case: most calls return without touching SQLite at all.
    pub fn after_batch(&mut self, conn: &Connection) -> CheckpointOutcome {
        self.batches_since_check += 1;
        if self.batches_since_check < self.cfg.check_every_batches * self.stall_backoff {
            return CheckpointOutcome::Skipped;
        }
        self.batches_since_check = 0;

        let wal_bytes = std::fs::metadata(&self.wal_path)
            .map(|m| m.len())
            .unwrap_or(0);
        self.counters.wal_bytes.store(wal_bytes, Ordering::Relaxed);

        if wal_bytes < self.cfg.soft_limit_bytes {
            return CheckpointOutcome::Skipped;
        }

        let outcome = self.passive(conn);

        let stalled = match &outcome {
            CheckpointOutcome::Passive {
                log_pages,
                checkpointed,
                busy,
            } => *busy || checkpointed < log_pages,
            _ => false,
        };

        if stalled {
            self.consecutive_stalls += 1;
            self.counters.stalls.fetch_add(1, Ordering::Relaxed);
            // Discovering that a checkpoint cannot advance still costs a scan proportional
            // to the WAL, and the WAL is growing precisely because we are stalled. Without
            // backoff the wasted work grows quadratically with how long a reader holds its
            // snapshot.
            self.stall_backoff = (self.stall_backoff * 2).min(self.cfg.max_stall_backoff);
        } else {
            self.consecutive_stalls = 0;
            self.stall_backoff = 1;
        }

        // Escalate only when passive attempts keep failing *and* the file has actually
        // grown past the hard limit. A short read stall is the correct trade against
        // unbounded disk growth ending in SQLITE_FULL.
        if self.consecutive_stalls >= self.cfg.stall_threshold
            && wal_bytes >= self.cfg.hard_limit_bytes
        {
            let escalated = self.truncate(conn, wal_bytes);
            // Reset regardless: on success the WAL is drained, and on failure we should
            // make another ladder of passive attempts before blocking readers again.
            self.consecutive_stalls = 0;
            if matches!(escalated, CheckpointOutcome::Truncated { .. }) {
                self.stall_backoff = 1;
            }
            return escalated;
        }

        outcome
    }

    fn passive(&self, conn: &Connection) -> CheckpointOutcome {
        match checkpoint(conn, "PASSIVE") {
            Ok((busy, log_pages, checkpointed)) => {
                self.counters.passive.fetch_add(1, Ordering::Relaxed);
                if busy != 0 || checkpointed < log_pages {
                    // The starvation signal. Worth seeing before it becomes an escalation.
                    tracing::debug!(
                        busy = busy != 0,
                        log_pages,
                        checkpointed,
                        "passive checkpoint could not fully drain the WAL"
                    );
                }
                CheckpointOutcome::Passive {
                    log_pages,
                    checkpointed,
                    busy: busy != 0,
                }
            }
            Err(e) => {
                self.counters.failures.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(error = %e, "passive checkpoint failed");
                CheckpointOutcome::Failed(e.to_string())
            }
        }
    }

    fn truncate(&self, conn: &Connection, before_bytes: u64) -> CheckpointOutcome {
        // Loud on purpose: reaching this means readers have been starving the checkpointer
        // long enough for the WAL to pass its hard limit. Silent escalation is how this
        // gets discovered in production instead of in testing.
        tracing::warn!(
            wal_bytes = before_bytes,
            consecutive_stalls = self.consecutive_stalls,
            "WAL passed its hard limit with checkpoints stalling; escalating to TRUNCATE, \
             which briefly blocks readers"
        );

        match checkpoint(conn, "TRUNCATE") {
            Ok(_) => {
                let after_bytes = std::fs::metadata(&self.wal_path)
                    .map(|m| m.len())
                    .unwrap_or(0);
                self.counters.truncated.fetch_add(1, Ordering::Relaxed);
                self.counters
                    .wal_bytes
                    .store(after_bytes, Ordering::Relaxed);
                CheckpointOutcome::Truncated {
                    before_bytes,
                    after_bytes,
                }
            }
            Err(e) => {
                self.counters.failures.fetch_add(1, Ordering::Relaxed);
                CheckpointOutcome::Failed(e.to_string())
            }
        }
    }
}

/// Returns `(busy, log_pages, checkpointed_pages)`.
///
/// `busy` is 1 when SQLite could not acquire the lock; the page counts are then -1.
fn checkpoint(conn: &Connection, mode: &str) -> rusqlite::Result<(i64, i64, i64)> {
    // The mode is an internal constant, never user input; PRAGMA cannot bind parameters.
    conn.query_row(&format!("PRAGMA wal_checkpoint({mode})"), [], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
    })
}

/// SQLite names the sidecar by appending to the full filename, so `x.db` becomes
/// `x.db-wal` — not by replacing the extension.
pub fn wal_path_for(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_os_string();
    s.push("-wal");
    PathBuf::from(s)
}
