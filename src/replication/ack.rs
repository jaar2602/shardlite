//! Quorum acknowledgement: making a client's "ok" mean a majority holds the write.
//!
//! # The property this exists for
//!
//! Without it, a leader commits locally, tells the client the write succeeded, and ships the
//! frames to followers whenever they next ask. If it dies in that window the write is gone —
//! and no amount of care during the election can recover it, because the new leader
//! legitimately never received it. The election restriction picks the most advanced
//! *survivor*, which is not the same as picking a node that holds everything acknowledged.
//!
//! So the acknowledgement has to wait for the replication, not the other way round. That is
//! the whole of it: a client that is told "ok" can rely on a majority holding the write, and
//! therefore on any future leader holding it too.
//!
//! # Why waiting here is affordable
//!
//! One batch is one wait. Group commit already merges everything queued behind a running
//! transaction into the next one, so under load the batch grows and the single round trip is
//! amortised across every write in it. The per-write cost of a quorum falls as load rises,
//! which is the opposite of the per-transaction intuition.
//!
//! # A follower's next request is its acknowledgement
//!
//! Followers pull, asking for `from_lsn`. That request is itself proof they hold everything
//! below it — there is no separate ack message to lose, get out of order, or forget to send.
//! The position a follower asks from and the position it has durably applied are the same
//! number by construction.
//!
//! # Timing out is not the same as failing
//!
//! If a quorum cannot be reached the data **is still committed locally**. Reporting that as a
//! plain failure would be a lie in the other direction — a client that retries could double
//! apply. So the timeout is reported as exactly what it is: committed here, not confirmed
//! elsewhere, durability unknown.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::shard::ShardId;

use super::stream::Lsn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckStats {
    /// Waits satisfied by a quorum.
    pub confirmed: u64,
    /// Waits that ran out of time. Each one is a write committed locally whose replication
    /// could not be confirmed — not a failure, but not a success either, and the number
    /// climbing means followers cannot keep up.
    pub timed_out: u64,
    /// Total time spent waiting for quorums, in microseconds. Divided by `confirmed` this is
    /// the real replication cost per batch.
    pub waited_us: u64,
}

/// Tracks how far each follower has durably applied, per shard, and lets a writer wait for a
/// majority to catch up.
#[derive(Debug)]
pub struct AckTracker {
    /// Total voting members, this node included. A majority of this is the quorum.
    voters: usize,
    /// `(shard, node)` to the highest LSN that node is known to hold.
    positions: Mutex<BTreeMap<(ShardId, u64), Lsn>>,
    progress: Condvar,
    how_long: Duration,
    confirmed: AtomicU64,
    timed_out: AtomicU64,
    waited_us: AtomicU64,
}

impl AckTracker {
    /// `voters` counts every member including this node; `how_long` bounds a single wait.
    ///
    /// `how_long` should sit near the election timeout. A leader that cannot reach a quorum
    /// for longer than that is about to be deposed anyway, so waiting past it achieves
    /// nothing but holding the client.
    pub fn new(voters: usize, how_long: Duration) -> Self {
        Self {
            voters: voters.max(1),
            positions: Mutex::new(BTreeMap::new()),
            progress: Condvar::new(),
            how_long,
            confirmed: AtomicU64::new(0),
            timed_out: AtomicU64::new(0),
            waited_us: AtomicU64::new(0),
        }
    }

    pub fn stats(&self) -> AckStats {
        AckStats {
            confirmed: self.confirmed.load(Ordering::Relaxed),
            timed_out: self.timed_out.load(Ordering::Relaxed),
            waited_us: self.waited_us.load(Ordering::Relaxed),
        }
    }

    /// Members that must hold a write before it is acknowledged, this node included.
    fn quorum(&self) -> usize {
        self.voters / 2 + 1
    }

    /// Record that `node` holds everything up to and including `lsn`.
    ///
    /// Monotonic per node: a follower that restarts and asks from an earlier position has not
    /// *lost* what it already applied, and letting its report go backwards would un-confirm
    /// writes that were legitimately acknowledged.
    pub fn record(&self, node: u64, shard: ShardId, lsn: Lsn) {
        let mut g = self.positions.lock().expect("ack mutex");
        let entry = g.entry((shard, node)).or_insert(0);
        if lsn > *entry {
            *entry = lsn;
            drop(g);
            self.progress.notify_all();
        }
    }

    /// How many members hold `lsn` for `shard`, counting this node, which by definition does.
    fn holders(
        &self,
        positions: &BTreeMap<(ShardId, u64), Lsn>,
        shard: ShardId,
        lsn: Lsn,
    ) -> usize {
        let followers = positions
            .iter()
            .filter(|((s, _), held)| *s == shard && **held >= lsn)
            .count();
        followers + 1
    }

    /// Block until a majority holds `lsn` for `shard`.
    ///
    /// Returns [`Error::NotReplicated`] on timeout — which says the write is committed here
    /// but unconfirmed elsewhere, rather than claiming it failed.
    pub fn wait_for_quorum(&self, shard: ShardId, lsn: Lsn) -> Result<()> {
        let needed = self.quorum();
        let started = Instant::now();

        let mut g = self.positions.lock().expect("ack mutex");
        loop {
            let have = self.holders(&g, shard, lsn);
            if have >= needed {
                self.confirmed.fetch_add(1, Ordering::Relaxed);
                self.waited_us
                    .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
                return Ok(());
            }

            let left = match self.how_long.checked_sub(started.elapsed()) {
                Some(d) if !d.is_zero() => d,
                _ => {
                    self.timed_out.fetch_add(1, Ordering::Relaxed);
                    self.waited_us
                        .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
                    tracing::warn!(
                        %shard,
                        lsn,
                        have,
                        needed,
                        "no quorum for a committed write; it is durable on this node but not \
                         confirmed elsewhere"
                    );
                    return Err(Error::NotReplicated {
                        shard: shard.to_string(),
                        lsn,
                        holders: have,
                        needed,
                    });
                }
            };

            let (guard, _) = self.progress.wait_timeout(g, left).expect("ack condvar");
            g = guard;
        }
    }

    /// Highest LSN a majority is known to hold for `shard`, without waiting.
    pub fn quorum_lsn(&self, shard: ShardId) -> Lsn {
        let g = self.positions.lock().expect("ack mutex");
        let mut held: Vec<Lsn> = g
            .iter()
            .filter(|((s, _), _)| *s == shard)
            .map(|(_, &lsn)| lsn)
            .collect();
        // This node holds everything, so it never binds; sort the followers and take the one
        // that, with this node, makes a majority.
        held.sort_unstable_by(|a, b| b.cmp(a));
        let followers_needed = self.quorum().saturating_sub(1);
        if followers_needed == 0 {
            return Lsn::MAX;
        }
        held.get(followers_needed - 1).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    const S: ShardId = ShardId(0);

    fn tracker(voters: usize) -> Arc<AckTracker> {
        Arc::new(AckTracker::new(voters, Duration::from_millis(300)))
    }

    #[test]
    fn a_single_node_cluster_is_its_own_quorum() {
        // Otherwise a standalone deployment would block on every write forever.
        let t = tracker(1);
        t.wait_for_quorum(S, 100).unwrap();
        assert_eq!(t.stats().confirmed, 1);
    }

    #[test]
    fn one_follower_of_three_is_enough() {
        let t = tracker(3);
        t.record(2, S, 50);
        t.wait_for_quorum(S, 50).unwrap();
    }

    #[test]
    fn a_write_no_follower_holds_is_not_confirmed() {
        // The exact case that loses data without this file: committed locally, acknowledged,
        // and held nowhere else.
        let t = tracker(3);
        let err = t.wait_for_quorum(S, 7).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("committed"), "{msg}");
        assert!(
            msg.contains("not confirmed") || msg.contains("quorum"),
            "the message must distinguish unconfirmed from failed: {msg}"
        );
        assert_eq!(t.stats().timed_out, 1);
    }

    #[test]
    fn a_writer_blocks_until_a_follower_catches_up() {
        // The property, exercised as it actually happens: the write waits, the follower
        // reports, the write proceeds.
        let t = tracker(3);
        let t2 = Arc::clone(&t);
        let waiter = std::thread::spawn(move || t2.wait_for_quorum(S, 42));

        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(t.stats().confirmed, 0, "it must still be waiting");

        t.record(2, S, 42);
        waiter.join().unwrap().expect("the wait should succeed");
        assert_eq!(t.stats().confirmed, 1);
    }

    #[test]
    fn a_follower_behind_the_write_does_not_count() {
        let t = tracker(3);
        t.record(2, S, 41);
        let err = t.wait_for_quorum(S, 42).unwrap_err();
        assert!(err.to_string().contains("42"), "{err}");
    }

    #[test]
    fn shards_are_counted_independently() {
        // A follower caught up on a busy shard must not confirm a write on a quiet one.
        let t = tracker(3);
        t.record(2, ShardId(1), 999);
        assert!(t.wait_for_quorum(ShardId(0), 1).is_err());
    }

    #[test]
    fn a_five_node_cluster_needs_two_followers() {
        let t = tracker(5);
        t.record(2, S, 10);
        assert!(
            t.wait_for_quorum(S, 10).is_err(),
            "one follower of five is not a majority"
        );
        t.record(3, S, 10);
        t.wait_for_quorum(S, 10).unwrap();
    }

    #[test]
    fn a_report_never_moves_backwards() {
        // A follower that restarts and re-asks from an earlier position has not lost what it
        // applied. Letting the report regress would un-confirm acknowledged writes.
        let t = tracker(3);
        t.record(2, S, 100);
        t.record(2, S, 5);
        t.wait_for_quorum(S, 100).unwrap();
    }

    #[test]
    fn quorum_lsn_reports_what_a_majority_holds() {
        let t = tracker(3);
        assert_eq!(t.quorum_lsn(S), 0);
        t.record(2, S, 10);
        assert_eq!(t.quorum_lsn(S), 10);
        t.record(3, S, 20);
        assert_eq!(
            t.quorum_lsn(S),
            20,
            "with two followers at 10 and 20, a majority of three holds 20"
        );
    }
}
