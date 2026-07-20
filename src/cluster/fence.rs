//! Fencing: making a deposed leader harmless rather than merely unlikely.
//!
//! # The window this closes
//!
//! A leader that has lost its quorum steps itself down ([`super::election`]) — but only once
//! it *notices*. Between losing contact and its lease expiring there is a window in which a
//! partitioned node still believes it leads. Meanwhile the majority side, which does not need
//! that node's consent, has already elected someone else. For that window there are two nodes
//! that both think they may write.
//!
//! The lease bounds the window. Fencing makes the window harmless: every replication message
//! carries the sender's term, and a follower refuses anything from a term below the highest it
//! has seen. A deposed leader can shout all it likes; nobody is listening any more.
//!
//! # The gate is per shard, because leadership is
//!
//! A node leads *some* shards and follows others — that is what multi-write means. A single
//! node-wide "may I write" flag cannot express it, and a node that led any shard would be
//! permitted to write every shard.
//!
//! The **term** stays node-wide: there is one election, so one logical clock. What varies per
//! shard is which files this node has been assigned.
//!
//! # Why the write gate is separate from the token
//!
//! Two different questions:
//!
//! - The **token** answers "should I accept this message from someone else?" — checked by
//!   followers, on the receiving side.
//! - The **gate** answers "may I write at all?" — checked by this node's own writers, on the
//!   sending side, before a commit.
//!
//! Both are needed. Only the token means a deposed leader still commits locally and diverges
//! its own copy even though nobody accepts the frames. Only the gate means a leader whose
//! step-down is slow keeps shipping frames that followers have no reason to refuse.
//!
//! # Rejections are counted
//!
//! A fenced message is the system working, but a node that is *persistently* fenced is a real
//! fault — a partition that has not healed, or a node that failed to notice a step-down.
//! Silent success would hide both.

use std::collections::BTreeMap;

use crate::sync::Mutex;
use crate::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

use super::term::{NodeId, Term};
use crate::shard::ShardId;

/// Proof of the right to write, attached to every replication message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FenceToken {
    pub term: Term,
    pub node: NodeId,
}

impl FenceToken {
    pub fn new(term: Term, node: NodeId) -> Self {
        Self { term, node }
    }
}

impl std::fmt::Display for FenceToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "term {} from node {}", self.term, self.node)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FenceStats {
    pub highest_seen: Term,
    /// Messages refused for carrying a stale term. Each one is a deposed leader still trying
    /// to write; a number that keeps climbing is a partition that has not healed.
    pub rejected: u64,
    /// Writes refused because this node is not currently permitted to write.
    pub gated_writes: u64,
    /// Attempts to open the gate for a term older than one already seen. Each is the
    /// deposed-leader race caught at the last line of defence: a placement applied
    /// concurrently with a step-down tried to reopen gates the step-down had closed.
    pub stale_opens: u64,
}

/// Tracks the highest term seen and refuses anything older.
#[derive(Debug, Default)]
pub struct Fence {
    /// This node's own id, so a token it issues is complete without the caller patching it.
    node: NodeId,
    highest: AtomicU64,
    rejected: AtomicU64,
    gated: AtomicU64,
    stale_opens: AtomicU64,
    /// Shards this node may write, and the term it was granted each in. Absent means closed.
    led: Mutex<BTreeMap<ShardId, Term>>,
}

impl Fence {
    pub fn new(node: NodeId, term: Term) -> Self {
        Self {
            node,
            highest: AtomicU64::new(term),
            ..Default::default()
        }
    }

    pub fn stats(&self) -> FenceStats {
        FenceStats {
            highest_seen: self.highest.load(Ordering::Acquire),
            rejected: self.rejected.load(Ordering::Relaxed),
            gated_writes: self.gated.load(Ordering::Relaxed),
            stale_opens: self.stale_opens.load(Ordering::Relaxed),
        }
    }

    pub fn highest_seen(&self) -> Term {
        self.highest.load(Ordering::Acquire)
    }

    /// Check an incoming replication message.
    ///
    /// A token from a term at or above the highest seen is accepted, and raises the bar for
    /// everything after it. Anything older is refused — that sender has been deposed and does
    /// not know yet.
    pub fn validate(&self, token: FenceToken) -> Result<()> {
        // Raise the bar and read the previous value in one step. Two followers racing here
        // must not both read the old value and then both accept a stale token.
        let previous = self.highest.fetch_max(token.term, Ordering::AcqRel);
        if token.term < previous {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                token = %token,
                highest_seen = previous,
                "fenced a stale message: the sender has been deposed and does not know yet"
            );
            return Err(Error::Fenced {
                token: token.to_string(),
                highest_seen: previous,
            });
        }
        Ok(())
    }

    /// Permit this node to write `shards` in `term`, and only those.
    ///
    /// Replaces the whole set rather than adding to it, so a placement that takes a shard
    /// away closes its gate without the caller having to work out the difference. Getting
    /// that subtraction wrong at a call site would leave a node writing a shard another node
    /// now owns.
    pub fn open_for(&self, shards: &[ShardId], term: Term) {
        // The gate mutex is taken FIRST, and the staleness check happens inside it. The
        // first version checked before locking, and the stress harness caught the hole once
        // in eighteen suite runs: a step-down can run *between* this thread's term check and
        // its gate update — check passes against the old term, step-down raises the bar and
        // closes, then this thread inserts its gates anyway. Check-then-act, one level below
        // the race it was built to stop. Holding the gate lock across both makes the check
        // and the mutation one step, so a step-down lands wholly before or wholly after.
        let mut led = self.led.lock().expect("fence mutex");

        let previous = self.highest.fetch_max(term, Ordering::AcqRel);
        if term < previous {
            self.stale_opens.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                term,
                highest_seen = previous,
                "refusing to open the write gate for a stale term; a newer term has already \
                 been seen, so this placement is a leftover from a deposed leadership"
            );
            return;
        }

        let before: Vec<ShardId> = led.keys().copied().collect();
        led.clear();
        for &s in shards {
            led.insert(s, term);
        }
        let dropped: Vec<ShardId> = before
            .into_iter()
            .filter(|s| !led.contains_key(s))
            .collect();
        if !dropped.is_empty() {
            tracing::warn!(?dropped, term, "write gate closed for shards no longer led");
        }
        tracing::info!(count = shards.len(), term, "write gate open");
    }

    /// Step down: raise the bar to `term` and close every gate, as one step.
    ///
    /// One step under the gate mutex, not a raise followed by a close — the raise is what
    /// makes a concurrently-applied stale placement refuse itself, and it must not be
    /// separable from the close it protects.
    pub fn step_down(&self, term: Term, why: &str) {
        let mut led = self.led.lock().expect("fence mutex");
        self.highest.fetch_max(term, Ordering::AcqRel);
        let was = std::mem::take(&mut *led);
        if !was.is_empty() {
            tracing::warn!(shards = was.len(), term, why, "write gate closed");
        }
    }

    /// Forbid this node from writing anything. Called the moment leadership is lost.
    ///
    /// Must happen **before** anything else on the step-down path. A step-down that stops the
    /// writer after draining its queue would commit the very writes the new leader has already
    /// started accepting.
    pub fn close(&self, why: &str) {
        let was = std::mem::take(&mut *self.led.lock().expect("fence mutex"));
        if !was.is_empty() {
            tracing::warn!(shards = was.len(), why, "write gate closed");
        }
    }

    /// Whether this node may write `shard`.
    pub fn is_open(&self, shard: ShardId) -> bool {
        self.led.lock().expect("fence mutex").contains_key(&shard)
    }

    /// Whether this node may write anything at all.
    pub fn leads_anything(&self) -> bool {
        !self.led.lock().expect("fence mutex").is_empty()
    }

    /// Shards this node currently leads.
    pub fn led_shards(&self) -> Vec<ShardId> {
        self.led
            .lock()
            .expect("fence mutex")
            .keys()
            .copied()
            .collect()
    }

    /// The term this node may write `shard` in, if any.
    pub fn open_term(&self, shard: ShardId) -> Option<Term> {
        self.led.lock().expect("fence mutex").get(&shard).copied()
    }

    /// Check before committing. The writer's own guard against writing a shard it does not
    /// lead — whether because it was deposed, or because placement gave the shard away.
    pub fn check_may_write(&self, shard: ShardId) -> Result<FenceToken> {
        match self.open_term(shard) {
            Some(term) => Ok(FenceToken {
                term,
                node: self.node,
            }),
            None => {
                self.gated.fetch_add(1, Ordering::Relaxed);
                Err(Error::NotLeader {
                    highest_seen: self.highest_seen(),
                })
            }
        }
    }
}

/// Wiring the gate to the write path.
///
/// The trait lives in `shard` so the dependency points one way: cluster knows about shards,
/// shards do not know about cluster. Without this impl the gate is a mechanism that nothing
/// consults — which is exactly what it was for one commit.
impl crate::shard::WriteGate for Fence {
    fn check_may_write(&self, shard: ShardId) -> Result<()> {
        self.check_may_write(shard).map(|_| ())
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn a_stale_token_is_refused() {
        let f = Fence::new(1, 0);
        assert!(f.validate(FenceToken::new(5, 1)).is_ok());
        let err = f.validate(FenceToken::new(4, 9)).unwrap_err();
        assert!(err.to_string().contains("fenced"), "{err}");
        assert_eq!(f.stats().rejected, 1, "the refusal must be counted");
    }

    #[test]
    fn the_current_term_is_accepted_and_so_is_a_newer_one() {
        let f = Fence::new(1, 0);
        assert!(f.validate(FenceToken::new(3, 1)).is_ok());
        assert!(
            f.validate(FenceToken::new(3, 1)).is_ok(),
            "same term is fine"
        );
        assert!(f.validate(FenceToken::new(7, 2)).is_ok());
        assert_eq!(f.highest_seen(), 7);
        assert_eq!(f.stats().rejected, 0);
    }

    #[test]
    fn accepting_a_newer_term_fences_the_older_leader_from_then_on() {
        // The actual failover sequence: the new leader's first message raises the bar, and
        // the old leader's next message bounces off it.
        let f = Fence::new(1, 0);
        assert!(f.validate(FenceToken::new(4, 1)).is_ok(), "old leader");
        assert!(f.validate(FenceToken::new(5, 2)).is_ok(), "new leader");
        assert!(
            f.validate(FenceToken::new(4, 1)).is_err(),
            "the old leader must now be refused"
        );
    }

    const S0: ShardId = ShardId(0);
    const S1: ShardId = ShardId(1);

    #[test]
    fn a_closed_gate_refuses_writes_and_counts_them() {
        let f = Fence::new(1, 0);
        assert!(!f.is_open(S0));
        let err = f.check_may_write(S0).unwrap_err();
        assert!(err.to_string().contains("not the leader"), "{err}");
        assert_eq!(f.stats().gated_writes, 1);

        f.open_for(&[S0], 3);
        assert!(f.is_open(S0));
        let token = f.check_may_write(S0).unwrap();
        assert_eq!(token.term, 3);
        assert_eq!(
            token.node, 1,
            "the token must identify this node without patching"
        );
    }

    #[test]
    fn a_node_may_write_only_the_shards_it_leads() {
        // The whole point of a per-shard gate. A node-wide flag would let a node that leads
        // any shard write every shard, which under multi-write means two writers on one file.
        let f = Fence::new(1, 0);
        f.open_for(&[S0], 4);
        assert!(f.check_may_write(S0).is_ok());
        let err = f
            .check_may_write(S1)
            .expect_err("a shard this node does not lead must be refused");
        assert!(err.to_string().contains("not the leader"), "{err}");
        assert!(f.leads_anything(), "and it does still lead something");
    }

    #[test]
    fn a_shard_taken_away_by_placement_has_its_gate_closed() {
        // Placement replaces the assignment wholesale. Working out the difference at the call
        // site is exactly the subtraction that gets missed, leaving a node writing a shard
        // another node now owns.
        let f = Fence::new(1, 0);
        f.open_for(&[S0, S1], 4);
        assert!(f.is_open(S0) && f.is_open(S1));

        f.open_for(&[S1], 5);
        assert!(
            !f.is_open(S0),
            "a shard dropped from the assignment must be closed, not merely not-added"
        );
        assert!(f.is_open(S1));
        assert_eq!(f.open_term(S1), Some(5), "and the new term applies");
    }

    #[test]
    fn closing_the_gate_stops_writes_immediately() {
        // The self-fencing half. A deposed leader that keeps committing locally diverges its
        // own copy even if every follower refuses its frames.
        let f = Fence::new(1, 0);
        f.open_for(&[S0, S1], 3);
        assert!(f.check_may_write(S0).is_ok());
        f.close("lost quorum");
        assert!(
            f.check_may_write(S0).is_err(),
            "a deposed leader must not commit even locally"
        );
        assert!(!f.leads_anything());
    }

    #[test]
    fn opening_the_gate_also_raises_the_fence() {
        // A new leader must not accept its own predecessor's frames afterwards.
        let f = Fence::new(1, 0);
        f.open_for(&[S0], 9);
        assert_eq!(f.highest_seen(), 9);
        assert!(f.validate(FenceToken::new(8, 1)).is_err());
    }

    #[test]
    fn the_fence_never_moves_backwards() {
        let f = Fence::new(1, 0);
        f.validate(FenceToken::new(10, 1)).unwrap();
        let _ = f.validate(FenceToken::new(2, 1));
        assert_eq!(
            f.highest_seen(),
            10,
            "a stale token must not lower the bar it just failed to clear"
        );
    }

    #[test]
    fn a_stale_open_is_refused_and_counted() {
        let f = Fence::new(1, 0);
        f.open_for(&[S0], 5);
        // A newer term is observed — a successor's frames, say.
        f.validate(FenceToken::new(7, 2)).unwrap();
        // A leftover placement from term 5 tries to open. It must bounce.
        f.open_for(&[S1], 5);
        assert!(!f.is_open(S1), "a stale open must not take effect");
        assert!(f.is_open(S0), "and must not disturb what was already open");
        assert_eq!(f.stats().stale_opens, 1, "the refusal must be counted");
    }

    #[test]
    fn a_deposed_leader_cannot_be_reopened_by_a_racing_placement() {
        // The interleaving this reproduces is real: `apply_placement` runs on every
        // connection thread as heartbeats arrive, and a step-down can land between its term
        // check and its `open_for`. Whichever order the two threads run in, the gate must be
        // closed once both are done — the step-down carries the higher term, so the late
        // open must refuse itself.
        for _ in 0..500 {
            let f = std::sync::Arc::new(Fence::new(1, 0));
            f.open_for(&[S0], 1);

            let depose = {
                let f = std::sync::Arc::clone(&f);
                std::thread::spawn(move || f.step_down(2, "deposed by term 2"))
            };
            let reopen = {
                let f = std::sync::Arc::clone(&f);
                std::thread::spawn(move || f.open_for(&[S0], 1))
            };
            depose.join().unwrap();
            reopen.join().unwrap();

            assert!(
                !f.is_open(S0),
                "a placement from term 1 reopened the gate after a term-2 step-down; a \
                 deposed leader can write again"
            );
        }
    }

    #[test]
    fn concurrent_validation_does_not_let_a_stale_token_through() {
        // Read-then-store would let two threads both observe the old value and both accept.
        // fetch_max makes raising the bar and learning the previous value one step.
        use std::sync::Arc;
        let f = Arc::new(Fence::new(1, 0));
        f.validate(FenceToken::new(100, 1)).unwrap();

        let mut handles = Vec::new();
        for _ in 0..8 {
            let f = Arc::clone(&f);
            handles.push(std::thread::spawn(move || {
                let mut accepted = 0;
                for _ in 0..1000 {
                    if f.validate(FenceToken::new(50, 2)).is_ok() {
                        accepted += 1;
                    }
                }
                accepted
            }));
        }
        let accepted: u32 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(accepted, 0, "no stale token may be accepted, ever");
        assert_eq!(f.stats().rejected, 8000);
        assert_eq!(f.highest_seen(), 100);
    }
}

/// Exhaustive interleaving checks for the races the concurrency audit found.
///
/// Run with `RUSTFLAGS="--cfg loom" cargo test --release --lib loom_`. Each `loom::model`
/// explores **every** schedule of the threads inside it, so a passing model is a proof over
/// the modelled operations — where the stress harness's hammer test could only say "not seen
/// in 500 tries". The hammer caught the check-outside-the-lock bug once in eighteen suite
/// runs; the model below finds it in every run, deterministically, when the fix is reverted.
#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use loom::sync::Arc;
    use loom::thread;

    const S0: ShardId = ShardId(0);
    const S1: ShardId = ShardId(1);

    /// Race 1/1b: a placement carrying the old term races the step-down deposing it.
    ///
    /// This is the exact interleaving from production: `apply_placement` on a connection
    /// thread, a step-down on another. Whatever the schedule, once both complete the gate
    /// must be closed — the stale open must never win.
    #[test]
    fn loom_a_deposed_leader_cannot_be_reopened() {
        loom::model(|| {
            let f = Arc::new(Fence::new(1, 0));
            f.open_for(&[S0], 1);

            let depose = {
                let f = Arc::clone(&f);
                thread::spawn(move || f.step_down(2, "deposed"))
            };
            let reopen = {
                let f = Arc::clone(&f);
                thread::spawn(move || f.open_for(&[S0], 1))
            };
            depose.join().unwrap();
            reopen.join().unwrap();

            assert!(
                !f.is_open(S0),
                "an interleaving exists where a stale placement reopens a deposed leader's gate"
            );
        });
    }

    /// Race 2's fence-level face: two placements racing must converge on the newer one,
    /// never interleave into a mixture or end on the older.
    #[test]
    fn loom_racing_placements_converge_on_the_newer_term() {
        loom::model(|| {
            let f = Arc::new(Fence::new(1, 0));

            let older = {
                let f = Arc::clone(&f);
                thread::spawn(move || f.open_for(&[S0], 1))
            };
            let newer = {
                let f = Arc::clone(&f);
                thread::spawn(move || f.open_for(&[S1], 2))
            };
            older.join().unwrap();
            newer.join().unwrap();

            // Either order: term 1 first then term 2 replaces it, or term 2 first and the
            // term-1 open refuses itself as stale. The end state is the same.
            assert!(!f.is_open(S0), "the older placement's gates survived");
            assert!(
                f.is_open(S1),
                "the newer placement's gates must be in force"
            );
            assert_eq!(f.open_term(S1), Some(2));
        });
    }

    /// A write racing a step-down may be granted a token or refused — both are safe — but a
    /// granted token must carry the *deposed* term, which every remote fence that has seen
    /// the successor will reject. It must never carry the deposing term.
    #[test]
    fn loom_a_token_issued_across_a_step_down_is_always_stale() {
        loom::model(|| {
            let f = Arc::new(Fence::new(1, 0));
            f.open_for(&[S0], 1);

            let depose = {
                let f = Arc::clone(&f);
                thread::spawn(move || f.step_down(2, "deposed"))
            };
            let token = f.check_may_write(S0);
            depose.join().unwrap();

            if let Ok(t) = token {
                assert_eq!(
                    t.term, 1,
                    "a token minted across a step-down claimed the deposing term"
                );
            }
            assert!(!f.is_open(S0));
        });
    }
}
