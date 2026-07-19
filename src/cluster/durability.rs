//! How up to date a node is, and the rule that decides who may lead.
//!
//! # This is the file that replaces Raft's log-matching property
//!
//! In Raft, a candidate wins only if its log is at least as up to date as the voter's, and
//! that restriction is what guarantees an elected leader already holds every committed
//! entry. Keeping frames out of the Raft log — the decision that makes this design fit in
//! 0.5 GB — means that guarantee has to be re-established here, over frame positions.
//!
//! Get it wrong and the symptom is not a crash. It is a cluster that elects a leader missing
//! writes it already acknowledged to a client, and then continues cheerfully. Every other
//! safety property in this project is downstream of this comparison being right.
//!
//! # Why comparing across epochs is refused, not approximated
//!
//! An epoch is a stream identity. LSNs are dense *within* an epoch and mean nothing across
//! one — after an unclean restart the epoch bumps precisely because the position reached is
//! unknown. So LSN 900 in epoch 4 is not "ahead of" LSN 5 in epoch 5; the two are not on the
//! same ruler at all.
//!
//! The tempting approximation is to order by `(epoch, lsn)` and move on. That silently
//! elects a node whose epoch happens to be higher but which holds *less* data, losing
//! acknowledged writes. So mismatched epochs are [`Comparison::Incomparable`] and a vote is
//! refused. This can stall an election that a human could see was safe — which is the
//! correct trade. A stalled election is visible and recoverable; a lost write is neither.
//!
//! # Domination is per shard, not in aggregate
//!
//! A candidate must be at least as advanced on **every** shard the voter knows about. Summing
//! or averaging LSNs across shards would let a node far ahead on a busy shard win while being
//! behind on a quiet one, losing exactly the writes on the shard nobody was watching.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::replication::Lsn;
use crate::shard::ShardId;

/// How far a node has durably progressed through each shard's stream.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Durability {
    /// The stream this node's positions belong to. Positions from different epochs cannot be
    /// compared.
    pub epoch: u64,
    /// Highest durable LSN per shard. A shard absent from the map is one this node holds
    /// nothing for, which is treated as position 0 rather than as "no opinion".
    pub shards: BTreeMap<ShardId, Lsn>,
}

/// The result of comparing two nodes' durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparison {
    /// At least as advanced on every shard — safe to lead.
    AtLeast,
    /// Behind on at least one shard. Leading would lose whatever that shard acknowledged.
    Behind {
        shard: ShardId,
        theirs: Lsn,
        ours: Lsn,
    },
    /// Positions belong to different streams and cannot be ordered.
    Incomparable { theirs: u64, ours: u64 },
}

impl Durability {
    pub fn new(epoch: u64) -> Self {
        Self {
            epoch,
            shards: BTreeMap::new(),
        }
    }

    pub fn with(mut self, shard: ShardId, lsn: Lsn) -> Self {
        self.shards.insert(shard, lsn);
        self
    }

    pub fn lsn(&self, shard: ShardId) -> Lsn {
        self.shards.get(&shard).copied().unwrap_or(0)
    }

    /// Whether `self` — a candidate — is at least as up to date as `voter`.
    ///
    /// Deliberately asymmetric in what it iterates: every shard the **voter** knows about
    /// must be covered. A candidate that has simply never heard of a shard has position 0 on
    /// it and loses, which is correct — it cannot lead a shard whose history it does not
    /// hold. Iterating the candidate's shards instead would let it win by knowing less.
    pub fn compare_to(&self, voter: &Durability) -> Comparison {
        if self.epoch != voter.epoch {
            return Comparison::Incomparable {
                theirs: self.epoch,
                ours: voter.epoch,
            };
        }
        for (&shard, &ours) in &voter.shards {
            let theirs = self.lsn(shard);
            if theirs < ours {
                return Comparison::Behind {
                    shard,
                    theirs,
                    ours,
                };
            }
        }
        Comparison::AtLeast
    }

    /// Whether this node may be granted a vote by `voter`.
    pub fn may_lead(&self, voter: &Durability) -> bool {
        matches!(self.compare_to(voter), Comparison::AtLeast)
    }
}

impl Comparison {
    /// A sentence an operator can act on. A refused vote that says only "refused" turns a
    /// stalled election into an unexplained one.
    pub fn why(&self) -> String {
        match self {
            Comparison::AtLeast => "at least as up to date on every shard".into(),
            Comparison::Behind {
                shard,
                theirs,
                ours,
            } => format!(
                "behind on {shard}: candidate is at LSN {theirs}, this node at {ours}; \
                 electing it would lose writes this node holds"
            ),
            Comparison::Incomparable { theirs, ours } => format!(
                "candidate is on epoch {theirs} and this node on {ours}; positions from \
                 different streams cannot be ordered, so the candidate cannot be shown to \
                 hold this node's writes"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(epoch: u64, pairs: &[(u32, Lsn)]) -> Durability {
        let mut x = Durability::new(epoch);
        for &(s, l) in pairs {
            x.shards.insert(ShardId(s), l);
        }
        x
    }

    #[test]
    fn a_candidate_ahead_everywhere_may_lead() {
        let candidate = d(1, &[(0, 10), (1, 20)]);
        let voter = d(1, &[(0, 8), (1, 20)]);
        assert_eq!(candidate.compare_to(&voter), Comparison::AtLeast);
        assert!(candidate.may_lead(&voter));
    }

    #[test]
    fn a_candidate_behind_on_any_single_shard_is_refused() {
        // The failure this rule exists to prevent: far ahead on a busy shard, behind on a
        // quiet one. Any aggregate comparison elects it and loses the quiet shard's writes.
        let candidate = d(1, &[(0, 10_000), (1, 4)]);
        let voter = d(1, &[(0, 9_000), (1, 5)]);
        match candidate.compare_to(&voter) {
            Comparison::Behind {
                shard,
                theirs,
                ours,
            } => {
                assert_eq!(shard, ShardId(1));
                assert_eq!((theirs, ours), (4, 5));
            }
            other => panic!("expected Behind, got {other:?}"),
        }
        assert!(!candidate.may_lead(&voter));
    }

    #[test]
    fn a_candidate_that_never_heard_of_a_shard_cannot_lead_it() {
        // Iterating the candidate's shards instead of the voter's would let a node win by
        // knowing less, which is the same lost-write bug wearing a different hat.
        let candidate = d(1, &[(0, 100)]);
        let voter = d(1, &[(0, 100), (7, 1)]);
        assert!(
            !candidate.may_lead(&voter),
            "a shard the candidate has never seen must count as position 0, not as agreement"
        );
        match candidate.compare_to(&voter) {
            Comparison::Behind { shard, theirs, .. } => {
                assert_eq!(shard, ShardId(7));
                assert_eq!(theirs, 0);
            }
            other => panic!("expected Behind, got {other:?}"),
        }
    }

    #[test]
    fn different_epochs_are_refused_rather_than_ordered() {
        // The tempting approximation is to order by (epoch, lsn). This candidate would win
        // it — higher epoch — while holding far less data.
        let candidate = d(5, &[(0, 3)]);
        let voter = d(4, &[(0, 900)]);
        match candidate.compare_to(&voter) {
            Comparison::Incomparable { theirs, ours } => assert_eq!((theirs, ours), (5, 4)),
            other => panic!("a higher epoch must not stand in for being ahead: {other:?}"),
        }
        assert!(!candidate.may_lead(&voter));

        // And the refusal is symmetric — a lower epoch is equally incomparable, not "behind".
        assert!(!voter.may_lead(&candidate));
    }

    #[test]
    fn an_equal_position_may_lead() {
        // Ties are allowed, or a cluster where every node is caught up could never elect.
        let a = d(2, &[(0, 5), (1, 5)]);
        let b = d(2, &[(0, 5), (1, 5)]);
        assert!(a.may_lead(&b));
        assert!(b.may_lead(&a));
    }

    #[test]
    fn a_voter_holding_nothing_accepts_anyone_in_its_epoch() {
        let candidate = d(1, &[(0, 10)]);
        let voter = Durability::new(1);
        assert!(candidate.may_lead(&voter));
    }

    #[test]
    fn a_refusal_explains_itself() {
        let candidate = d(1, &[(0, 1)]);
        let voter = d(1, &[(0, 9)]);
        let why = candidate.compare_to(&voter).why();
        assert!(why.contains("shard_0"), "{why}");
        assert!(why.contains('1') && why.contains('9'), "{why}");

        let why = d(2, &[]).compare_to(&d(3, &[])).why();
        assert!(why.contains("epoch"), "{why}");
    }
}
