//! Which node leads which shard — the map that makes writes concurrent.
//!
//! # This is where multi-write actually comes from
//!
//! Everything up to here gives one leader for everything, so writes still serialise onto a
//! single node. Spreading shard primaries across nodes is what lets several nodes accept
//! writes at once. It is the project's headline goal and it is entirely this file plus the
//! per-shard write gate.
//!
//! # One coordinator, not one election per shard
//!
//! The alternative is a Raft group per shard. At the default of 64 shards that is 64 election
//! state machines and 64× the heartbeat traffic, on a third of a CPU. Instead the single
//! elected leader computes the map and publishes it; nodes apply it locally.
//!
//! The coordinator is deliberately **not** in the write path. Clients talk to a shard's
//! primary directly. If the coordinator dies, existing primaries keep accepting writes and
//! only *changes* to the map stall — until a new leader is elected, which is the same
//! election that was already there.
//!
//! # The map is derived, not stored
//!
//! It is recomputed from (live members, shard count, term) rather than being replicated
//! state that has to be kept consistent. This keeps the promise the plan made — the Raft log
//! carries only membership and leadership — and it means a new coordinator does not need to
//! recover the previous map, only to compute one. The cost is that assignments can move when
//! the live set changes, which is why moving a shard has to be safe rather than rare.
//!
//! # Assignment is deterministic given its inputs
//!
//! Two nodes computing from the same inputs get the same answer. That matters because a
//! follower applying a map, and a coordinator that has just been elected, must not disagree
//! about who owns what — disagreement is two writers on one file.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::shard::ShardId;

use super::term::{NodeId, Term};

/// A shard-to-node assignment.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Placement {
    /// The term whose coordinator produced this. A node ignores a map from an older term:
    /// that coordinator has been deposed and its opinion about ownership is stale.
    pub term: Term,
    pub assignments: BTreeMap<ShardId, NodeId>,
}

impl Placement {
    /// Spread `shard_count` shards over `members` as evenly as possible.
    ///
    /// Deterministic in its inputs, and stable in the sense that matters: `members` is sorted
    /// first, so the same set produces the same map regardless of the order it was collected
    /// in. Without that, two coordinators with the same members could disagree — and
    /// disagreement about ownership is two writers on one file.
    pub fn balanced(shard_count: u32, members: &[NodeId], term: Term) -> Self {
        let mut members: Vec<NodeId> = members.to_vec();
        members.sort_unstable();
        members.dedup();

        let mut assignments = BTreeMap::new();
        if members.is_empty() {
            // No one to assign to. An empty map is the honest answer: every node will find
            // itself leading nothing and close its gates, which is better than a map that
            // names a node that is not there.
            return Self { term, assignments };
        }
        for s in 0..shard_count {
            let owner = members[s as usize % members.len()];
            assignments.insert(ShardId(s), owner);
        }
        Self { term, assignments }
    }

    /// Shards assigned to `node`.
    pub fn shards_for(&self, node: NodeId) -> Vec<ShardId> {
        self.assignments
            .iter()
            .filter(|(_, owner)| **owner == node)
            .map(|(&s, _)| s)
            .collect()
    }

    pub fn owner(&self, shard: ShardId) -> Option<NodeId> {
        self.assignments.get(&shard).copied()
    }

    /// Nodes that lead at least one shard.
    pub fn leaders(&self) -> Vec<NodeId> {
        let mut v: Vec<NodeId> = self.assignments.values().copied().collect();
        v.sort_unstable();
        v.dedup();
        v
    }

    /// How lopsided the assignment is: the difference between the busiest and quietest node.
    ///
    /// Exposed because a map that quietly puts everything on one node is indistinguishable
    /// from a healthy one until throughput is measured.
    pub fn imbalance(&self, members: &[NodeId]) -> usize {
        if members.is_empty() {
            return 0;
        }
        let counts: Vec<usize> = members.iter().map(|&n| self.shards_for(n).len()).collect();
        let max = counts.iter().copied().max().unwrap_or(0);
        let min = counts.iter().copied().min().unwrap_or(0);
        max - min
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shards_are_spread_across_every_member() {
        // The multi-write property in its smallest form: more than one node leads something.
        let p = Placement::balanced(8, &[1, 2, 3], 1);
        assert_eq!(p.leaders(), vec![1, 2, 3]);
        assert_eq!(p.assignments.len(), 8);
    }

    #[test]
    fn the_spread_is_even() {
        let members = [1, 2, 3];
        let p = Placement::balanced(64, &members, 1);
        assert_eq!(
            p.imbalance(&members),
            1,
            "64 over 3 nodes should differ by at most one: {:?}",
            members.map(|n| p.shards_for(n).len())
        );
    }

    #[test]
    fn every_shard_has_exactly_one_owner() {
        // Two owners is two writers on one file; zero owners is a shard nobody serves.
        let p = Placement::balanced(16, &[1, 2, 3], 1);
        for s in 0..16 {
            assert!(
                p.owner(ShardId(s)).is_some(),
                "shard {s} was assigned to nobody"
            );
        }
        let total: usize = [1, 2, 3].iter().map(|&n| p.shards_for(n).len()).sum();
        assert_eq!(total, 16, "a shard was assigned twice");
    }

    #[test]
    fn the_same_members_give_the_same_map_whatever_order_they_arrive_in() {
        // Two coordinators disagreeing about ownership is two writers on one file, so the
        // map must not depend on the order the member set happened to be collected in.
        let a = Placement::balanced(32, &[3, 1, 2], 5);
        let b = Placement::balanced(32, &[1, 2, 3], 5);
        let c = Placement::balanced(32, &[2, 3, 1, 3], 5);
        assert_eq!(a, b);
        assert_eq!(b, c, "duplicates must not change the answer either");
    }

    #[test]
    fn losing_a_member_reassigns_its_shards_and_only_its_shards() {
        // Reassignment should be a repair, not a reshuffle. This records what actually
        // happens rather than asserting a stability the modulo scheme does not provide.
        let before = Placement::balanced(6, &[1, 2, 3], 1);
        let after = Placement::balanced(6, &[1, 2], 2);

        assert!(
            after.shards_for(3).is_empty(),
            "the departed node must lead nothing"
        );
        assert_eq!(after.leaders(), vec![1, 2]);
        assert_eq!(after.assignments.len(), 6, "every shard still has an owner");

        // Modulo assignment moves more than the departed node's shards. Recorded, not
        // asserted away: it is the cost of deriving the map instead of storing it, and it is
        // why moving a shard has to be safe rather than rare.
        let moved = (0..6)
            .filter(|&s| before.owner(ShardId(s)) != after.owner(ShardId(s)))
            .count();
        assert!(moved >= 2, "expected reassignment, saw {moved}");
    }

    #[test]
    fn no_members_means_nobody_leads_anything() {
        // Better than a map naming a node that is not there: every node finds it leads
        // nothing and closes its gates, which is a visible outage rather than a silent one.
        let p = Placement::balanced(8, &[], 1);
        assert!(p.assignments.is_empty());
        assert!(p.leaders().is_empty());
        assert!(p.shards_for(1).is_empty());
    }

    #[test]
    fn a_single_member_leads_everything() {
        let p = Placement::balanced(8, &[7], 1);
        assert_eq!(p.shards_for(7).len(), 8);
        assert_eq!(p.leaders(), vec![7]);
    }
}
