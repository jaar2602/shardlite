//! Stable, one-operation-at-a-time primary placement planning.
//!
//! The old placement code recomputes every assignment whenever reachability changes. That is a
//! useful failure detector, but a poor rebalance policy: adding one node can move most shards at
//! once. This planner treats the catalog map as durable truth and proposes at most one transfer
//! from it. Re-running after each committed transfer converges without moving a shard that does not
//! improve balance.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::shard::ShardId;

use super::catalog::{Catalog, MemberState, OperationKind};
use super::term::NodeId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RebalanceDecision {
    /// No movement is useful with the current shard and eligible member counts.
    Balanced,
    /// A previous topology operation owns the single-operation budget.
    Waiting { operation: u64 },
    /// There are fewer shards than eligible storage nodes. Moving a shard would merely make a
    /// different node idle; a logical split is required to add a write lane.
    NeedsSplit {
        active_shards: u32,
        eligible_members: usize,
    },
    /// Bootstrap this shard to `destination`, then execute the fenced transfer state machine.
    Transfer {
        shard: ShardId,
        source: NodeId,
        destination: NodeId,
        expected_generation: u64,
    },
}

/// Choose the next movement while preserving every existing assignment not named by the result.
pub fn next_rebalance(catalog: &Catalog) -> Result<RebalanceDecision> {
    if let Some(operation) = catalog.operations.values().find(|operation| {
        matches!(
            operation.kind,
            OperationKind::Transfer | OperationKind::Split
        ) && !operation.phase.terminal()
    }) {
        return Ok(RebalanceDecision::Waiting {
            operation: operation.id,
        });
    }

    if catalog.placements.len() != catalog.routing.shard_count() as usize {
        return Err(Error::ClusterConfig(format!(
            "cannot rebalance an incomplete placement map: {} of {} active shards are assigned",
            catalog.placements.len(),
            catalog.routing.shard_count()
        )));
    }

    let eligible: Vec<NodeId> = catalog
        .members
        .values()
        .filter(|member| member.may_receive_placement())
        .map(|member| member.node)
        .collect();
    if eligible.is_empty() {
        return Err(Error::ClusterConfig(
            "cannot rebalance: no active storage or voter member may receive a primary".into(),
        ));
    }

    let mut counts: BTreeMap<NodeId, usize> = catalog
        .members
        .keys()
        .copied()
        .map(|node| (node, 0))
        .collect();
    for placement in catalog.placements.values() {
        *counts.entry(placement.primary).or_default() += 1;
    }

    // Draining and cordoned owners are evacuated before ordinary balance. A cordon must not leave
    // a primary behind merely because the remaining active nodes are already evenly loaded.
    if let Some((&shard, placement)) = catalog.placements.iter().find(|(_, placement)| {
        catalog
            .members
            .get(&placement.primary)
            .is_some_and(|member| member.state != MemberState::Active)
    }) {
        let destination =
            least_loaded(&eligible, &counts, Some(placement.primary)).ok_or_else(|| {
                Error::ClusterConfig(format!(
                    "cannot evacuate node {}: no other active storage member is available",
                    placement.primary
                ))
            })?;
        return Ok(RebalanceDecision::Transfer {
            shard,
            source: placement.primary,
            destination,
            expected_generation: placement.generation,
        });
    }

    if catalog.routing.shard_count() < eligible.len() as u32 {
        return Ok(RebalanceDecision::NeedsSplit {
            active_shards: catalog.routing.shard_count(),
            eligible_members: eligible.len(),
        });
    }

    let (&source, &source_count) = eligible
        .iter()
        .map(|node| (node, counts.get(node).unwrap_or(&0)))
        .max_by_key(|(node, count)| (**count, std::cmp::Reverse(**node)))
        .expect("eligible is non-empty");
    let (&destination, &destination_count) = eligible
        .iter()
        .map(|node| (node, counts.get(node).unwrap_or(&0)))
        .min_by_key(|(node, count)| (**count, **node))
        .expect("eligible is non-empty");

    if source_count <= destination_count + 1 {
        return Ok(RebalanceDecision::Balanced);
    }

    // Prefer a caught-up replica on the least-loaded destination. Otherwise choose the lowest
    // shard ID, making the bootstrap plan deterministic and stable across retries.
    let (&shard, placement) = catalog
        .placements
        .iter()
        .filter(|(_, placement)| placement.primary == source)
        .min_by_key(|(shard, placement)| (!placement.replicas.contains(&destination), shard.0))
        .expect("a positive source count has a placement");

    Ok(RebalanceDecision::Transfer {
        shard,
        source,
        destination,
        expected_generation: placement.generation,
    })
}

/// Persist the planner's next transfer into the catalog. Balanced/split/waiting decisions are
/// returned unchanged and do not mutate it.
pub fn plan_next(catalog: &mut Catalog) -> Result<RebalanceDecision> {
    let decision = next_rebalance(catalog)?;
    if let RebalanceDecision::Transfer {
        shard, destination, ..
    } = decision
    {
        catalog.start_transfer(shard, destination)?;
    }
    Ok(decision)
}

fn least_loaded(
    eligible: &[NodeId],
    counts: &BTreeMap<NodeId, usize>,
    exclude: Option<NodeId>,
) -> Option<NodeId> {
    eligible
        .iter()
        .copied()
        .filter(|node| Some(*node) != exclude)
        .min_by_key(|node| (counts.get(node).copied().unwrap_or_default(), *node))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{ClusterId, Compatibility, MemberRole, OperationPhase, ReplicaSet};
    use crate::shard::{LinearV1, Routing};

    fn catalog(shards: u32, nodes: u64) -> Catalog {
        let routing = Routing::LinearV1(LinearV1::for_shard_count(shards).unwrap());
        let mut catalog = Catalog::bootstrap(
            ClusterId([9; 16]),
            1,
            "node-1:4600".into(),
            routing,
            Compatibility::current(routing).unwrap(),
        )
        .unwrap();
        for node in 2..=nodes {
            catalog
                .add_learner(node, 1, format!("node-{node}:4600"))
                .unwrap();
            catalog.promote(node, MemberRole::Storage).unwrap();
        }
        catalog.initialize_placements(1).unwrap();
        catalog
    }

    #[test]
    fn adding_a_node_moves_only_one_shard_per_plan() {
        let mut catalog = catalog(12, 3);
        catalog.add_learner(4, 1, "node-4:4600".into()).unwrap();
        catalog.promote(4, MemberRole::Storage).unwrap();
        let before = catalog.placements.clone();

        let decision = plan_next(&mut catalog).unwrap();
        let RebalanceDecision::Transfer {
            source,
            destination,
            ..
        } = decision
        else {
            panic!("expected a transfer, got {decision:?}");
        };
        assert_eq!((source, destination), (1, 4));
        assert_eq!(
            catalog.placements, before,
            "planning must not cut ownership over"
        );
        assert_eq!(
            catalog
                .operations
                .values()
                .filter(|operation| !operation.phase.terminal())
                .count(),
            1
        );
        assert!(matches!(
            next_rebalance(&catalog).unwrap(),
            RebalanceDecision::Waiting { .. }
        ));
    }

    #[test]
    fn already_balanced_placement_does_not_churn() {
        let catalog = catalog(12, 3);
        assert_eq!(
            next_rebalance(&catalog).unwrap(),
            RebalanceDecision::Balanced
        );
    }

    #[test]
    fn too_few_shards_requests_a_split_instead_of_moving_idleness() {
        let catalog = catalog(2, 3);
        assert_eq!(
            next_rebalance(&catalog).unwrap(),
            RebalanceDecision::NeedsSplit {
                active_shards: 2,
                eligible_members: 3,
            }
        );
    }

    #[test]
    fn drain_is_prioritised_even_when_remaining_members_take_extra_load() {
        let mut catalog = catalog(4, 2);
        catalog.set_member_state(1, MemberState::Draining).unwrap();
        assert!(matches!(
            next_rebalance(&catalog).unwrap(),
            RebalanceDecision::Transfer {
                source: 1,
                destination: 2,
                ..
            }
        ));
    }

    #[test]
    fn caught_up_replica_is_preferred_over_a_new_bootstrap() {
        let mut catalog = catalog(6, 2);
        catalog.add_learner(3, 1, "node-3:4600".into()).unwrap();
        catalog.promote(3, MemberRole::Storage).unwrap();
        // Node 1 is most loaded and node 3 least loaded. Make shard 2 its existing replica.
        catalog.placements.insert(
            ShardId(2),
            ReplicaSet {
                generation: 1,
                primary: 1,
                replicas: vec![3],
            },
        );
        assert!(matches!(
            next_rebalance(&catalog).unwrap(),
            RebalanceDecision::Transfer {
                shard: ShardId(2),
                source: 1,
                destination: 3,
                ..
            }
        ));
    }

    #[test]
    fn incomplete_map_is_refused() {
        let mut catalog = catalog(4, 2);
        catalog.placements.remove(&ShardId(3));
        assert!(
            next_rebalance(&catalog)
                .unwrap_err()
                .to_string()
                .contains("incomplete")
        );
    }

    #[test]
    fn join_intent_does_not_block_rebalance_but_a_split_does() {
        let mut catalog = catalog(4, 2);
        let join = catalog
            .start_operation(OperationKind::Join, None, None, Some(3), None)
            .unwrap();
        assert_eq!(
            next_rebalance(&catalog).unwrap(),
            RebalanceDecision::Balanced
        );
        catalog.operations.get_mut(&join).unwrap().phase = OperationPhase::Complete;
        let operation = catalog
            .start_operation(
                OperationKind::Split,
                Some(ShardId(0)),
                Some(1),
                None,
                Some(1),
            )
            .unwrap();
        assert_eq!(
            next_rebalance(&catalog).unwrap(),
            RebalanceDecision::Waiting { operation }
        );
        catalog.operations.get_mut(&operation).unwrap().phase = OperationPhase::Aborted;
        assert_eq!(
            next_rebalance(&catalog).unwrap(),
            RebalanceDecision::Balanced
        );
    }
}
