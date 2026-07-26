//! Typed, idempotent catalog mutations shared by native RPC, HTTP, and the reconciler.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::{
    Catalog, CatalogQuorum, ClusterId, Compatibility, MemberRole, MemberState, OperationKind,
    OperationPhase, RebalanceDecision,
};
use crate::error::{Error, Result};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransferProgress {
    BeginSnapshot,
    SnapshotInstalled { epoch: u64, lsn: u64 },
    CatchUp { epoch: u64, durable_lsn: u64 },
    Prepare,
    BeginFencing,
    FinalLsn { epoch: u64, lsn: u64 },
    RestartFencedSource { epoch: u64, final_lsn: u64 },
    FencedBootstrap { epoch: u64, durable_lsn: u64 },
    FencedCatchUp { epoch: u64, durable_lsn: u64 },
    Commit,
    BeginCleanup,
    Complete,
    Abort { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SplitProgress {
    BeginSnapshot,
    SnapshotReady { barrier: u64 },
    Replay { sequence: u64 },
    Prepare,
    BeginFencing,
    Fenced { sequence: u64 },
    Commit,
    BeginCleanup,
    Complete,
    Abort { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CatalogCommand {
    Join {
        local_cluster: Option<ClusterId>,
        compatibility: Compatibility,
        node: u64,
        incarnation: u64,
        address: String,
    },
    CompleteJoin {
        operation: u64,
        durable_catalog_version: u64,
    },
    Rebalance,
    Transfer {
        operation: u64,
        progress: TransferProgress,
    },
    Split {
        operation: u64,
        progress: SplitProgress,
    },
    Cordon {
        node: u64,
        cordoned: bool,
    },
    Drain {
        node: u64,
    },
    CompleteDrain {
        operation: u64,
    },
    Remove {
        node: u64,
    },
    BeginVoterChange {
        voters: std::collections::BTreeSet<u64>,
    },
    FinalizeVoterChange,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogCommandResult {
    pub catalog: Catalog,
    pub operation: Option<u64>,
    pub rebalance: Option<RebalanceDecision>,
}

pub struct CatalogControl {
    quorum: Arc<CatalogQuorum>,
}

impl CatalogControl {
    pub fn new(quorum: Arc<CatalogQuorum>) -> Self {
        Self { quorum }
    }

    pub fn snapshot(&self) -> Catalog {
        self.quorum.snapshot()
    }

    /// Best-effort dissemination of the latest committed snapshot to storage/learner members that
    /// may have been offline when it originally committed.
    pub fn repair_members(&self) -> Result<()> {
        self.quorum.repair_members()
    }

    /// Repair catalog state after a coordinator or voter crash.
    ///
    /// A prepared value is recovered first because it may already have been chosen by a quorum.
    /// Re-publishing then clears prepared markers on voters that missed the old leader's commit,
    /// and snapshot dissemination catches up members that were offline for several versions.
    /// Finally, an interrupted joint voter transition is completed through the joint quorum
    /// rather than left waiting for an operator to repeat the second command.
    pub fn repair_cluster(&self) -> Result<()> {
        if self.quorum.recover_prepared()? {
            // Recovery committed a newer catalog value. Let callers refresh their snapshot before
            // attempting another state-machine transition.
            return Ok(());
        }
        self.quorum.republish_committed()?;
        self.quorum.repair_members()?;
        if self.quorum.snapshot().voter_transition.is_some() {
            self.apply(CatalogCommand::FinalizeVoterChange)?;
        }
        Ok(())
    }

    pub fn apply(&self, command: CatalogCommand) -> Result<CatalogCommandResult> {
        if let CatalogCommand::BeginVoterChange { voters } = &command
            && self
                .quorum
                .snapshot()
                .voter_transition
                .as_ref()
                .is_some_and(|transition| &transition.new == voters)
        {
            self.quorum.repair_members()?;
            return Ok(CatalogCommandResult {
                catalog: self.quorum.snapshot(),
                operation: None,
                rebalance: None,
            });
        }
        if matches!(&command, CatalogCommand::FinalizeVoterChange)
            && self.quorum.snapshot().voter_transition.is_none()
        {
            return Ok(CatalogCommandResult {
                catalog: self.quorum.snapshot(),
                operation: None,
                rebalance: None,
            });
        }
        let boundary = command_boundary(&command);
        let is_rebalance = matches!(&command, CatalogCommand::Rebalance);
        let mut operation = None;
        let mut rebalance = None;
        self.quorum.change_current(|catalog| {
            match command {
                CatalogCommand::Join {
                    local_cluster,
                    ref compatibility,
                    node,
                    incarnation,
                    ref address,
                } => {
                    if let Some(member) = catalog.members.get(&node) {
                        if member.incarnation != incarnation || member.address != *address {
                            return Err(Error::ClusterConfig(format!(
                                "node {node} is already registered as incarnation {} at {}",
                                member.incarnation, member.address
                            )));
                        }
                        operation = catalog
                            .operations
                            .values()
                            .find(|candidate| {
                                candidate.kind == OperationKind::Join
                                    && candidate.destination == Some(node)
                            })
                            .map(|candidate| candidate.id);
                    } else {
                        operation = Some(catalog.start_join(
                            local_cluster,
                            compatibility,
                            node,
                            incarnation,
                            address.clone(),
                        )?);
                    }
                }
                CatalogCommand::CompleteJoin {
                    operation: id,
                    durable_catalog_version,
                } => {
                    let phase = catalog
                        .operations
                        .get(&id)
                        .ok_or_else(|| {
                            Error::ClusterConfig(format!("operation {id} does not exist"))
                        })?
                        .phase;
                    if phase != OperationPhase::Complete {
                        catalog.record_join_catch_up(id, durable_catalog_version)?;
                        catalog.prepare_join(id)?;
                        catalog.commit_storage_join(id)?;
                        catalog.complete_join(id)?;
                    }
                    operation = Some(id);
                }
                CatalogCommand::Rebalance => {
                    let decision = super::rebalance::plan_next(catalog)?;
                    operation = match decision {
                        RebalanceDecision::Transfer { shard, .. } => catalog
                            .operations
                            .values()
                            .find(|candidate| {
                                candidate.kind == OperationKind::Transfer
                                    && candidate.shard == Some(shard)
                                    && !candidate.phase.terminal()
                            })
                            .map(|candidate| candidate.id),
                        RebalanceDecision::Waiting { operation: id } => Some(id),
                        RebalanceDecision::NeedsSplit { .. } => Some(catalog.start_split()?),
                        _ => None,
                    };
                    rebalance = Some(decision);
                }
                CatalogCommand::Transfer {
                    operation: id,
                    ref progress,
                } => {
                    apply_transfer(catalog, id, progress)?;
                    operation = Some(id);
                }
                CatalogCommand::Split {
                    operation: id,
                    ref progress,
                } => {
                    apply_split(catalog, id, progress)?;
                    operation = Some(id);
                }
                CatalogCommand::Cordon { node, cordoned } => {
                    catalog.set_member_state(
                        node,
                        if cordoned {
                            MemberState::Cordoned
                        } else {
                            MemberState::Active
                        },
                    )?;
                }
                CatalogCommand::Drain { node } => {
                    operation = catalog
                        .operations
                        .values()
                        .find(|candidate| {
                            candidate.kind == OperationKind::Drain
                                && candidate.source == Some(node)
                                && !candidate.phase.terminal()
                        })
                        .map(|candidate| candidate.id);
                    if operation.is_none() {
                        operation = Some(catalog.start_drain(node)?);
                    }
                }
                CatalogCommand::CompleteDrain { operation: id } => {
                    catalog.complete_drain(id)?;
                    operation = Some(id);
                }
                CatalogCommand::Remove { node } => {
                    if catalog
                        .members
                        .get(&node)
                        .is_some_and(|member| member.role == MemberRole::Voter)
                    {
                        return Err(Error::ClusterConfig(
                            "remove the node from the voter set before removing membership".into(),
                        ));
                    }
                    catalog.remove_member(node)?;
                }
                CatalogCommand::BeginVoterChange { ref voters } => {
                    catalog.begin_voter_change(voters.clone())?;
                }
                CatalogCommand::FinalizeVoterChange => {
                    catalog.finalize_voter_change()?;
                }
            }
            Ok(())
        })?;
        let result = CatalogCommandResult {
            catalog: self.quorum.snapshot(),
            operation,
            rebalance,
        };
        crate::failpoint::hit(boundary);
        if is_rebalance {
            match result.rebalance {
                Some(RebalanceDecision::Transfer { .. }) => {
                    crate::failpoint::hit("transfer.after_planned")
                }
                Some(RebalanceDecision::NeedsSplit { .. }) => {
                    crate::failpoint::hit("split.after_planned")
                }
                _ => {}
            }
        }
        Ok(result)
    }
}

fn command_boundary(command: &CatalogCommand) -> &'static str {
    match command {
        CatalogCommand::Join { .. } => "join.after_registered",
        CatalogCommand::CompleteJoin { .. } => "join.after_complete",
        CatalogCommand::Rebalance => "catalog.rebalance.after_commit",
        CatalogCommand::Transfer { progress, .. } => match progress {
            TransferProgress::BeginSnapshot => "transfer.after_snapshotting",
            TransferProgress::SnapshotInstalled { .. } => "transfer.after_snapshot_installed",
            TransferProgress::CatchUp { .. } => "transfer.after_catchup",
            TransferProgress::Prepare => "transfer.after_prepared",
            TransferProgress::BeginFencing => "transfer.after_fencing",
            TransferProgress::FinalLsn { .. } => "transfer.after_final_lsn",
            TransferProgress::RestartFencedSource { .. } => "transfer.after_fenced_stream_restart",
            TransferProgress::FencedBootstrap { .. } => "transfer.after_fenced_bootstrap",
            TransferProgress::FencedCatchUp { .. } => "transfer.after_fenced_catchup",
            TransferProgress::Commit => "transfer.after_committed",
            TransferProgress::BeginCleanup => "transfer.after_cleaning",
            TransferProgress::Complete => "transfer.after_complete",
            TransferProgress::Abort { .. } => "transfer.after_aborted",
        },
        CatalogCommand::Split { progress, .. } => match progress {
            SplitProgress::BeginSnapshot => "split.after_snapshotting",
            SplitProgress::SnapshotReady { .. } => "split.after_snapshot_ready",
            SplitProgress::Replay { .. } => "split.after_replay",
            SplitProgress::Prepare => "split.after_prepared",
            SplitProgress::BeginFencing => "split.after_fencing",
            SplitProgress::Fenced { .. } => "split.after_fenced",
            SplitProgress::Commit => "split.after_committed",
            SplitProgress::BeginCleanup => "split.after_cleaning",
            SplitProgress::Complete => "split.after_complete",
            SplitProgress::Abort { .. } => "split.after_aborted",
        },
        CatalogCommand::Cordon { .. } => "member.after_cordon",
        CatalogCommand::Drain { .. } => "member.after_drain",
        CatalogCommand::CompleteDrain { .. } => "member.after_drain_complete",
        CatalogCommand::Remove { .. } => "member.after_remove",
        CatalogCommand::BeginVoterChange { .. } => "voter.after_joint_commit",
        CatalogCommand::FinalizeVoterChange => "voter.after_finalize_commit",
    }
}

fn apply_split(catalog: &mut Catalog, id: u64, progress: &SplitProgress) -> Result<()> {
    let current = catalog
        .operations
        .get(&id)
        .ok_or_else(|| Error::ClusterConfig(format!("operation {id} does not exist")))?
        .phase;
    match progress {
        SplitProgress::BeginSnapshot if current == OperationPhase::Planned => {
            catalog.begin_split_snapshot(id)
        }
        SplitProgress::SnapshotReady { barrier } if current == OperationPhase::Snapshotting => {
            catalog.split_snapshot_ready(id, *barrier)
        }
        SplitProgress::Replay { sequence } if current == OperationPhase::CatchingUp => {
            catalog.record_split_replay(id, *sequence)
        }
        SplitProgress::Prepare if current == OperationPhase::CatchingUp => {
            catalog.prepare_split(id)
        }
        SplitProgress::BeginFencing if current == OperationPhase::Prepared => {
            catalog.begin_split_fencing(id)
        }
        SplitProgress::Fenced { sequence } if current == OperationPhase::Fencing => {
            catalog.split_fenced(id, *sequence)
        }
        SplitProgress::Commit if current == OperationPhase::Installing => catalog.commit_split(id),
        SplitProgress::BeginCleanup if current == OperationPhase::Committed => {
            catalog.begin_split_cleanup(id)
        }
        SplitProgress::Complete if current == OperationPhase::Cleaning => {
            catalog.complete_split(id)
        }
        SplitProgress::Abort { reason } if current.may_abort() => {
            catalog.abort_operation(id, reason.clone())
        }
        _ if split_progress_already_applied(current, progress) => Ok(()),
        _ => Err(Error::ClusterConfig(format!(
            "split operation {id} is in phase {current:?}; command {progress:?} is stale or out of \
             order"
        ))),
    }
}

fn split_progress_already_applied(phase: OperationPhase, progress: &SplitProgress) -> bool {
    if phase == OperationPhase::Aborted {
        return matches!(progress, SplitProgress::Abort { .. });
    }
    let rank = |phase| match phase {
        OperationPhase::Planned => 0,
        OperationPhase::Snapshotting => 1,
        OperationPhase::CatchingUp => 2,
        OperationPhase::Prepared => 3,
        OperationPhase::Fencing => 4,
        OperationPhase::Installing => 5,
        OperationPhase::Committed => 6,
        OperationPhase::Cleaning => 7,
        OperationPhase::Complete => 8,
        OperationPhase::Aborted => 9,
    };
    let target = match progress {
        SplitProgress::BeginSnapshot => OperationPhase::Snapshotting,
        SplitProgress::SnapshotReady { .. } | SplitProgress::Replay { .. } => {
            OperationPhase::CatchingUp
        }
        SplitProgress::Prepare => OperationPhase::Prepared,
        SplitProgress::BeginFencing => OperationPhase::Fencing,
        SplitProgress::Fenced { .. } => OperationPhase::Installing,
        SplitProgress::Commit => OperationPhase::Committed,
        SplitProgress::BeginCleanup => OperationPhase::Cleaning,
        SplitProgress::Complete => OperationPhase::Complete,
        SplitProgress::Abort { .. } => OperationPhase::Aborted,
    };
    rank(phase) >= rank(target)
}

fn apply_transfer(catalog: &mut Catalog, id: u64, progress: &TransferProgress) -> Result<()> {
    let current = catalog
        .operations
        .get(&id)
        .ok_or_else(|| Error::ClusterConfig(format!("operation {id} does not exist")))?
        .phase;
    match progress {
        TransferProgress::BeginSnapshot if current == OperationPhase::Planned => {
            catalog.begin_snapshot(id)
        }
        TransferProgress::SnapshotInstalled { epoch, lsn }
            if current == OperationPhase::Snapshotting =>
        {
            catalog.snapshot_installed(id, *epoch, *lsn)
        }
        TransferProgress::CatchUp { epoch, durable_lsn }
            if current == OperationPhase::CatchingUp =>
        {
            catalog.record_catch_up(id, *epoch, *durable_lsn)
        }
        TransferProgress::Prepare if current == OperationPhase::CatchingUp => {
            catalog.prepare_transfer(id)
        }
        TransferProgress::BeginFencing if current == OperationPhase::Prepared => {
            catalog.begin_fencing(id)
        }
        TransferProgress::FinalLsn { epoch, lsn } if current == OperationPhase::Fencing => {
            catalog.record_final_lsn(id, *epoch, *lsn)
        }
        TransferProgress::RestartFencedSource { epoch, final_lsn }
            if current == OperationPhase::Fencing =>
        {
            catalog.restart_fenced_stream(id, *epoch, *final_lsn)
        }
        TransferProgress::FencedBootstrap { epoch, durable_lsn }
            if current == OperationPhase::Fencing =>
        {
            catalog.record_fenced_bootstrap(id, *epoch, *durable_lsn)
        }
        TransferProgress::FencedCatchUp { epoch, durable_lsn }
            if current == OperationPhase::Fencing =>
        {
            catalog.record_fenced_catch_up(id, *epoch, *durable_lsn)
        }
        TransferProgress::Commit if current == OperationPhase::Fencing => {
            catalog.commit_transfer(id)
        }
        TransferProgress::BeginCleanup if current == OperationPhase::Committed => {
            catalog.begin_cleanup(id)
        }
        TransferProgress::Complete if current == OperationPhase::Cleaning => {
            catalog.complete_transfer(id)
        }
        TransferProgress::Abort { reason } if current.may_abort() => {
            catalog.abort_operation(id, reason.clone())
        }
        // A retried command that has already taken effect is an idempotent success.
        _ if transfer_progress_already_applied(current, progress) => Ok(()),
        _ => Err(Error::ClusterConfig(format!(
            "transfer operation {id} is in phase {current:?}; command {progress:?} is stale or out \
             of order"
        ))),
    }
}

fn transfer_progress_already_applied(phase: OperationPhase, progress: &TransferProgress) -> bool {
    if phase == OperationPhase::Aborted {
        return matches!(progress, TransferProgress::Abort { .. });
    }
    let rank = |phase| match phase {
        OperationPhase::Planned => 0,
        OperationPhase::Snapshotting => 1,
        OperationPhase::CatchingUp => 2,
        OperationPhase::Prepared => 3,
        OperationPhase::Fencing => 4,
        OperationPhase::Committed => 5,
        OperationPhase::Cleaning => 6,
        OperationPhase::Complete => 7,
        OperationPhase::Aborted => 8,
        OperationPhase::Installing => 2,
    };
    let target = match progress {
        TransferProgress::BeginSnapshot => OperationPhase::Snapshotting,
        TransferProgress::SnapshotInstalled { .. } | TransferProgress::CatchUp { .. } => {
            OperationPhase::CatchingUp
        }
        TransferProgress::Prepare => OperationPhase::Prepared,
        TransferProgress::BeginFencing
        | TransferProgress::FinalLsn { .. }
        | TransferProgress::RestartFencedSource { .. }
        | TransferProgress::FencedBootstrap { .. }
        | TransferProgress::FencedCatchUp { .. } => OperationPhase::Fencing,
        TransferProgress::Commit => OperationPhase::Committed,
        TransferProgress::BeginCleanup => OperationPhase::Cleaning,
        TransferProgress::Complete => OperationPhase::Complete,
        TransferProgress::Abort { .. } => OperationPhase::Aborted,
    };
    rank(phase) >= rank(target)
}

impl std::fmt::Debug for CatalogControl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CatalogControl")
            .field("version", &self.quorum.snapshot().version)
            .finish_non_exhaustive()
    }
}
