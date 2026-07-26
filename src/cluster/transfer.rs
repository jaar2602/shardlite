//! Resumable whole-shard transfer reconciliation.
//!
//! Bulk data moves directly from source to destination. Only small, guarded phase updates go
//! through the catalog leader, so a retry after process or leader failure resumes from the
//! follower position and durable operation record instead of guessing where the copy stopped.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use crate::error::{Error, Result};
use crate::net::{Client, Replica, ReplicaConfig};
use crate::replication::Follower;
use crate::shard::{ShardId, ShardManager};

use super::{
    CatalogCommand, CatalogControl, CatalogStore, ClusterNode, OperationKind, OperationPhase,
    TransferProgress,
};

pub struct TransferSupervisor {
    node: u64,
    manager: Arc<ShardManager>,
    catalog: Arc<CatalogStore>,
    cluster: Arc<ClusterNode>,
    control: Arc<CatalogControl>,
    credentials: Option<(String, String)>,
    staging_dir: PathBuf,
    last_catalog_repair: Mutex<Instant>,
}

impl TransferSupervisor {
    pub fn new(
        node: u64,
        manager: Arc<ShardManager>,
        catalog: Arc<CatalogStore>,
        cluster: Arc<ClusterNode>,
        control: Arc<CatalogControl>,
        credentials: Option<(String, String)>,
    ) -> Self {
        let staging_dir = manager.dir().join("cluster").join("transfers");
        Self {
            node,
            manager,
            catalog,
            cluster,
            control,
            credentials,
            staging_dir,
            last_catalog_repair: Mutex::new(Instant::now() - Duration::from_secs(1)),
        }
    }

    pub fn run(&self, interval: Duration) {
        while !self.cluster.is_stopped() {
            if let Err(error) = self.tick_once() {
                tracing::warn!(node = self.node, %error, "transfer reconciliation will retry");
            }
            std::thread::sleep(interval);
        }
    }

    pub fn tick_once(&self) -> Result<()> {
        if self.cluster.is_leader() {
            let mut repaired = self.last_catalog_repair.lock().expect("repair clock");
            if repaired.elapsed() >= Duration::from_secs(1) {
                self.control.repair_cluster()?;
                *repaired = Instant::now();
            }
        }
        let catalog = self.catalog.snapshot();
        self.manager
            .set_catalog_routing(catalog.routing, catalog.routing_epoch)?;

        // A join command can lose its response after the learner has durably installed the
        // catalog. When that learner starts (or restarts), its own durable version is proof of
        // catch-up; finish the idempotent promotion instead of requiring the operator to rerun
        // `join`.
        if let Some(operation) = catalog.operations.values().find(|operation| {
            operation.kind == OperationKind::Join
                && operation.destination == Some(self.node)
                && !operation.phase.terminal()
        }) {
            self.submit(CatalogCommand::CompleteJoin {
                operation: operation.id,
                durable_catalog_version: catalog.version,
            })?;
            return Ok(());
        }

        let Some(operation) = catalog.operations.values().find(|operation| {
            operation.kind == OperationKind::Transfer && !operation.phase.terminal()
        }) else {
            if self.cluster.is_leader()
                && let Some(drain) = catalog.operations.values().find(|operation| {
                    operation.kind == OperationKind::Drain && !operation.phase.terminal()
                })
            {
                let source = drain
                    .source
                    .ok_or_else(|| Error::ClusterConfig("drain has no source node".into()))?;
                let still_used = catalog.placements.values().any(|placement| {
                    placement.primary == source || placement.replicas.contains(&source)
                });
                self.control.apply(if still_used {
                    CatalogCommand::Rebalance
                } else {
                    CatalogCommand::CompleteDrain {
                        operation: drain.id,
                    }
                })?;
                return Ok(());
            }
            // Dynamic mode is intentionally "add a node and let the cluster use it". Start one
            // stable move—or one linear split when there are too few write lanes—then wait for its
            // durable operation to finish before planning the next.
            let split_blocked = catalog.operations.values().rev().any(|operation| {
                operation.kind == OperationKind::Split
                    && operation.phase == OperationPhase::Aborted
                    && operation.routing_before == Some(catalog.routing)
            });
            if self.cluster.is_leader()
                && !split_blocked
                && matches!(
                    super::next_rebalance(&catalog)?,
                    super::RebalanceDecision::Transfer { .. }
                        | super::RebalanceDecision::NeedsSplit { .. }
                )
            {
                self.control.apply(CatalogCommand::Rebalance)?;
            }
            return Ok(());
        };
        let shard = operation
            .shard
            .ok_or_else(|| Error::ClusterConfig("transfer has no shard".into()))?;
        let source = operation
            .source
            .ok_or_else(|| Error::ClusterConfig("transfer has no source".into()))?;
        let destination = operation
            .destination
            .ok_or_else(|| Error::ClusterConfig("transfer has no destination".into()))?;

        let local_epoch = self.manager.epoch();
        if self.node == source
            && operation.phase == OperationPhase::Snapshotting
            && self
                .catalog
                .snapshot()
                .scaling_policy
                .max_transfer_source_bytes
                > 0
        {
            let limit = self
                .catalog
                .snapshot()
                .scaling_policy
                .max_transfer_source_bytes;
            let bytes = std::fs::metadata(shard.path(self.manager.dir()))
                .map(|metadata| metadata.len())
                .unwrap_or_default();
            if bytes > limit {
                self.submit(CatalogCommand::Transfer {
                    operation: operation.id,
                    progress: TransferProgress::Abort {
                        reason: format!(
                            "transfer source is {bytes} bytes, above policy max_transfer_source_bytes {limit}"
                        ),
                    },
                })?;
                return Ok(());
            }
        }
        if self.node == source
            && matches!(
                operation.phase,
                OperationPhase::CatchingUp | OperationPhase::Prepared
            )
            && operation.stream_epoch.is_some()
            && operation.stream_epoch != local_epoch
        {
            self.submit(CatalogCommand::Transfer {
                operation: operation.id,
                progress: TransferProgress::Abort {
                    reason: format!(
                        "source restarted from stream epoch {:?} into {:?} before fencing; \
                         automatic repair will plan a fresh transfer",
                        operation.stream_epoch, local_epoch
                    ),
                },
            })?;
            return Ok(());
        }

        // Fencing is the roll-forward boundary. If the source crashed here, close its newly-opened
        // gate again and establish a replacement final position in the new stream. The destination
        // will take a fresh snapshot under this fence before ownership can commit.
        if self.node == source && operation.phase == OperationPhase::Fencing {
            self.cluster
                .fence()
                .close_shard(shard, "catalog transfer reached its fencing barrier");
            self.manager.drain_writes(shard)?;
            let epoch = local_epoch.ok_or_else(|| {
                Error::ClusterConfig(format!(
                    "cannot transfer {shard} without frame capture and a stream epoch"
                ))
            })?;
            let lsn = self.manager.last_lsn(shard);
            if operation.stream_epoch != Some(epoch) {
                self.submit(CatalogCommand::Transfer {
                    operation: operation.id,
                    progress: TransferProgress::RestartFencedSource {
                        epoch,
                        final_lsn: lsn,
                    },
                })?;
                return Ok(());
            }
            if operation.final_lsn.is_none() {
                crate::failpoint::hit("transfer.source.after_fence");
                self.submit(CatalogCommand::Transfer {
                    operation: operation.id,
                    progress: TransferProgress::FinalLsn { epoch, lsn },
                })?;
                return Ok(());
            }
        }

        // The old source, not the leader by inference, advances ownership into cleanup. This makes
        // closing its SQLite handles an acknowledged boundary: a leader cannot mark the operation
        // complete while the source has not yet demoted itself.
        if self.node == source && operation.phase == OperationPhase::Committed {
            self.manager.follow(shard)?;
            crate::failpoint::hit("transfer.source.after_demotion");
            self.submit(CatalogCommand::Transfer {
                operation: operation.id,
                progress: TransferProgress::BeginCleanup,
            })?;
            return Ok(());
        }

        // The leader advances phase boundaries that depend only on already committed evidence.
        if self.cluster.is_leader() {
            let progress = match operation.phase {
                OperationPhase::Planned => Some(TransferProgress::BeginSnapshot),
                OperationPhase::Prepared => Some(TransferProgress::BeginFencing),
                OperationPhase::Fencing
                    if operation.final_lsn.is_some()
                        && operation.durable_lsn.unwrap_or_default()
                            >= operation.final_lsn.unwrap_or(u64::MAX) =>
                {
                    Some(TransferProgress::Commit)
                }
                OperationPhase::Cleaning => Some(TransferProgress::Complete),
                _ => None,
            };
            if let Some(progress) = progress {
                self.control.apply(CatalogCommand::Transfer {
                    operation: operation.id,
                    progress,
                })?;
                return Ok(());
            }
        }

        if self.node == destination
            && matches!(
                operation.phase,
                OperationPhase::Snapshotting | OperationPhase::CatchingUp
            )
        {
            let position = self.pull_from(source, shard)?;
            let progress = if operation.phase == OperationPhase::Snapshotting {
                TransferProgress::SnapshotInstalled {
                    epoch: position.epoch,
                    lsn: position.applied_lsn,
                }
            } else {
                TransferProgress::CatchUp {
                    epoch: position.epoch,
                    durable_lsn: position.applied_lsn,
                }
            };
            self.submit(CatalogCommand::Transfer {
                operation: operation.id,
                progress,
            })?;
            if operation.phase == OperationPhase::CatchingUp {
                // Catch-up is an explicit durable destination acknowledgement, not something the
                // leader infers merely because the snapshot had an LSN. If either command loses
                // its response, both are idempotent and the destination repeats them after
                // restart.
                self.submit(CatalogCommand::Transfer {
                    operation: operation.id,
                    progress: TransferProgress::Prepare,
                })?;
            }
            return Ok(());
        }

        if self.node == destination
            && operation.phase == OperationPhase::Fencing
            && operation.final_lsn.is_some()
            && (operation.snapshot_lsn.is_none()
                || operation.durable_lsn.unwrap_or_default()
                    < operation.final_lsn.unwrap_or(u64::MAX))
        {
            let position = self.pull_from(source, shard)?;
            let progress = if operation.snapshot_lsn.is_none() {
                TransferProgress::FencedBootstrap {
                    epoch: position.epoch,
                    durable_lsn: position.applied_lsn,
                }
            } else {
                TransferProgress::FencedCatchUp {
                    epoch: position.epoch,
                    durable_lsn: position.applied_lsn,
                }
            };
            self.submit(CatalogCommand::Transfer {
                operation: operation.id,
                progress,
            })?;
            return Ok(());
        }

        Ok(())
    }

    fn pull_from(&self, source: u64, shard: ShardId) -> Result<crate::replication::Position> {
        let catalog = self.catalog.snapshot();
        let address = catalog
            .members
            .get(&source)
            .map(|member| member.address.clone())
            .ok_or_else(|| {
                Error::ClusterConfig(format!("transfer source node {source} is not a member"))
            })?;
        self.manager.follow(shard)?;
        let follower = Arc::new(Follower::open(self.manager.dir())?);
        follower.coordinate_with(Arc::clone(self.manager.modes()));
        let replica = Replica::new(
            ReplicaConfig {
                primary_addr: address,
                shards: vec![shard],
                poll_interval: Duration::from_millis(50),
                batch: 256,
                snapshot_chunk: crate::replication::bootstrap::DEFAULT_CHUNK,
                staging_dir: self.staging_dir.clone(),
                node: self.node,
                credentials: self.credentials.clone(),
            },
            Arc::clone(&follower),
        );
        if follower.position(shard) == crate::replication::Position::default() {
            replica.bootstrap_once(shard)?;
            crate::failpoint::hit("transfer.destination.after_snapshot_file");
        }
        replica.sync_once()?;
        crate::failpoint::hit("transfer.destination.after_catchup_file");
        Ok(follower.position(shard))
    }

    fn submit(&self, command: CatalogCommand) -> Result<()> {
        if self.cluster.is_leader() {
            self.control.apply(command)?;
            return Ok(());
        }
        let leader = self.cluster.leader().ok_or_else(|| {
            Error::ClusterConfig("catalog has no elected leader; transfer will retry".into())
        })?;
        let address = self.cluster.peer_addr(leader).ok_or_else(|| {
            Error::ClusterConfig(format!("catalog leader node {leader} has no address"))
        })?;
        let credentials = self
            .credentials
            .as_ref()
            .map(|(name, secret)| (name.clone(), crate::net::auth::derive_key(secret)));
        let mut client = Client::connect_full(
            &address,
            Duration::from_secs(2),
            Duration::from_secs(10),
            credentials,
        )?;
        client.catalog_command(command)?;
        Ok(())
    }
}

impl std::fmt::Debug for TransferSupervisor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TransferSupervisor")
            .field("node", &self.node)
            .field("catalog_version", &self.catalog.snapshot().version)
            .finish_non_exhaustive()
    }
}
