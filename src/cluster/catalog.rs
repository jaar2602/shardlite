//! Durable cluster identity, compatibility, routing, membership, and placement metadata.
//!
//! This is the state machine a metadata quorum will replicate. `CatalogStore` is the local
//! durability half: compare-and-apply, checksum, fsync-before-publish, and strict validation.
//! Network quorum replication is deliberately layered above it so a catalog entry cannot be
//! acknowledged remotely before the local voter has made it crash-safe.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::shard::{Routing, ShardId};

use super::term::NodeId;

const DIRECTORY: &str = "cluster";
const FILE: &str = "catalog";
const PREPARED_FILE: &str = "catalog.prepared";
const MAGIC: &[u8] = b"shardlite-catalog-v2\n";
const PREPARED_MAGIC: &[u8] = b"shardlite-catalog-prepared-v2\n";
const MAX_CATALOG_BYTES: usize = 16 * 1024 * 1024;
pub const CATALOG_FORMAT_VERSION: u32 = 2;

/// Stable identity of one cluster. A local data directory may never silently join another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ClusterId(pub [u8; 16]);

impl ClusterId {
    pub fn generate(first_node: NodeId) -> Self {
        static NONCE: AtomicU64 = AtomicU64::new(0);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"shardlite-cluster-id-v1");
        hasher.update(&first_node.to_le_bytes());
        hasher.update(&std::process::id().to_le_bytes());
        hasher.update(&now.to_le_bytes());
        hasher.update(&NONCE.fetch_add(1, Ordering::Relaxed).to_le_bytes());
        let mut id = [0; 16];
        id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
        Self(id)
    }
}

impl fmt::Display for ClusterId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Values that must agree before byte-level replication is safe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Compatibility {
    pub directory_format: u32,
    pub sqlite_version: String,
    pub page_size: u32,
    pub routing_scheme: String,
    pub hash_scheme: String,
}

impl Compatibility {
    pub fn current(routing: Routing) -> Result<Self> {
        let connection = rusqlite::Connection::open_in_memory()?;
        let page_size = connection.query_row("PRAGMA page_size", [], |row| row.get(0))?;
        Ok(Self {
            directory_format: crate::shard::manifest::FORMAT_VERSION,
            sqlite_version: rusqlite::version().to_string(),
            page_size,
            routing_scheme: match routing {
                Routing::ModuloV1(_) => "modulo-v1",
                Routing::LinearV1(_) => "linear-v1",
            }
            .into(),
            hash_scheme: "fnv1a-v1".into(),
        })
    }

    pub fn check(&self, candidate: &Self) -> Result<()> {
        let mismatch = if self.directory_format != candidate.directory_format {
            Some((
                "directory format",
                self.directory_format.to_string(),
                candidate.directory_format.to_string(),
            ))
        } else if self.sqlite_version != candidate.sqlite_version {
            Some((
                "SQLite version",
                self.sqlite_version.clone(),
                candidate.sqlite_version.clone(),
            ))
        } else if self.page_size != candidate.page_size {
            Some((
                "page size",
                self.page_size.to_string(),
                candidate.page_size.to_string(),
            ))
        } else if self.routing_scheme != candidate.routing_scheme {
            Some((
                "routing scheme",
                self.routing_scheme.clone(),
                candidate.routing_scheme.clone(),
            ))
        } else if self.hash_scheme != candidate.hash_scheme {
            Some((
                "hash scheme",
                self.hash_scheme.clone(),
                candidate.hash_scheme.clone(),
            ))
        } else {
            None
        };

        match mismatch {
            Some((field, cluster, joining)) => Err(Error::ClusterConfig(format!(
                "joining node has incompatible {field}: cluster requires `{cluster}`, node has \
                 `{joining}`; refusing before it can receive data or own writes"
            ))),
            None => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberRole {
    Learner,
    Storage,
    Voter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberState {
    Active,
    Cordoned,
    Draining,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    pub node: NodeId,
    pub incarnation: u64,
    pub address: String,
    pub role: MemberRole,
    pub state: MemberState,
    /// Relative primary-placement capacity. A node with weight 4 receives approximately four
    /// times as many primaries as a node with weight 1.
    pub capacity_weight: u32,
    /// Operator supplied topology label (for example an availability zone). Replica placement
    /// prefers distinct non-empty domains.
    pub failure_domain: Option<String>,
}

impl Member {
    pub fn may_receive_placement(&self) -> bool {
        self.role != MemberRole::Learner && self.state == MemberState::Active
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ScalingPolicy {
    /// Maximum retained logical dirty-row records before affected writes receive backpressure.
    pub split_log_max_rows: u64,
    /// Rows durably processed between resumable split checkpoints.
    pub split_backfill_batch_rows: u32,
    /// Refuse a split whose source file exceeds this size. Zero disables the ceiling.
    pub max_split_source_bytes: u64,
    /// Refuse a whole-shard transfer whose source file exceeds this budget. Zero disables it.
    #[serde(default)]
    pub max_transfer_source_bytes: u64,
    /// Current implementation deliberately serializes topology changes; values above one are
    /// refused until per-node movement accounting is implemented.
    pub max_concurrent_topology_operations: u32,
}

impl Default for ScalingPolicy {
    fn default() -> Self {
        Self {
            split_log_max_rows: 100_000,
            split_backfill_batch_rows: 2_048,
            max_split_source_bytes: 0,
            max_transfer_source_bytes: 0,
            max_concurrent_topology_operations: 1,
        }
    }
}

impl ScalingPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.split_log_max_rows == 0 {
            return Err(Error::ClusterConfig(
                "split_log_max_rows must be at least one".into(),
            ));
        }
        if self.split_backfill_batch_rows == 0 {
            return Err(Error::ClusterConfig(
                "split_backfill_batch_rows must be at least one".into(),
            ));
        }
        if self.max_concurrent_topology_operations != 1 {
            return Err(Error::ClusterConfig(
                "max_concurrent_topology_operations must remain 1 in this release".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaSet {
    pub generation: u64,
    pub primary: NodeId,
    pub replicas: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoterTransition {
    pub old: std::collections::BTreeSet<NodeId>,
    pub new: std::collections::BTreeSet<NodeId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationKind {
    Join,
    Transfer,
    Split,
    Drain,
    Remove,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationPhase {
    Planned,
    Snapshotting,
    CatchingUp,
    Prepared,
    Fencing,
    Installing,
    Committed,
    Cleaning,
    Complete,
    Aborted,
}

impl OperationPhase {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Complete | Self::Aborted)
    }

    pub fn may_abort(self) -> bool {
        matches!(
            self,
            Self::Planned | Self::Snapshotting | Self::CatchingUp | Self::Prepared
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogOperation {
    pub id: u64,
    pub kind: OperationKind,
    pub phase: OperationPhase,
    pub shard: Option<ShardId>,
    pub source: Option<NodeId>,
    pub destination: Option<NodeId>,
    pub expected_generation: Option<u64>,
    /// Physical stream identity copied to the destination.
    pub stream_epoch: Option<u64>,
    /// LSN represented by the installed snapshot.
    pub snapshot_lsn: Option<u64>,
    /// Highest LSN the destination has confirmed durable.
    pub durable_lsn: Option<u64>,
    /// Source LSN after its write gate was fenced and its queue drained.
    pub final_lsn: Option<u64>,
    /// Highest catalog version the joining learner has installed durably.
    pub catalog_version: Option<u64>,
    /// Routing states bracketing a logical split. Kept in the operation so crash recovery never
    /// has to infer which linear-hash step an already-installed pair of files belongs to.
    pub routing_before: Option<Routing>,
    pub routing_after: Option<Routing>,
    /// Whether the old primary must remain a replica after cleanup. This is true when the
    /// destination was already a replica before the move; otherwise bootstrap temporarily added
    /// a copy and cleanup removes the source to restore the prior copy count.
    pub retain_source_replica: bool,
    /// Nodes that must install both split images before the routing epoch may publish.
    pub split_required_nodes: BTreeSet<NodeId>,
    /// Required nodes that durably installed both images.
    pub split_installed_nodes: BTreeSet<NodeId>,
    /// Required nodes that removed split capture/staging state after routing commit.
    pub split_cleaned_nodes: BTreeSet<NodeId>,
    pub created_version: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Catalog {
    pub format_version: u32,
    pub cluster_id: ClusterId,
    pub version: u64,
    pub routing_epoch: u64,
    pub compatibility: Compatibility,
    pub routing: Routing,
    pub scaling_policy: ScalingPolicy,
    pub members: BTreeMap<NodeId, Member>,
    /// Joint consensus while changing voters. Decisions require a majority of both sets.
    pub voter_transition: Option<VoterTransition>,
    pub placements: BTreeMap<ShardId, ReplicaSet>,
    pub operations: BTreeMap<u64, CatalogOperation>,
    pub next_operation_id: u64,
}

/// One catalog value durably accepted by a voter but not yet committed.
///
/// The election term is the proposal ballot. A higher term may replace an uncommitted proposal;
/// two different values in the same term/version are refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogProposal {
    pub term: u64,
    pub expected_version: u64,
    pub catalog: Catalog,
}

impl Catalog {
    pub fn bootstrap(
        cluster_id: ClusterId,
        first_node: NodeId,
        address: String,
        routing: Routing,
        compatibility: Compatibility,
    ) -> Result<Self> {
        compatibility.check(&Compatibility::current(routing)?)?;
        let mut members = BTreeMap::new();
        members.insert(
            first_node,
            Member {
                node: first_node,
                incarnation: 1,
                address,
                role: MemberRole::Voter,
                state: MemberState::Active,
                capacity_weight: 1,
                failure_domain: None,
            },
        );
        let catalog = Self {
            format_version: CATALOG_FORMAT_VERSION,
            cluster_id,
            version: 1,
            routing_epoch: 1,
            compatibility,
            routing,
            scaling_policy: ScalingPolicy::default(),
            members,
            voter_transition: None,
            placements: BTreeMap::new(),
            operations: BTreeMap::new(),
            next_operation_id: 1,
        };
        catalog.validate()?;
        Ok(catalog)
    }

    pub fn validate_join(
        &self,
        local_cluster: Option<ClusterId>,
        compatibility: &Compatibility,
    ) -> Result<()> {
        if let Some(local) = local_cluster
            && local != self.cluster_id
        {
            return Err(Error::ClusterConfig(format!(
                "data directory belongs to cluster {local}, not {}; refusing to join with foreign \
                 shard files",
                self.cluster_id
            )));
        }
        self.compatibility.check(compatibility)
    }

    pub fn add_learner(&mut self, node: NodeId, incarnation: u64, address: String) -> Result<()> {
        if address.trim().is_empty() {
            return Err(Error::ClusterConfig(
                "joining node must advertise a non-empty address".into(),
            ));
        }
        if let Some(existing) = self.members.get(&node) {
            if existing.incarnation == incarnation && existing.address == address {
                return Ok(());
            }
            return Err(Error::ClusterConfig(format!(
                "node {node} is already registered as incarnation {} at {}; replace it explicitly \
                 rather than reusing its identity",
                existing.incarnation, existing.address
            )));
        }
        self.members.insert(
            node,
            Member {
                node,
                incarnation,
                address,
                role: MemberRole::Learner,
                state: MemberState::Active,
                capacity_weight: 1,
                failure_domain: None,
            },
        );
        Ok(())
    }

    pub fn promote(&mut self, node: NodeId, role: MemberRole) -> Result<()> {
        let member = self
            .members
            .get_mut(&node)
            .ok_or_else(|| Error::ClusterConfig(format!("node {node} is not a member")))?;
        if role == MemberRole::Learner {
            return Err(Error::ClusterConfig(
                "promotion target must be storage or voter".into(),
            ));
        }
        member.role = role;
        Ok(())
    }

    pub fn set_member_state(&mut self, node: NodeId, state: MemberState) -> Result<()> {
        let member = self
            .members
            .get_mut(&node)
            .ok_or_else(|| Error::ClusterConfig(format!("node {node} is not a member")))?;
        member.state = state;
        Ok(())
    }

    pub fn set_member_policy(
        &mut self,
        node: NodeId,
        capacity_weight: u32,
        failure_domain: Option<String>,
    ) -> Result<()> {
        if capacity_weight == 0 {
            return Err(Error::ClusterConfig(
                "member capacity_weight must be at least one".into(),
            ));
        }
        let failure_domain = failure_domain
            .map(|domain| domain.trim().to_string())
            .filter(|domain| !domain.is_empty());
        let member = self
            .members
            .get_mut(&node)
            .ok_or_else(|| Error::ClusterConfig(format!("node {node} is not a member")))?;
        member.capacity_weight = capacity_weight;
        member.failure_domain = failure_domain;
        Ok(())
    }

    pub fn set_scaling_policy(&mut self, policy: ScalingPolicy) -> Result<()> {
        policy.validate()?;
        self.scaling_policy = policy;
        Ok(())
    }

    pub fn voter_sets(
        &self,
    ) -> (
        std::collections::BTreeSet<NodeId>,
        Option<std::collections::BTreeSet<NodeId>>,
    ) {
        match &self.voter_transition {
            Some(transition) => (transition.old.clone(), Some(transition.new.clone())),
            None => (
                self.members
                    .values()
                    .filter(|member| member.role == MemberRole::Voter)
                    .map(|member| member.node)
                    .collect(),
                None,
            ),
        }
    }

    pub fn is_voter(&self, node: NodeId) -> bool {
        let (current, next) = self.voter_sets();
        current.contains(&node) || next.as_ref().is_some_and(|set| set.contains(&node))
    }

    pub fn begin_voter_change(&mut self, new: std::collections::BTreeSet<NodeId>) -> Result<()> {
        if self.voter_transition.is_some() {
            return Err(Error::ClusterConfig(
                "a voter change is already in joint consensus; finalize it first".into(),
            ));
        }
        if new.is_empty() {
            return Err(Error::ClusterConfig(
                "a voter configuration cannot be empty".into(),
            ));
        }
        for node in &new {
            let member = self.members.get(node).ok_or_else(|| {
                Error::ClusterConfig(format!("new voter node {node} is not a member"))
            })?;
            if member.role == MemberRole::Learner {
                return Err(Error::ClusterConfig(format!(
                    "learner node {node} must complete its storage join before becoming a voter"
                )));
            }
        }
        let (old, _) = self.voter_sets();
        if old == new {
            return Err(Error::ClusterConfig(
                "new voter set is identical to the current set".into(),
            ));
        }
        self.voter_transition = Some(VoterTransition { old, new });
        Ok(())
    }

    pub fn finalize_voter_change(&mut self) -> Result<()> {
        let transition = self.voter_transition.take().ok_or_else(|| {
            Error::ClusterConfig("there is no joint voter change to finalize".into())
        })?;
        for member in self.members.values_mut() {
            if transition.new.contains(&member.node) {
                member.role = MemberRole::Voter;
            } else if member.role == MemberRole::Voter {
                member.role = MemberRole::Storage;
            }
        }
        Ok(())
    }

    /// Create the first committed placement map. This is intentionally separate from
    /// [`Self::bootstrap`]: nodes may join as learners before any shard is assigned.
    ///
    /// `copies` includes the primary. A value larger than the eligible member count is refused
    /// instead of silently weakening the requested durability.
    pub fn initialize_placements(&mut self, copies: usize) -> Result<()> {
        if !self.placements.is_empty() {
            return Err(Error::ClusterConfig(
                "initial placement already exists; use transfer operations to change it".into(),
            ));
        }
        if copies == 0 {
            return Err(Error::ClusterConfig(
                "placement requires at least one copy".into(),
            ));
        }
        let eligible: Vec<NodeId> = self
            .members
            .values()
            .filter(|member| member.may_receive_placement())
            .map(|member| member.node)
            .collect();
        if eligible.len() < copies {
            return Err(Error::ClusterConfig(format!(
                "placement requests {copies} copies but only {} active storage/voter members are \
                 eligible",
                eligible.len()
            )));
        }

        let mut primary_counts: BTreeMap<NodeId, u64> =
            eligible.iter().copied().map(|node| (node, 0)).collect();
        for shard_number in 0..self.routing.shard_count() {
            let primary = eligible
                .iter()
                .copied()
                .min_by(|left, right| {
                    normalized_load_cmp(
                        primary_counts[left],
                        self.members[left].capacity_weight,
                        primary_counts[right],
                        self.members[right].capacity_weight,
                    )
                    .then_with(|| left.cmp(right))
                })
                .expect("eligible is non-empty");
            *primary_counts.get_mut(&primary).expect("eligible count") += 1;
            let mut replica_candidates: Vec<NodeId> = eligible
                .iter()
                .copied()
                .filter(|node| *node != primary)
                .collect();
            replica_candidates.sort_by_key(|node| {
                let same_domain = same_failure_domain(
                    self.members[&primary].failure_domain.as_deref(),
                    self.members[node].failure_domain.as_deref(),
                );
                (same_domain, *node)
            });
            let replicas = replica_candidates.into_iter().take(copies - 1).collect();
            self.placements.insert(
                ShardId(shard_number),
                ReplicaSet {
                    generation: 1,
                    primary,
                    replicas,
                },
            );
        }
        Ok(())
    }

    pub fn remove_member(&mut self, node: NodeId) -> Result<()> {
        let member = self
            .members
            .get(&node)
            .ok_or_else(|| Error::ClusterConfig(format!("node {node} is not a member")))?;
        if self.is_voter(node) {
            return Err(Error::ClusterConfig(format!(
                "node {node} is in the active voter configuration; complete a joint voter change \
                 before removal"
            )));
        }
        let still_used = self
            .placements
            .iter()
            .find(|(_, placement)| placement.primary == node || placement.replicas.contains(&node));
        if let Some((shard, _)) = still_used {
            return Err(Error::ClusterConfig(format!(
                "node {node} still holds a required copy of {shard}; drain it before removal"
            )));
        }
        if member.role == MemberRole::Voter
            && self
                .members
                .values()
                .filter(|member| member.role == MemberRole::Voter)
                .count()
                == 1
        {
            return Err(Error::ClusterConfig(
                "removal would leave the catalog with no voters".into(),
            ));
        }
        self.members.remove(&node);
        Ok(())
    }

    pub fn start_operation(
        &mut self,
        kind: OperationKind,
        shard: Option<ShardId>,
        source: Option<NodeId>,
        destination: Option<NodeId>,
        expected_generation: Option<u64>,
    ) -> Result<u64> {
        if let Some(shard) = shard
            && self
                .operations
                .values()
                .any(|operation| operation.shard == Some(shard) && !operation.phase.terminal())
        {
            return Err(Error::ClusterConfig(format!(
                "{shard} already has an active topology operation"
            )));
        }
        let id = self.next_operation_id;
        self.next_operation_id = self
            .next_operation_id
            .checked_add(1)
            .ok_or_else(|| Error::ClusterConfig("operation id space exhausted".into()))?;
        self.operations.insert(
            id,
            CatalogOperation {
                id,
                kind,
                phase: OperationPhase::Planned,
                shard,
                source,
                destination,
                expected_generation,
                stream_epoch: None,
                snapshot_lsn: None,
                durable_lsn: None,
                final_lsn: None,
                catalog_version: None,
                routing_before: None,
                routing_after: None,
                retain_source_replica: false,
                split_required_nodes: BTreeSet::new(),
                split_installed_nodes: BTreeSet::new(),
                split_cleaned_nodes: BTreeSet::new(),
                created_version: self.version,
                last_error: None,
            },
        );
        Ok(id)
    }

    /// Validate and register a previously unknown node as a write-ineligible learner.
    pub fn start_join(
        &mut self,
        local_cluster: Option<ClusterId>,
        compatibility: &Compatibility,
        node: NodeId,
        incarnation: u64,
        address: String,
    ) -> Result<u64> {
        self.validate_join(local_cluster, compatibility)?;
        if self.operations.values().any(|operation| {
            operation.kind == OperationKind::Join
                && operation.destination == Some(node)
                && !operation.phase.terminal()
        }) {
            return Err(Error::ClusterConfig(format!(
                "node {node} already has an active join operation"
            )));
        }
        self.add_learner(node, incarnation, address)?;
        self.start_operation(OperationKind::Join, None, None, Some(node), None)
    }

    /// Record that a learner has durably installed catalog state through `version`.
    pub fn record_join_catch_up(&mut self, operation: u64, version: u64) -> Result<()> {
        let phase = self
            .operations
            .get(&operation)
            .ok_or_else(|| Error::ClusterConfig(format!("operation {operation} does not exist")))?
            .phase;
        if phase == OperationPhase::Planned {
            self.transition(
                operation,
                OperationKind::Join,
                OperationPhase::Planned,
                OperationPhase::CatchingUp,
            )?;
        }
        let op = self.operation_in(operation, OperationKind::Join, OperationPhase::CatchingUp)?;
        let previous = op.catalog_version.unwrap_or_default();
        if version < previous {
            return Err(Error::ClusterConfig(format!(
                "join operation {operation} catalog version regressed from {previous} to {version}"
            )));
        }
        op.catalog_version = Some(version);
        Ok(())
    }

    pub fn prepare_join(&mut self, operation: u64) -> Result<()> {
        let op = self.operation_in(operation, OperationKind::Join, OperationPhase::CatchingUp)?;
        let required = op.created_version.saturating_add(1);
        if op.catalog_version.unwrap_or_default() < required {
            return Err(Error::ClusterConfig(format!(
                "join operation {operation} learner has catalog version {}, short of required \
                 version {required}",
                op.catalog_version.unwrap_or_default()
            )));
        }
        op.phase = OperationPhase::Prepared;
        Ok(())
    }

    /// Make a caught-up learner eligible for storage placement. Voter promotion is deliberately
    /// separate and requires joint consensus.
    pub fn commit_storage_join(&mut self, operation: u64) -> Result<()> {
        let destination = {
            let op = self.operation_in(operation, OperationKind::Join, OperationPhase::Prepared)?;
            op.destination.expect("validated join destination")
        };
        self.promote(destination, MemberRole::Storage)?;
        self.operations
            .get_mut(&operation)
            .expect("join operation checked above")
            .phase = OperationPhase::Committed;
        Ok(())
    }

    pub fn complete_join(&mut self, operation: u64) -> Result<()> {
        self.transition(
            operation,
            OperationKind::Join,
            OperationPhase::Committed,
            OperationPhase::Complete,
        )
    }

    /// Persist drain intent. The planner will evacuate one primary at a time; this long-lived
    /// operation does not itself consume the per-shard transfer budget.
    pub fn start_drain(&mut self, node: NodeId) -> Result<u64> {
        if self.operations.values().any(|operation| {
            operation.kind == OperationKind::Drain
                && operation.source == Some(node)
                && !operation.phase.terminal()
        }) {
            return Err(Error::ClusterConfig(format!(
                "node {node} already has an active drain operation"
            )));
        }
        self.set_member_state(node, MemberState::Draining)?;
        self.start_operation(OperationKind::Drain, None, Some(node), None, None)
    }

    pub fn complete_drain(&mut self, operation: u64) -> Result<()> {
        let node = {
            let op = self.operation_in(operation, OperationKind::Drain, OperationPhase::Planned)?;
            op.source.expect("validated drain source")
        };
        if let Some((&shard, _)) = self
            .placements
            .iter()
            .find(|(_, placement)| placement.primary == node || placement.replicas.contains(&node))
        {
            return Err(Error::ClusterConfig(format!(
                "drain operation {operation} cannot complete: node {node} still holds {shard}"
            )));
        }
        self.operations
            .get_mut(&operation)
            .expect("drain operation checked above")
            .phase = OperationPhase::Complete;
        Ok(())
    }

    /// Plan one primary transfer from the committed placement map.
    pub fn start_transfer(&mut self, shard: ShardId, destination: NodeId) -> Result<u64> {
        let (source, generation, destination_was_replica) = {
            let placement = self.placements.get(&shard).ok_or_else(|| {
                Error::ClusterConfig(format!("{shard} has no committed placement to transfer"))
            })?;
            (
                placement.primary,
                placement.generation,
                placement.replicas.contains(&destination),
            )
        };
        let member = self.members.get(&destination).ok_or_else(|| {
            Error::ClusterConfig(format!("destination node {destination} is not a member"))
        })?;
        if !member.may_receive_placement() {
            return Err(Error::ClusterConfig(format!(
                "destination node {destination} is {:?}/{:?} and may not receive a primary",
                member.role, member.state
            )));
        }
        if source == destination {
            return Err(Error::ClusterConfig(format!(
                "destination node {destination} already owns {shard}"
            )));
        }
        let id = self.start_operation(
            OperationKind::Transfer,
            Some(shard),
            Some(source),
            Some(destination),
            Some(generation),
        )?;
        self.operations
            .get_mut(&id)
            .expect("new transfer operation")
            .retain_source_replica = destination_was_replica;
        Ok(id)
    }

    /// Plan the next deterministic linear-hash split.
    ///
    /// Every current copy is recorded as a required split participant. Routing cannot publish
    /// until all of them durably acknowledge both shadow images.
    pub fn start_split(&mut self) -> Result<u64> {
        let Routing::LinearV1(current) = self.routing else {
            return Err(Error::ClusterConfig(
                "online shard growth requires linear-v1 routing".into(),
            ));
        };
        if current.shard_count() >= crate::shard::ShardConfig::MAX_SHARDS {
            return Err(Error::ClusterConfig(format!(
                "online shard growth reached the current {}-shard implementation limit",
                crate::shard::ShardConfig::MAX_SHARDS
            )));
        }
        if self
            .operations
            .values()
            .any(|operation| operation.kind == OperationKind::Split && !operation.phase.terminal())
        {
            return Err(Error::ClusterConfig(
                "a logical split is already active; only one split may run cluster-wide".into(),
            ));
        }
        let source = current.split_source();
        let destination = current.split_destination();
        let placement = self.placements.get(&source).ok_or_else(|| {
            Error::ClusterConfig(format!("{source} has no committed placement to split"))
        })?;
        let mut required_nodes: BTreeSet<NodeId> = placement.replicas.iter().copied().collect();
        required_nodes.insert(placement.primary);
        if self.placements.contains_key(&destination) {
            return Err(Error::ClusterConfig(format!(
                "split destination {destination} already has a placement"
            )));
        }
        let next = Routing::LinearV1(current.grown()?);
        let id = self.start_operation(
            OperationKind::Split,
            Some(source),
            Some(placement.primary),
            Some(placement.primary),
            Some(placement.generation),
        )?;
        let operation = self.operations.get_mut(&id).expect("new split operation");
        operation.routing_before = Some(self.routing);
        operation.routing_after = Some(next);
        operation.split_required_nodes = required_nodes;
        Ok(id)
    }

    pub fn begin_split_snapshot(&mut self, operation: u64) -> Result<()> {
        self.transition(
            operation,
            OperationKind::Split,
            OperationPhase::Planned,
            OperationPhase::Snapshotting,
        )
    }

    pub fn split_snapshot_ready(&mut self, operation: u64, barrier: u64) -> Result<()> {
        let op = self.operation_in(
            operation,
            OperationKind::Split,
            OperationPhase::Snapshotting,
        )?;
        op.snapshot_lsn = Some(barrier);
        op.durable_lsn = Some(barrier);
        op.phase = OperationPhase::CatchingUp;
        Ok(())
    }

    pub fn record_split_replay(&mut self, operation: u64, sequence: u64) -> Result<()> {
        let op = self.operation_in(operation, OperationKind::Split, OperationPhase::CatchingUp)?;
        if sequence < op.durable_lsn.unwrap_or_default() {
            return Err(Error::ClusterConfig(format!(
                "split operation {operation} replay sequence regressed from {} to {sequence}",
                op.durable_lsn.unwrap_or_default()
            )));
        }
        op.durable_lsn = Some(sequence);
        Ok(())
    }

    pub fn prepare_split(&mut self, operation: u64) -> Result<()> {
        self.transition(
            operation,
            OperationKind::Split,
            OperationPhase::CatchingUp,
            OperationPhase::Prepared,
        )
    }

    pub fn begin_split_fencing(&mut self, operation: u64) -> Result<()> {
        self.transition(
            operation,
            OperationKind::Split,
            OperationPhase::Prepared,
            OperationPhase::Fencing,
        )
    }

    pub fn split_fenced(&mut self, operation: u64, sequence: u64) -> Result<()> {
        let op = self.operation_in(operation, OperationKind::Split, OperationPhase::Fencing)?;
        if sequence < op.durable_lsn.unwrap_or_default() {
            return Err(Error::ClusterConfig(format!(
                "split operation {operation} final sequence {sequence} is behind replayed {}",
                op.durable_lsn.unwrap_or_default()
            )));
        }
        op.final_lsn = Some(sequence);
        op.durable_lsn = Some(sequence);
        op.phase = OperationPhase::Installing;
        Ok(())
    }

    pub fn record_split_installed(&mut self, operation: u64, node: NodeId) -> Result<()> {
        let op = self.operation_in(operation, OperationKind::Split, OperationPhase::Installing)?;
        if !op.split_required_nodes.contains(&node) {
            return Err(Error::ClusterConfig(format!(
                "node {node} is not a required participant in split operation {operation}"
            )));
        }
        op.split_installed_nodes.insert(node);
        Ok(())
    }

    /// Publish an already-installed pair of split images and its new routing epoch atomically in
    /// one catalog value.
    pub fn commit_split(&mut self, operation: u64) -> Result<()> {
        let (
            source,
            owner,
            generation,
            before,
            after,
            final_sequence,
            durable_sequence,
            required_nodes,
            installed_nodes,
        ) = {
            let op =
                self.operation_in(operation, OperationKind::Split, OperationPhase::Installing)?;
            (
                op.shard.expect("validated split source"),
                op.source.expect("validated split owner"),
                op.expected_generation.expect("validated split generation"),
                op.routing_before.expect("validated old split routing"),
                op.routing_after.expect("validated new split routing"),
                op.final_lsn,
                op.durable_lsn,
                op.split_required_nodes.clone(),
                op.split_installed_nodes.clone(),
            )
        };
        if final_sequence.is_none() || durable_sequence < final_sequence {
            return Err(Error::ClusterConfig(format!(
                "split operation {operation} has not durably replayed its final logical sequence"
            )));
        }
        if !required_nodes.is_subset(&installed_nodes) {
            let missing: Vec<_> = required_nodes
                .difference(&installed_nodes)
                .copied()
                .collect();
            return Err(Error::ClusterConfig(format!(
                "split operation {operation} cannot commit; nodes {missing:?} have not installed \
                 both split images"
            )));
        }
        if self.routing != before {
            return Err(Error::ClusterConfig(format!(
                "split operation {operation} expected routing {before:?}, catalog has {:?}",
                self.routing
            )));
        }
        let placement = self.placements.get(&source).ok_or_else(|| {
            Error::ClusterConfig(format!(
                "split operation {operation} lost {source} placement"
            ))
        })?;
        if placement.primary != owner || placement.generation != generation {
            return Err(Error::ClusterConfig(format!(
                "split operation {operation} expected {source} generation {generation} on node \
                 {owner}, catalog has generation {} on node {}",
                placement.generation, placement.primary
            )));
        }
        let destination = match before {
            Routing::LinearV1(linear) => linear.split_destination(),
            Routing::ModuloV1(_) => unreachable!("split routing validated as linear"),
        };
        self.placements.insert(
            destination,
            ReplicaSet {
                generation: 1,
                primary: owner,
                replicas: required_nodes
                    .iter()
                    .copied()
                    .filter(|node| *node != owner)
                    .collect(),
            },
        );
        self.routing = after;
        self.routing_epoch = self
            .routing_epoch
            .checked_add(1)
            .ok_or_else(|| Error::ClusterConfig("routing epoch exhausted".into()))?;
        self.operations
            .get_mut(&operation)
            .expect("operation checked above")
            .phase = OperationPhase::Committed;
        Ok(())
    }

    pub fn begin_split_cleanup(&mut self, operation: u64) -> Result<()> {
        self.transition(
            operation,
            OperationKind::Split,
            OperationPhase::Committed,
            OperationPhase::Cleaning,
        )
    }

    pub fn record_split_cleaned(&mut self, operation: u64, node: NodeId) -> Result<()> {
        let op = self.operation_in(operation, OperationKind::Split, OperationPhase::Cleaning)?;
        if !op.split_required_nodes.contains(&node) {
            return Err(Error::ClusterConfig(format!(
                "node {node} is not a required participant in split operation {operation}"
            )));
        }
        op.split_cleaned_nodes.insert(node);
        Ok(())
    }

    pub fn complete_split(&mut self, operation: u64) -> Result<()> {
        let op = self.operation_in(operation, OperationKind::Split, OperationPhase::Cleaning)?;
        if !op.split_required_nodes.is_subset(&op.split_cleaned_nodes) {
            let missing: Vec<_> = op
                .split_required_nodes
                .difference(&op.split_cleaned_nodes)
                .copied()
                .collect();
            return Err(Error::ClusterConfig(format!(
                "split operation {operation} cannot complete; nodes {missing:?} have not cleaned \
                 their split staging state"
            )));
        }
        self.transition(
            operation,
            OperationKind::Split,
            OperationPhase::Cleaning,
            OperationPhase::Complete,
        )
    }

    pub fn begin_snapshot(&mut self, operation: u64) -> Result<()> {
        self.transition(
            operation,
            OperationKind::Transfer,
            OperationPhase::Planned,
            OperationPhase::Snapshotting,
        )
    }

    /// Record the exact snapshot identity and move the transfer into catch-up.
    pub fn snapshot_installed(&mut self, operation: u64, epoch: u64, lsn: u64) -> Result<()> {
        if epoch == 0 {
            return Err(Error::ClusterConfig(format!(
                "operation {operation} destination reported an uninitialised stream epoch; it \
                 must install a source snapshot before catch-up"
            )));
        }
        let op = self.operation_in(
            operation,
            OperationKind::Transfer,
            OperationPhase::Snapshotting,
        )?;
        op.stream_epoch = Some(epoch);
        op.snapshot_lsn = Some(lsn);
        op.durable_lsn = Some(lsn);
        op.phase = OperationPhase::CatchingUp;
        Ok(())
    }

    /// Advance the destination's *durable* receive position. Regressions and stream changes are
    /// refused so an in-memory receive cursor can never masquerade as a safe cutover point.
    pub fn record_catch_up(&mut self, operation: u64, epoch: u64, durable_lsn: u64) -> Result<()> {
        let op = self.operation_in(
            operation,
            OperationKind::Transfer,
            OperationPhase::CatchingUp,
        )?;
        if op.stream_epoch != Some(epoch) {
            return Err(Error::ClusterConfig(format!(
                "operation {operation} snapshot is from epoch {:?}, catch-up reported epoch \
                 {epoch}",
                op.stream_epoch
            )));
        }
        let previous = op.durable_lsn.unwrap_or_default();
        if durable_lsn < previous {
            return Err(Error::ClusterConfig(format!(
                "operation {operation} durable LSN regressed from {previous} to {durable_lsn}"
            )));
        }
        op.durable_lsn = Some(durable_lsn);
        Ok(())
    }

    pub fn prepare_transfer(&mut self, operation: u64) -> Result<()> {
        self.transition(
            operation,
            OperationKind::Transfer,
            OperationPhase::CatchingUp,
            OperationPhase::Prepared,
        )
    }

    pub fn begin_fencing(&mut self, operation: u64) -> Result<()> {
        self.transition(
            operation,
            OperationKind::Transfer,
            OperationPhase::Prepared,
            OperationPhase::Fencing,
        )
    }

    /// Record the final source position after its write gate has closed. The destination must
    /// subsequently report this LSN durable before ownership can commit.
    pub fn record_final_lsn(&mut self, operation: u64, epoch: u64, lsn: u64) -> Result<()> {
        let op = self.operation_in(operation, OperationKind::Transfer, OperationPhase::Fencing)?;
        if op.stream_epoch != Some(epoch) {
            return Err(Error::ClusterConfig(format!(
                "operation {operation} cannot fence epoch {epoch}; its snapshot is epoch {:?}",
                op.stream_epoch
            )));
        }
        if lsn < op.durable_lsn.unwrap_or_default() {
            return Err(Error::ClusterConfig(format!(
                "operation {operation} final LSN {lsn} is behind destination durable LSN {}",
                op.durable_lsn.unwrap_or_default()
            )));
        }
        op.final_lsn = Some(lsn);
        Ok(())
    }

    /// Roll a fenced transfer forward after the source process restarts into a new stream epoch.
    ///
    /// The source is fenced again before this command. Existing destination evidence belongs to
    /// the old epoch and is deliberately discarded; ownership remains unchanged while the
    /// destination takes a fresh snapshot under the closed gate.
    pub fn restart_fenced_stream(
        &mut self,
        operation: u64,
        epoch: u64,
        final_lsn: u64,
    ) -> Result<()> {
        if epoch == 0 {
            return Err(Error::ClusterConfig(
                "a fenced transfer cannot restart into stream epoch zero".into(),
            ));
        }
        let op = self.operation_in(operation, OperationKind::Transfer, OperationPhase::Fencing)?;
        if op.stream_epoch == Some(epoch) {
            if op.final_lsn == Some(final_lsn) && op.snapshot_lsn.is_none() {
                return Ok(());
            }
            return Err(Error::ClusterConfig(format!(
                "operation {operation} already uses stream epoch {epoch} with final LSN {:?}",
                op.final_lsn
            )));
        }
        op.stream_epoch = Some(epoch);
        op.snapshot_lsn = None;
        op.durable_lsn = None;
        op.final_lsn = Some(final_lsn);
        Ok(())
    }

    /// Record a replacement snapshot installed while the restarted source remains fenced.
    pub fn record_fenced_bootstrap(
        &mut self,
        operation: u64,
        epoch: u64,
        durable_lsn: u64,
    ) -> Result<()> {
        let op = self.operation_in(operation, OperationKind::Transfer, OperationPhase::Fencing)?;
        if op.stream_epoch != Some(epoch) {
            return Err(Error::ClusterConfig(format!(
                "operation {operation} restarted in epoch {:?}, destination bootstrapped epoch \
                 {epoch}",
                op.stream_epoch
            )));
        }
        if durable_lsn < op.final_lsn.unwrap_or_default() {
            return Err(Error::ClusterConfig(format!(
                "operation {operation} replacement snapshot LSN {durable_lsn} is behind fenced \
                 final LSN {}",
                op.final_lsn.unwrap_or_default()
            )));
        }
        op.snapshot_lsn = Some(durable_lsn);
        op.durable_lsn = Some(durable_lsn);
        Ok(())
    }

    /// Record catch-up while the source is fenced. This is deliberately distinct from the normal
    /// catch-up phase so duplicate or delayed phase commands remain harmless.
    pub fn record_fenced_catch_up(
        &mut self,
        operation: u64,
        epoch: u64,
        durable_lsn: u64,
    ) -> Result<()> {
        let op = self.operation_in(operation, OperationKind::Transfer, OperationPhase::Fencing)?;
        if op.stream_epoch != Some(epoch) {
            return Err(Error::ClusterConfig(format!(
                "operation {operation} expected epoch {:?}, got {epoch}",
                op.stream_epoch
            )));
        }
        let previous = op.durable_lsn.unwrap_or_default();
        if durable_lsn < previous {
            return Err(Error::ClusterConfig(format!(
                "operation {operation} durable LSN regressed from {previous} to {durable_lsn}"
            )));
        }
        op.durable_lsn = Some(durable_lsn);
        Ok(())
    }

    /// Atomically increment the ownership generation and publish the destination primary.
    ///
    /// The source remains a replica until cleanup, preserving a recoverable copy through the
    /// commit boundary.
    pub fn commit_transfer(&mut self, operation: u64) -> Result<()> {
        let (shard, source, destination, expected_generation, snapshot_lsn, durable_lsn, final_lsn) = {
            let op =
                self.operation_in(operation, OperationKind::Transfer, OperationPhase::Fencing)?;
            (
                op.shard.expect("validated transfer shard"),
                op.source.expect("validated transfer source"),
                op.destination.expect("validated transfer destination"),
                op.expected_generation
                    .expect("validated transfer generation"),
                op.snapshot_lsn,
                op.durable_lsn,
                op.final_lsn,
            )
        };
        if snapshot_lsn.is_none() {
            return Err(Error::ClusterConfig(format!(
                "operation {operation} has no destination snapshot for its current stream epoch"
            )));
        }
        let final_lsn = final_lsn.ok_or_else(|| {
            Error::ClusterConfig(format!(
                "operation {operation} has no final fenced LSN; ownership cannot commit"
            ))
        })?;
        if durable_lsn.unwrap_or_default() < final_lsn {
            return Err(Error::ClusterConfig(format!(
                "operation {operation} destination is durable through LSN {}, short of final LSN \
                 {final_lsn}",
                durable_lsn.unwrap_or_default()
            )));
        }

        let placement = self
            .placements
            .get_mut(&shard)
            .ok_or_else(|| Error::ClusterConfig(format!("{shard} lost its committed placement")))?;
        if placement.generation != expected_generation || placement.primary != source {
            return Err(Error::ClusterConfig(format!(
                "operation {operation} expected {shard} generation {expected_generation} owned by \
                 node {source}, but catalog has generation {} owned by node {}; refusing stale \
                 cutover",
                placement.generation, placement.primary
            )));
        }
        placement.generation = placement.generation.checked_add(1).ok_or_else(|| {
            Error::ClusterConfig(format!("{shard} ownership generation exhausted"))
        })?;
        placement.primary = destination;
        placement.replicas.retain(|node| *node != destination);
        if !placement.replicas.contains(&source) {
            placement.replicas.push(source);
            placement.replicas.sort_unstable();
        }
        self.operations
            .get_mut(&operation)
            .expect("operation checked above")
            .phase = OperationPhase::Committed;
        Ok(())
    }

    pub fn begin_cleanup(&mut self, operation: u64) -> Result<()> {
        self.transition(
            operation,
            OperationKind::Transfer,
            OperationPhase::Committed,
            OperationPhase::Cleaning,
        )
    }

    pub fn complete_transfer(&mut self, operation: u64) -> Result<()> {
        let (shard, source, retain_source_replica) = {
            let op =
                self.operation_in(operation, OperationKind::Transfer, OperationPhase::Cleaning)?;
            (op.shard, op.source, op.retain_source_replica)
        };
        if !retain_source_replica {
            let placement = self
                .placements
                .get_mut(&shard.expect("validated transfer shard"))
                .ok_or_else(|| {
                    Error::ClusterConfig(format!(
                        "operation {operation} lost its placement during cleanup"
                    ))
                })?;
            placement.replicas.retain(|node| Some(*node) != source);
        }
        self.operations
            .get_mut(&operation)
            .expect("operation checked above")
            .phase = OperationPhase::Complete;
        Ok(())
    }

    pub fn abort_operation(&mut self, operation: u64, reason: String) -> Result<()> {
        let op = self
            .operations
            .get_mut(&operation)
            .ok_or_else(|| Error::ClusterConfig(format!("operation {operation} does not exist")))?;
        if !op.phase.may_abort() {
            return Err(Error::ClusterConfig(format!(
                "operation {operation} is {:?}; the ownership boundary may have been crossed, so \
                 recovery must roll forward",
                op.phase
            )));
        }
        op.phase = OperationPhase::Aborted;
        op.last_error = Some(reason);
        Ok(())
    }

    fn transition(
        &mut self,
        operation: u64,
        kind: OperationKind,
        expected: OperationPhase,
        next: OperationPhase,
    ) -> Result<()> {
        let op = self.operation_in(operation, kind, expected)?;
        op.phase = next;
        Ok(())
    }

    fn operation_in(
        &mut self,
        operation: u64,
        kind: OperationKind,
        phase: OperationPhase,
    ) -> Result<&mut CatalogOperation> {
        let op = self
            .operations
            .get_mut(&operation)
            .ok_or_else(|| Error::ClusterConfig(format!("operation {operation} does not exist")))?;
        if op.kind != kind || op.phase != phase {
            return Err(Error::ClusterConfig(format!(
                "operation {operation} is {:?}/{:?}, expected {kind:?}/{phase:?}; refusing stale \
                 or out-of-order command",
                op.kind, op.phase
            )));
        }
        Ok(op)
    }

    pub fn validate(&self) -> Result<()> {
        if self.format_version != CATALOG_FORMAT_VERSION {
            return Err(Error::ClusterConfig(format!(
                "catalog format {} is unsupported by format {}",
                self.format_version, CATALOG_FORMAT_VERSION
            )));
        }
        if self.routing.shard_count() == 0 {
            return Err(Error::ClusterConfig(
                "catalog routing has no active shards".into(),
            ));
        }
        self.scaling_policy.validate()?;
        for member in self.members.values() {
            if member.capacity_weight == 0 {
                return Err(Error::ClusterConfig(format!(
                    "member {} has zero placement capacity weight",
                    member.node
                )));
            }
            if member
                .failure_domain
                .as_ref()
                .is_some_and(|domain| domain.trim().is_empty())
            {
                return Err(Error::ClusterConfig(format!(
                    "member {} has an empty failure domain",
                    member.node
                )));
            }
        }
        if !self
            .members
            .values()
            .any(|member| member.role == MemberRole::Voter)
        {
            return Err(Error::ClusterConfig(
                "catalog must contain at least one voter".into(),
            ));
        }
        if let Some(transition) = &self.voter_transition {
            if transition.old.is_empty() || transition.new.is_empty() {
                return Err(Error::ClusterConfig(
                    "joint voter configurations cannot be empty".into(),
                ));
            }
            for node in transition.old.union(&transition.new) {
                let member = self.members.get(node).ok_or_else(|| {
                    Error::ClusterConfig(format!(
                        "joint voter configuration names missing node {node}"
                    ))
                })?;
                if member.role == MemberRole::Learner {
                    return Err(Error::ClusterConfig(format!(
                        "joint voter configuration names learner node {node}"
                    )));
                }
            }
        }
        for (&shard, placement) in &self.placements {
            if shard.0 >= self.routing.shard_count() {
                return Err(Error::ClusterConfig(format!(
                    "placement contains inactive {shard}; routing has {} shards",
                    self.routing.shard_count()
                )));
            }
            let primary = self.members.get(&placement.primary).ok_or_else(|| {
                Error::ClusterConfig(format!(
                    "{shard} names missing primary node {}",
                    placement.primary
                ))
            })?;
            if primary.role == MemberRole::Learner {
                return Err(Error::ClusterConfig(format!(
                    "{shard} names learner node {} as primary",
                    placement.primary
                )));
            }
            for replica in &placement.replicas {
                if *replica == placement.primary {
                    return Err(Error::ClusterConfig(format!(
                        "{shard} lists primary {} as its own replica",
                        placement.primary
                    )));
                }
                if !self.members.contains_key(replica) {
                    return Err(Error::ClusterConfig(format!(
                        "{shard} names missing replica node {replica}"
                    )));
                }
            }
        }
        let mut active_shards = std::collections::BTreeSet::new();
        for (&operation_id, operation) in &self.operations {
            if operation.id == 0 || operation.id != operation_id {
                return Err(Error::ClusterConfig(
                    "catalog operation id is zero or does not match its map key".into(),
                ));
            }
            if let Some(shard) = operation.shard
                && !operation.phase.terminal()
                && !active_shards.insert(shard)
            {
                return Err(Error::ClusterConfig(format!(
                    "{shard} has more than one active topology operation"
                )));
            }
            if operation.kind == OperationKind::Transfer {
                let shard = operation.shard.ok_or_else(|| {
                    Error::ClusterConfig(format!(
                        "transfer operation {} has no shard",
                        operation.id
                    ))
                })?;
                let source = operation.source.ok_or_else(|| {
                    Error::ClusterConfig(format!(
                        "transfer operation {} has no source",
                        operation.id
                    ))
                })?;
                let destination = operation.destination.ok_or_else(|| {
                    Error::ClusterConfig(format!(
                        "transfer operation {} has no destination",
                        operation.id
                    ))
                })?;
                if source == destination {
                    return Err(Error::ClusterConfig(format!(
                        "transfer operation {} has the same source and destination",
                        operation.id
                    )));
                }
                if shard.0 >= self.routing.shard_count()
                    || !self.members.contains_key(&source)
                    || !self.members.contains_key(&destination)
                    || operation.expected_generation.is_none()
                {
                    return Err(Error::ClusterConfig(format!(
                        "transfer operation {} references invalid topology state",
                        operation.id
                    )));
                }
            }
            if operation.kind == OperationKind::Split {
                let source = operation.shard.ok_or_else(|| {
                    Error::ClusterConfig(format!(
                        "split operation {} has no source shard",
                        operation.id
                    ))
                })?;
                let owner = operation.source.ok_or_else(|| {
                    Error::ClusterConfig(format!(
                        "split operation {} has no source owner",
                        operation.id
                    ))
                })?;
                let (Some(Routing::LinearV1(before)), Some(Routing::LinearV1(after))) =
                    (operation.routing_before, operation.routing_after)
                else {
                    return Err(Error::ClusterConfig(format!(
                        "split operation {} does not carry linear before/after routing",
                        operation.id
                    )));
                };
                if before.split_source() != source
                    || before.grown()? != after
                    || operation.destination != Some(owner)
                    || operation.expected_generation.is_none()
                    || !self.members.contains_key(&owner)
                    || !operation.split_required_nodes.contains(&owner)
                    || operation.split_required_nodes.is_empty()
                    || !operation
                        .split_installed_nodes
                        .is_subset(&operation.split_required_nodes)
                    || !operation
                        .split_cleaned_nodes
                        .is_subset(&operation.split_required_nodes)
                {
                    return Err(Error::ClusterConfig(format!(
                        "split operation {} references invalid topology state",
                        operation.id
                    )));
                }
                for node in &operation.split_required_nodes {
                    if !self.members.contains_key(node) {
                        return Err(Error::ClusterConfig(format!(
                            "split operation {} requires missing node {node}",
                            operation.id
                        )));
                    }
                }
                let committed = matches!(
                    operation.phase,
                    OperationPhase::Committed | OperationPhase::Cleaning | OperationPhase::Complete
                );
                if committed && self.routing != Routing::LinearV1(after) {
                    return Err(Error::ClusterConfig(format!(
                        "committed split operation {} does not match catalog routing",
                        operation.id
                    )));
                }
                if committed
                    && !operation
                        .split_required_nodes
                        .is_subset(&operation.split_installed_nodes)
                {
                    return Err(Error::ClusterConfig(format!(
                        "committed split operation {} is missing install acknowledgements",
                        operation.id
                    )));
                }
                if operation.phase == OperationPhase::Complete
                    && !operation
                        .split_required_nodes
                        .is_subset(&operation.split_cleaned_nodes)
                {
                    return Err(Error::ClusterConfig(format!(
                        "complete split operation {} is missing cleanup acknowledgements",
                        operation.id
                    )));
                }
                if !committed
                    && operation.phase != OperationPhase::Aborted
                    && self.routing != Routing::LinearV1(before)
                {
                    return Err(Error::ClusterConfig(format!(
                        "uncommitted split operation {} does not match catalog routing",
                        operation.id
                    )));
                }
            }
            if operation.kind == OperationKind::Join {
                let destination = operation.destination.ok_or_else(|| {
                    Error::ClusterConfig(format!(
                        "join operation {} has no destination",
                        operation.id
                    ))
                })?;
                if !self.members.contains_key(&destination) {
                    return Err(Error::ClusterConfig(format!(
                        "join operation {} names missing node {destination}",
                        operation.id
                    )));
                }
            }
            if operation.kind == OperationKind::Drain {
                let source = operation.source.ok_or_else(|| {
                    Error::ClusterConfig(format!("drain operation {} has no source", operation.id))
                })?;
                if !self.members.contains_key(&source) {
                    return Err(Error::ClusterConfig(format!(
                        "drain operation {} names missing node {source}",
                        operation.id
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Crash-safe local catalog copy.
#[derive(Debug)]
pub struct CatalogStore {
    path: PathBuf,
    prepared_path: PathBuf,
    inner: Mutex<Catalog>,
    prepared: Mutex<Option<CatalogProposal>>,
}

impl CatalogStore {
    pub fn exists(dir: &Path) -> bool {
        catalog_path(dir).exists()
    }

    pub fn create(dir: &Path, catalog: Catalog) -> Result<Self> {
        catalog.validate()?;
        let path = catalog_path(dir);
        if path.exists() {
            return Err(Error::ClusterConfig(format!(
                "{} already exists; refusing to replace a cluster catalog",
                path.display()
            )));
        }
        persist(&path, &catalog)?;
        Ok(Self {
            prepared_path: prepared_catalog_path(dir),
            path,
            inner: Mutex::new(catalog),
            prepared: Mutex::new(None),
        })
    }

    pub fn open(dir: &Path) -> Result<Self> {
        let path = catalog_path(dir);
        let catalog = read(&path)?;
        catalog.validate()?;
        let prepared_path = prepared_catalog_path(dir);
        let prepared = if prepared_path.exists() {
            let proposal: CatalogProposal = read_value(&prepared_path, PREPARED_MAGIC)?;
            if catalog == proposal.catalog {
                // The committed catalog rename won the crash race and only marker cleanup was
                // interrupted. The value is committed; finish the idempotent cleanup.
                remove_prepared(&prepared_path)?;
                None
            } else {
                validate_proposal(&catalog, &proposal)?;
                Some(proposal)
            }
        } else {
            None
        };
        Ok(Self {
            path,
            inner: Mutex::new(catalog),
            prepared_path,
            prepared: Mutex::new(prepared),
        })
    }

    pub fn snapshot(&self) -> Catalog {
        self.inner.lock().expect("catalog mutex").clone()
    }

    /// Install a leader-supplied committed snapshot on a lagging member.
    ///
    /// Voters normally advance through prepare/commit, but may miss enough committed values that
    /// replaying only the current value cannot bridge the version gap. This is the metadata
    /// equivalent of snapshot install: the authorized leader supplies an already chosen value,
    /// while the monotonic, identity, conflict, and prepared-value checks below prevent it from
    /// overwriting incompatible local evidence. A learner joining offline uses the same primitive
    /// before announcing catch-up.
    pub fn install_committed(&self, catalog: Catalog) -> Result<()> {
        catalog.validate()?;
        let mut current = self.inner.lock().expect("catalog mutex");
        if catalog.cluster_id != current.cluster_id {
            return Err(Error::ClusterConfig(format!(
                "cannot install catalog for cluster {} over local cluster {}",
                catalog.cluster_id, current.cluster_id
            )));
        }
        if catalog.version < current.version {
            return Err(Error::ClusterConfig(format!(
                "catalog snapshot regresses from version {} to {}",
                current.version, catalog.version
            )));
        }
        if catalog.version == current.version {
            if catalog == *current {
                return Ok(());
            }
            return Err(Error::ClusterConfig(format!(
                "catalog version {} has conflicting contents",
                catalog.version
            )));
        }
        if self
            .prepared
            .lock()
            .expect("prepared catalog mutex")
            .is_some()
        {
            return Err(Error::ClusterConfig(
                "cannot install a catalog snapshot while a proposal is prepared".into(),
            ));
        }
        persist(&self.path, &catalog)?;
        *current = catalog;
        Ok(())
    }

    pub fn prepared_snapshot(&self) -> Option<CatalogProposal> {
        self.prepared
            .lock()
            .expect("prepared catalog mutex")
            .clone()
    }

    /// Latest durable catalog value this voter must carry into an election. Prepared values count:
    /// once a quorum has fsynced one it is chosen even if the old leader dies before broadcasting
    /// the commit marker.
    pub fn durability_position(&self) -> super::durability::CatalogPosition {
        let current = self.inner.lock().expect("catalog mutex");
        let prepared = self.prepared.lock().expect("prepared catalog mutex");
        let catalog = prepared
            .as_ref()
            .map(|proposal| &proposal.catalog)
            .unwrap_or(&current);
        super::durability::CatalogPosition {
            version: catalog.version,
            digest: catalog_digest(catalog).expect("a persisted catalog must remain encodable"),
        }
    }

    /// Compare, mutate, validate, fsync, then publish in memory.
    ///
    /// A quorum layer calls this before acknowledging the catalog entry to its leader.
    pub fn apply<T>(
        &self,
        expected_version: u64,
        change: impl FnOnce(&mut Catalog) -> Result<T>,
    ) -> Result<T> {
        let mut current = self.inner.lock().expect("catalog mutex");
        if current.version != expected_version {
            return Err(Error::ClusterConfig(format!(
                "stale catalog update expected version {expected_version}, current version is {}",
                current.version
            )));
        }
        if self
            .prepared
            .lock()
            .expect("prepared catalog mutex")
            .is_some()
        {
            return Err(Error::ClusterConfig(
                "a catalog proposal is prepared; commit or supersede it before a local update"
                    .into(),
            ));
        }
        let mut next = current.clone();
        let output = change(&mut next)?;
        next.version = next
            .version
            .checked_add(1)
            .ok_or_else(|| Error::ClusterConfig("catalog version space exhausted".into()))?;
        next.validate()?;
        persist(&self.path, &next)?;
        *current = next;
        Ok(output)
    }

    /// Build a validated next catalog value without publishing it. A leader prepares this exact
    /// value on voters, then commits the same bytes after a quorum accepts it.
    pub fn propose<T>(
        &self,
        term: u64,
        expected_version: u64,
        change: impl FnOnce(&mut Catalog) -> Result<T>,
    ) -> Result<(T, CatalogProposal)> {
        let current = self.inner.lock().expect("catalog mutex");
        if current.version != expected_version {
            return Err(Error::ClusterConfig(format!(
                "stale catalog proposal expected version {expected_version}, current version is {}",
                current.version
            )));
        }
        let mut next = current.clone();
        let output = change(&mut next)?;
        next.version = next
            .version
            .checked_add(1)
            .ok_or_else(|| Error::ClusterConfig("catalog version space exhausted".into()))?;
        next.validate()?;
        Ok((
            output,
            CatalogProposal {
                term,
                expected_version,
                catalog: next,
            },
        ))
    }

    /// Fsync a proposal before acknowledging it to the leader.
    pub fn prepare(&self, proposal: CatalogProposal) -> Result<()> {
        let current = self.inner.lock().expect("catalog mutex");
        if current.version == proposal.catalog.version && *current == proposal.catalog {
            // The commit reached this voter and its acknowledgement was lost.
            return Ok(());
        }
        validate_proposal(&current, &proposal)?;

        let mut prepared = self.prepared.lock().expect("prepared catalog mutex");
        if let Some(existing) = prepared.as_ref() {
            if existing == &proposal {
                return Ok(());
            }
            if existing.term > proposal.term {
                return Err(Error::ClusterConfig(format!(
                    "catalog proposal term {} is stale; this voter prepared term {}",
                    proposal.term, existing.term
                )));
            }
            if existing.term == proposal.term {
                return Err(Error::ClusterConfig(format!(
                    "catalog term {} already prepared a different value for version {}; refusing \
                     equivocation",
                    proposal.term, proposal.catalog.version
                )));
            }
        }
        persist_value(&self.prepared_path, PREPARED_MAGIC, &proposal)?;
        *prepared = Some(proposal);
        Ok(())
    }

    /// Commit only the exact value this voter previously fsynced.
    pub fn commit_prepared(&self, term: u64, version: u64) -> Result<()> {
        let mut current = self.inner.lock().expect("catalog mutex");
        let mut prepared = self.prepared.lock().expect("prepared catalog mutex");
        if current.version == version {
            if prepared.as_ref().is_some_and(|proposal| {
                proposal.term == term && proposal.catalog.version == version
            }) {
                remove_prepared(&self.prepared_path)?;
                *prepared = None;
            }
            return Ok(());
        }
        let proposal = prepared.as_ref().ok_or_else(|| {
            Error::ClusterConfig(format!(
                "catalog version {version} term {term} was not prepared on this voter"
            ))
        })?;
        if proposal.term != term || proposal.catalog.version != version {
            return Err(Error::ClusterConfig(format!(
                "commit requests term {term} version {version}, but this voter prepared term {} \
                 version {}",
                proposal.term, proposal.catalog.version
            )));
        }
        persist(&self.path, &proposal.catalog)?;
        *current = proposal.catalog.clone();
        remove_prepared(&self.prepared_path)?;
        *prepared = None;
        Ok(())
    }
}

fn normalized_load_cmp(
    left_count: u64,
    left_weight: u32,
    right_count: u64,
    right_weight: u32,
) -> std::cmp::Ordering {
    (left_count as u128 * right_weight as u128).cmp(&(right_count as u128 * left_weight as u128))
}

fn same_failure_domain(left: Option<&str>, right: Option<&str>) -> bool {
    matches!((left, right), (Some(left), Some(right)) if left == right)
}

fn catalog_path(dir: &Path) -> PathBuf {
    dir.join(DIRECTORY).join(FILE)
}

fn prepared_catalog_path(dir: &Path) -> PathBuf {
    dir.join(DIRECTORY).join(PREPARED_FILE)
}

fn persist(path: &Path, catalog: &Catalog) -> Result<()> {
    persist_value(path, MAGIC, catalog)
}

fn persist_value<T: Serialize>(path: &Path, magic: &[u8], value: &T) -> Result<()> {
    if crate::failpoint::active("disk.catalog.write") {
        return Err(Error::Manifest(format!(
            "injected disk-full fault while persisting {}",
            path.display()
        )));
    }
    let parent = path.parent().expect("catalog path has parent");
    fs::create_dir_all(parent)
        .map_err(|error| Error::Manifest(format!("creating {}: {error}", parent.display())))?;
    let body = bincode::serde::encode_to_vec(value, bincode::config::standard())
        .map_err(|error| Error::Manifest(format!("encoding catalog: {error}")))?;
    if body.len() > MAX_CATALOG_BYTES {
        return Err(Error::Manifest(format!(
            "catalog is {} bytes, above the {} byte limit",
            body.len(),
            MAX_CATALOG_BYTES
        )));
    }
    let checksum = blake3::hash(&body);
    let tmp = path.with_extension("tmp");
    {
        let mut file = fs::File::create(&tmp)
            .map_err(|error| Error::Manifest(format!("creating {}: {error}", tmp.display())))?;
        file.write_all(magic)
            .and_then(|_| file.write_all(checksum.as_bytes()))
            .and_then(|_| file.write_all(&(body.len() as u64).to_le_bytes()))
            .and_then(|_| file.write_all(&body))
            .map_err(|error| Error::Manifest(format!("writing {}: {error}", tmp.display())))?;
        file.sync_all()
            .map_err(|error| Error::Manifest(format!("fsyncing {}: {error}", tmp.display())))?;
    }
    fs::rename(&tmp, path)
        .map_err(|error| Error::Manifest(format!("renaming into {}: {error}", path.display())))?;
    if let Ok(directory) = fs::File::open(parent) {
        directory
            .sync_all()
            .map_err(|error| Error::Manifest(format!("fsyncing {}: {error}", parent.display())))?;
    }
    Ok(())
}

fn read(path: &Path) -> Result<Catalog> {
    read_value(path, MAGIC)
}

fn read_value<T: for<'de> Deserialize<'de>>(path: &Path, magic: &[u8]) -> Result<T> {
    let bytes = fs::read(path)
        .map_err(|error| Error::Manifest(format!("reading {}: {error}", path.display())))?;
    let prefix = magic.len() + 32 + 8;
    if bytes.len() < prefix || &bytes[..magic.len()] != magic {
        return Err(Error::Manifest(format!(
            "{} is not a shardlite catalog or has an unknown format",
            path.display()
        )));
    }
    let checksum = &bytes[magic.len()..magic.len() + 32];
    let length = u64::from_le_bytes(
        bytes[magic.len() + 32..prefix]
            .try_into()
            .expect("eight-byte catalog length"),
    ) as usize;
    if length > MAX_CATALOG_BYTES || bytes.len() != prefix + length {
        return Err(Error::Manifest(format!(
            "{} has invalid catalog length {length}",
            path.display()
        )));
    }
    let body = &bytes[prefix..];
    if blake3::hash(body).as_bytes() != checksum {
        return Err(Error::Manifest(format!(
            "{} failed its catalog checksum; refusing torn or corrupted membership state",
            path.display()
        )));
    }
    let (value, used): (T, usize) =
        bincode::serde::decode_from_slice(body, bincode::config::standard())
            .map_err(|error| Error::Manifest(format!("decoding {}: {error}", path.display())))?;
    if used != body.len() {
        return Err(Error::Manifest(format!(
            "{} has {} trailing catalog bytes",
            path.display(),
            body.len() - used
        )));
    }
    Ok(value)
}

fn validate_proposal(current: &Catalog, proposal: &CatalogProposal) -> Result<()> {
    if proposal.expected_version != current.version
        || proposal.catalog.version != current.version.saturating_add(1)
    {
        return Err(Error::ClusterConfig(format!(
            "catalog proposal expected/next versions are {}/{}, current committed version is {}",
            proposal.expected_version, proposal.catalog.version, current.version
        )));
    }
    if proposal.catalog.cluster_id != current.cluster_id {
        return Err(Error::ClusterConfig(format!(
            "catalog proposal belongs to cluster {}, local voter belongs to {}",
            proposal.catalog.cluster_id, current.cluster_id
        )));
    }
    if proposal.catalog.routing_epoch < current.routing_epoch {
        return Err(Error::ClusterConfig(format!(
            "catalog proposal routing epoch {} regresses from {}",
            proposal.catalog.routing_epoch, current.routing_epoch
        )));
    }
    current
        .compatibility
        .check(&proposal.catalog.compatibility)?;
    proposal.catalog.validate()
}

fn catalog_digest(catalog: &Catalog) -> Result<[u8; 32]> {
    let bytes = bincode::serde::encode_to_vec(catalog, bincode::config::standard())
        .map_err(|error| Error::Manifest(format!("encoding catalog digest: {error}")))?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

fn remove_prepared(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(Error::Manifest(format!(
                "removing {}: {error}",
                path.display()
            )));
        }
    }
    if let Some(parent) = path.parent()
        && let Ok(directory) = fs::File::open(parent)
    {
        directory
            .sync_all()
            .map_err(|error| Error::Manifest(format!("fsyncing {}: {error}", parent.display())))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::{LinearV1, ModuloV1};
    use tempfile::TempDir;

    fn catalog() -> Catalog {
        let routing = Routing::LinearV1(LinearV1::ONE);
        Catalog::bootstrap(
            ClusterId([7; 16]),
            1,
            "node-1:4600".into(),
            routing,
            Compatibility::current(routing).unwrap(),
        )
        .unwrap()
    }

    fn placed_catalog() -> Catalog {
        let mut catalog = catalog();
        catalog.add_learner(2, 1, "node-2:4600".into()).unwrap();
        catalog.promote(2, MemberRole::Storage).unwrap();
        catalog.initialize_placements(1).unwrap();
        catalog
    }

    #[test]
    fn initial_placement_honours_weights_and_separates_replica_domains() {
        let routing = Routing::LinearV1(LinearV1::for_shard_count(8).unwrap());
        let mut catalog = Catalog::bootstrap(
            ClusterId([8; 16]),
            1,
            "node-1:4600".into(),
            routing,
            Compatibility::current(routing).unwrap(),
        )
        .unwrap();
        for node in 2..=3 {
            catalog
                .add_learner(node, 1, format!("node-{node}:4600"))
                .unwrap();
            catalog.promote(node, MemberRole::Storage).unwrap();
        }
        catalog
            .set_member_policy(1, 3, Some("zone-a".into()))
            .unwrap();
        catalog
            .set_member_policy(2, 1, Some("zone-a".into()))
            .unwrap();
        catalog
            .set_member_policy(3, 1, Some("zone-b".into()))
            .unwrap();
        catalog.initialize_placements(2).unwrap();

        let primaries_on_one = catalog
            .placements
            .values()
            .filter(|placement| placement.primary == 1)
            .count();
        assert!(
            primaries_on_one >= 4,
            "higher-capacity node did not receive proportionally more primaries"
        );
        for placement in catalog.placements.values() {
            let primary_domain = catalog.members[&placement.primary]
                .failure_domain
                .as_deref();
            assert!(
                placement.replicas.iter().any(|replica| {
                    catalog.members[replica].failure_domain.as_deref() != primary_domain
                }),
                "replica was not placed in the available alternate failure domain"
            );
        }
    }

    #[test]
    fn scaling_policy_rejects_unbounded_or_unsupported_runtime_settings() {
        let mut catalog = catalog();
        let mut policy = ScalingPolicy::default();
        policy.split_log_max_rows = 0;
        assert!(catalog.set_scaling_policy(policy).is_err());
        let mut policy = ScalingPolicy::default();
        policy.max_concurrent_topology_operations = 2;
        assert!(catalog.set_scaling_policy(policy).is_err());
    }

    #[test]
    fn catalog_is_durable_before_an_update_is_visible() {
        let dir = TempDir::new().unwrap();
        let store = CatalogStore::create(dir.path(), catalog()).unwrap();
        store
            .apply(1, |catalog| catalog.add_learner(2, 1, "node-2:4600".into()))
            .unwrap();
        assert_eq!(store.snapshot().version, 2);

        let reopened = CatalogStore::open(dir.path()).unwrap();
        assert_eq!(reopened.snapshot(), store.snapshot());
        assert_eq!(reopened.snapshot().members[&2].role, MemberRole::Learner);
    }

    #[test]
    fn stale_updates_are_refused_without_mutation() {
        let dir = TempDir::new().unwrap();
        let store = CatalogStore::create(dir.path(), catalog()).unwrap();
        let before = store.snapshot();
        let error = store
            .apply(99, |catalog| {
                catalog.add_learner(2, 1, "node-2:4600".into())
            })
            .unwrap_err();
        assert!(error.to_string().contains("stale catalog update"));
        assert_eq!(store.snapshot(), before);
    }

    #[test]
    fn prepared_catalog_is_durable_and_invisible_until_commit() {
        let dir = TempDir::new().unwrap();
        let store = CatalogStore::create(dir.path(), catalog()).unwrap();
        let (_, proposal) = store
            .propose(4, 1, |catalog| {
                catalog.add_learner(2, 1, "node-2:4600".into())
            })
            .unwrap();
        store.prepare(proposal.clone()).unwrap();
        assert_eq!(store.snapshot().version, 1);
        assert!(!store.snapshot().members.contains_key(&2));
        drop(store);

        let reopened = CatalogStore::open(dir.path()).unwrap();
        assert_eq!(reopened.prepared_snapshot(), Some(proposal));
        assert_eq!(reopened.snapshot().version, 1);
        reopened.commit_prepared(4, 2).unwrap();
        assert_eq!(reopened.snapshot().version, 2);
        assert_eq!(reopened.snapshot().members[&2].role, MemberRole::Learner);
        assert!(!prepared_catalog_path(dir.path()).exists());
    }

    #[test]
    fn catalog_prepare_and_commit_are_idempotent_but_equivocation_is_refused() {
        let dir = TempDir::new().unwrap();
        let store = CatalogStore::create(dir.path(), catalog()).unwrap();
        let (_, proposal) = store
            .propose(8, 1, |catalog| {
                catalog.add_learner(2, 1, "node-2:4600".into())
            })
            .unwrap();
        store.prepare(proposal.clone()).unwrap();
        store.prepare(proposal.clone()).unwrap();

        let (_, conflicting) = store
            .propose(8, 1, |catalog| {
                catalog.add_learner(3, 1, "node-3:4600".into())
            })
            .unwrap();
        let error = store.prepare(conflicting).unwrap_err();
        assert!(error.to_string().contains("equivocation"), "{error}");

        store.commit_prepared(8, 2).unwrap();
        store.commit_prepared(8, 2).unwrap();
        // A duplicate prepare received after commit is also harmless.
        store.prepare(proposal).unwrap();
    }

    #[test]
    fn a_higher_term_supersedes_an_uncommitted_proposal() {
        let dir = TempDir::new().unwrap();
        let store = CatalogStore::create(dir.path(), catalog()).unwrap();
        let (_, old) = store
            .propose(2, 1, |catalog| {
                catalog.add_learner(2, 1, "node-2:4600".into())
            })
            .unwrap();
        let (_, new) = store
            .propose(3, 1, |catalog| {
                catalog.add_learner(3, 1, "node-3:4600".into())
            })
            .unwrap();
        store.prepare(old).unwrap();
        store.prepare(new).unwrap();
        let error = store.commit_prepared(2, 2).unwrap_err();
        assert!(error.to_string().contains("prepared term 3"), "{error}");
        store.commit_prepared(3, 2).unwrap();
        assert!(store.snapshot().members.contains_key(&3));
        assert!(!store.snapshot().members.contains_key(&2));
    }

    #[test]
    fn checksum_detects_corruption() {
        let dir = TempDir::new().unwrap();
        CatalogStore::create(dir.path(), catalog()).unwrap();
        let path = catalog_path(dir.path());
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        fs::write(&path, bytes).unwrap();
        let error = CatalogStore::open(dir.path()).unwrap_err();
        assert!(error.to_string().contains("checksum"), "{error}");
    }

    #[test]
    fn foreign_cluster_and_incompatible_join_are_refused() {
        let catalog = catalog();
        let compatibility = catalog.compatibility.clone();
        assert!(
            catalog
                .validate_join(Some(ClusterId([8; 16])), &compatibility)
                .unwrap_err()
                .to_string()
                .contains("foreign")
        );

        let mut incompatible = compatibility;
        incompatible.sqlite_version = "something-else".into();
        assert!(
            catalog
                .validate_join(None, &incompatible)
                .unwrap_err()
                .to_string()
                .contains("SQLite version")
        );
    }

    #[test]
    fn join_stays_write_ineligible_until_catalog_catch_up_is_committed() {
        let mut catalog = catalog();
        let compatibility = catalog.compatibility.clone();
        let operation = catalog
            .start_join(None, &compatibility, 2, 1, "node-2:4600".into())
            .unwrap();
        assert_eq!(catalog.members[&2].role, MemberRole::Learner);
        assert!(!catalog.members[&2].may_receive_placement());

        catalog.record_join_catch_up(operation, 1).unwrap();
        let error = catalog.prepare_join(operation).unwrap_err();
        assert!(error.to_string().contains("short of required"), "{error}");
        catalog.record_join_catch_up(operation, 2).unwrap();
        catalog.prepare_join(operation).unwrap();
        catalog.commit_storage_join(operation).unwrap();
        assert_eq!(catalog.members[&2].role, MemberRole::Storage);
        catalog.complete_join(operation).unwrap();
    }

    #[test]
    fn drain_is_durable_intent_and_removal_waits_for_every_copy() {
        let mut catalog = placed_catalog();
        let operation = catalog.start_drain(1).unwrap();
        assert_eq!(catalog.members[&1].state, MemberState::Draining);
        let error = catalog.complete_drain(operation).unwrap_err();
        assert!(error.to_string().contains("still holds"), "{error}");
        let error = catalog.remove_member(1).unwrap_err();
        assert!(error.to_string().contains("voter"), "{error}");
        catalog
            .begin_voter_change(std::collections::BTreeSet::from([2]))
            .unwrap();
        catalog.finalize_voter_change().unwrap();
        let error = catalog.remove_member(1).unwrap_err();
        assert!(error.to_string().contains("drain"), "{error}");

        catalog.placements.get_mut(&ShardId(0)).unwrap().primary = 2;
        catalog.complete_drain(operation).unwrap();
        catalog.remove_member(1).unwrap();
        assert!(!catalog.members.contains_key(&1));
    }

    #[test]
    fn voter_changes_pass_through_joint_consensus() {
        let mut catalog = catalog();
        catalog.add_learner(2, 1, "node-2:4600".into()).unwrap();
        catalog.promote(2, MemberRole::Storage).unwrap();
        catalog
            .begin_voter_change(std::collections::BTreeSet::from([1, 2]))
            .unwrap();
        let (old, next) = catalog.voter_sets();
        assert_eq!(old, std::collections::BTreeSet::from([1]));
        assert_eq!(next, Some(std::collections::BTreeSet::from([1, 2])));
        assert!(catalog.is_voter(2));

        catalog.finalize_voter_change().unwrap();
        assert!(catalog.voter_transition.is_none());
        assert_eq!(catalog.members[&2].role, MemberRole::Voter);
    }

    #[test]
    fn a_split_commits_one_new_shard_and_one_routing_epoch() {
        let mut catalog = placed_catalog();
        let operation = catalog.start_split().unwrap();
        let split = catalog.operations[&operation].clone();
        assert_eq!(split.shard, Some(ShardId(0)));
        assert_eq!(
            split.routing_after,
            Some(Routing::LinearV1(LinearV1::for_shard_count(2).unwrap()))
        );

        catalog.begin_split_snapshot(operation).unwrap();
        catalog.split_snapshot_ready(operation, 3).unwrap();
        catalog.record_split_replay(operation, 8).unwrap();
        catalog.prepare_split(operation).unwrap();
        catalog.begin_split_fencing(operation).unwrap();
        catalog.split_fenced(operation, 9).unwrap();
        catalog.record_split_installed(operation, 1).unwrap();
        let old_epoch = catalog.routing_epoch;
        catalog.commit_split(operation).unwrap();

        assert_eq!(catalog.routing.shard_count(), 2);
        assert_eq!(catalog.routing_epoch, old_epoch + 1);
        assert_eq!(catalog.placements[&ShardId(1)].primary, 1);
        assert_eq!(
            catalog.operations[&operation].phase,
            OperationPhase::Committed
        );
    }

    #[test]
    fn a_split_waits_for_every_replica_install_before_routing_commit() {
        let mut catalog = placed_catalog();
        catalog
            .placements
            .get_mut(&ShardId(0))
            .unwrap()
            .replicas
            .push(2);
        let operation = catalog.start_split().unwrap();
        assert_eq!(
            catalog.operations[&operation].split_required_nodes,
            BTreeSet::from([1, 2])
        );
        catalog.begin_split_snapshot(operation).unwrap();
        catalog.split_snapshot_ready(operation, 3).unwrap();
        catalog.record_split_replay(operation, 8).unwrap();
        catalog.prepare_split(operation).unwrap();
        catalog.begin_split_fencing(operation).unwrap();
        catalog.split_fenced(operation, 9).unwrap();
        catalog.record_split_installed(operation, 1).unwrap();
        let error = catalog.commit_split(operation).unwrap_err();
        assert!(error.to_string().contains("[2]"), "{error}");

        catalog.record_split_installed(operation, 2).unwrap();
        catalog.commit_split(operation).unwrap();
        assert_eq!(catalog.placements[&ShardId(1)].replicas, vec![2]);
    }

    #[test]
    fn learner_cannot_be_a_primary_and_used_member_cannot_be_removed() {
        let mut catalog = catalog();
        catalog.add_learner(2, 1, "node-2:4600".into()).unwrap();
        catalog.placements.insert(
            ShardId(0),
            ReplicaSet {
                generation: 1,
                primary: 2,
                replicas: vec![1],
            },
        );
        assert!(
            catalog
                .validate()
                .unwrap_err()
                .to_string()
                .contains("learner")
        );
        assert!(
            catalog
                .remove_member(2)
                .unwrap_err()
                .to_string()
                .contains("drain")
        );
    }

    #[test]
    fn one_operation_per_shard_is_enforced_and_abort_boundary_is_explicit() {
        let mut catalog = catalog();
        let id = catalog
            .start_operation(
                OperationKind::Split,
                Some(ShardId(0)),
                Some(1),
                Some(2),
                Some(1),
            )
            .unwrap();
        assert!(catalog.operations[&id].phase.may_abort());
        assert!(
            catalog
                .start_operation(
                    OperationKind::Transfer,
                    Some(ShardId(0)),
                    Some(1),
                    Some(3),
                    Some(1)
                )
                .unwrap_err()
                .to_string()
                .contains("active topology operation")
        );
        catalog.operations.get_mut(&id).unwrap().phase = OperationPhase::Committed;
        assert!(!catalog.operations[&id].phase.may_abort());
    }

    #[test]
    fn transfer_requires_exact_durable_final_lsn_and_commits_one_generation() {
        let mut catalog = placed_catalog();
        let operation = catalog.start_transfer(ShardId(0), 2).unwrap();
        catalog.begin_snapshot(operation).unwrap();
        catalog.snapshot_installed(operation, 7, 100).unwrap();
        catalog.record_catch_up(operation, 7, 120).unwrap();
        catalog.prepare_transfer(operation).unwrap();
        catalog.begin_fencing(operation).unwrap();
        catalog.record_final_lsn(operation, 7, 125).unwrap();

        let error = catalog.commit_transfer(operation).unwrap_err();
        assert!(
            error.to_string().contains("short of final LSN 125"),
            "{error}"
        );
        assert_eq!(catalog.placements[&ShardId(0)].primary, 1);

        catalog.record_fenced_catch_up(operation, 7, 125).unwrap();
        catalog.commit_transfer(operation).unwrap();
        let placement = &catalog.placements[&ShardId(0)];
        assert_eq!(placement.primary, 2);
        assert_eq!(placement.generation, 2);
        assert_eq!(placement.replicas, vec![1]);

        catalog.begin_cleanup(operation).unwrap();
        catalog.complete_transfer(operation).unwrap();
        assert!(catalog.placements[&ShardId(0)].replicas.is_empty());
        assert_eq!(
            catalog.operations[&operation].phase,
            OperationPhase::Complete
        );
    }

    #[test]
    fn transfer_resumes_from_a_durable_fencing_record_after_restart() {
        let dir = TempDir::new().unwrap();
        let store = CatalogStore::create(dir.path(), placed_catalog()).unwrap();
        let operation = store
            .apply(1, |catalog| catalog.start_transfer(ShardId(0), 2))
            .unwrap();
        store
            .apply(2, |catalog| catalog.begin_snapshot(operation))
            .unwrap();
        store
            .apply(3, |catalog| catalog.snapshot_installed(operation, 4, 10))
            .unwrap();
        store
            .apply(4, |catalog| catalog.prepare_transfer(operation))
            .unwrap();
        store
            .apply(5, |catalog| catalog.begin_fencing(operation))
            .unwrap();
        store
            .apply(6, |catalog| catalog.record_final_lsn(operation, 4, 11))
            .unwrap();
        drop(store);

        let reopened = CatalogStore::open(dir.path()).unwrap();
        let persisted = reopened.snapshot();
        assert_eq!(
            persisted.operations[&operation].phase,
            OperationPhase::Fencing
        );
        assert_eq!(persisted.operations[&operation].final_lsn, Some(11));
        reopened
            .apply(7, |catalog| {
                catalog.record_fenced_catch_up(operation, 4, 11)
            })
            .unwrap();
        reopened
            .apply(8, |catalog| catalog.commit_transfer(operation))
            .unwrap();
        assert_eq!(reopened.snapshot().placements[&ShardId(0)].primary, 2);
    }

    #[test]
    fn stale_or_out_of_order_transfer_commands_are_refused() {
        let mut catalog = placed_catalog();
        let operation = catalog.start_transfer(ShardId(0), 2).unwrap();
        let error = catalog.snapshot_installed(operation, 1, 10).unwrap_err();
        assert!(error.to_string().contains("out-of-order"), "{error}");
        catalog.begin_snapshot(operation).unwrap();
        catalog.snapshot_installed(operation, 1, 10).unwrap();
        let error = catalog.record_catch_up(operation, 2, 11).unwrap_err();
        assert!(error.to_string().contains("reported epoch 2"), "{error}");

        catalog
            .abort_operation(operation, "operator cancelled".into())
            .unwrap();
        let error = catalog.begin_snapshot(operation).unwrap_err();
        assert!(error.to_string().contains("out-of-order"), "{error}");
    }

    #[test]
    fn source_is_retained_when_an_existing_replica_is_promoted() {
        let mut catalog = placed_catalog();
        catalog.placements.get_mut(&ShardId(0)).unwrap().replicas = vec![2];
        let operation = catalog.start_transfer(ShardId(0), 2).unwrap();
        catalog.begin_snapshot(operation).unwrap();
        catalog.snapshot_installed(operation, 1, 5).unwrap();
        catalog.prepare_transfer(operation).unwrap();
        catalog.begin_fencing(operation).unwrap();
        catalog.record_final_lsn(operation, 1, 5).unwrap();
        catalog.commit_transfer(operation).unwrap();
        catalog.begin_cleanup(operation).unwrap();
        catalog.complete_transfer(operation).unwrap();
        assert_eq!(catalog.placements[&ShardId(0)].replicas, vec![1]);
    }

    #[test]
    fn legacy_compatibility_is_versioned_separately() {
        let routing = Routing::ModuloV1(ModuloV1::new(64).unwrap());
        let compatibility = Compatibility::current(routing).unwrap();
        assert_eq!(compatibility.routing_scheme, "modulo-v1");
        assert_eq!(compatibility.hash_scheme, "fnv1a-v1");
    }
}
