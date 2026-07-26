//! Cluster membership, leadership, and failover.
//!
//! Leadership is decided by Raft's **election** algorithm and nothing else. There is no
//! replicated log here: frames stream out of band with their own epoch and dense LSN, and
//! carry their own gap detection. What remains is the part of Raft that decides who may
//! write — terms, votes, and a heartbeat lease — plus the fencing token that makes a
//! deposed leader harmless.
//!
//! Skipping the log is not a shortcut around the hard part; it moves the hard part. Raft's
//! log-matching property is what normally guarantees a new leader holds every committed
//! entry. Here that guarantee has to be re-established over frame positions, which is what
//! the election restriction in [`election`] does.

pub mod catalog;
pub mod catalog_quorum;
pub mod control;
pub mod durability;
pub mod election;
pub mod fence;
pub mod node;
pub mod placement;
pub mod promotion;
pub mod rebalance;
pub mod split;
pub mod term;
pub mod transfer;

pub use catalog::{
    Catalog, CatalogOperation, CatalogProposal, CatalogStore, ClusterId, Compatibility, Member,
    MemberRole, MemberState, OperationKind, OperationPhase, ReplicaSet, ScalingPolicy,
    VoterTransition,
};
pub use catalog_quorum::CatalogQuorum;
pub use control::{
    CatalogCommand, CatalogCommandResult, CatalogControl, JoinTokenStore, SplitProgress,
    TransferProgress,
};
pub use durability::{CatalogPosition, Comparison, Durability};
pub use election::{
    Action, Election, ElectionConfig, Heartbeat, HeartbeatReply, Role, VoteReply, VoteRequest,
};
pub use fence::{Fence, FenceStats, FenceToken};
pub use node::{ClusterNode, ClusterStats, DurabilitySource};
pub use placement::Placement;
pub use promotion::Promotion;
pub use rebalance::{RebalanceDecision, next_rebalance, plan_next};
pub use split::SplitSupervisor;
pub use term::{NodeId, Term, TermStore};
pub use transfer::TransferSupervisor;
