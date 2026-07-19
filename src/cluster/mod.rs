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

pub mod durability;
pub mod election;
pub mod fence;
pub mod node;
pub mod promotion;
pub mod term;

pub use durability::{Comparison, Durability};
pub use election::{
    Action, Election, ElectionConfig, Heartbeat, HeartbeatReply, Role, VoteReply, VoteRequest,
};
pub use fence::{Fence, FenceStats, FenceToken};
pub use node::{ClusterNode, ClusterStats, DurabilitySource};
pub use promotion::Promotion;
pub use term::{NodeId, Term, TermStore};
