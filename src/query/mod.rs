//! Cross-shard query planning and result merging.
//!
//! Shards are separate SQLite databases. A query that spans them is answered by fanning out
//! and combining, which only works for shapes where combining partial results reproduces
//! the global answer. Everything else is refused — see [`plan`].

pub mod merge;
pub mod plan;

pub use merge::merge_results;
pub use plan::{Combine, Grouped, OutputCol, Plan, PostProcess, SortKey, Unsupported, plan};
