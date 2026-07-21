//! Cross-shard query planning and result merging.
//!
//! Shards are separate SQLite databases. A query that spans them is answered by fanning out
//! and combining, which only works for shapes where combining partial results reproduces
//! the global answer. Everything else is refused — see [`plan`].

pub mod merge;
pub mod plan;

pub use merge::{evaluate_set_tree, finalize_rows, merge_results};
pub use plan::{
    Combine, CompareOp, Grouped, HavingExpr, HavingValue, OutputCol, Plan, PostProcess, SetKind,
    SetOp, SetTree, ShardKeys, SortKey, Unsupported, plan, plan_with, substitute_scalar_subqueries,
};
