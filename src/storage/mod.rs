//! Storage layer: connection configuration and lifecycle for a single database file.
//!
//! Nothing in this module knows about replication or clustering. That separation is
//! deliberate — it keeps the writer, batching, and crash-safety work independently
//! buildable and testable.

pub mod apply;
pub mod checkpoint;
pub mod exec;
pub mod open;
pub mod pragma;
pub mod schema;

pub use checkpoint::{CheckpointOutcome, CheckpointStats};
pub use exec::{Executed, Outcome, QueryResult, Statement, Value, WriteOutcome};
pub use open::{open_reader_existing, open_writer, open_writer_vfs};
pub use pragma::{WalConversionStats, wal_conversion_stats};
