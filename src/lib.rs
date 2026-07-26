//! shardlite — a high-availability, multi-write SQLite server.
//!
//! Writes to any single database file are serialized through one writer connection and
//! batched into shared transactions (group commit); concurrency for writers comes from
//! sharding across files, not from concurrent writers to one file. SQLite serializes
//! writes at the file level in the pager, and no unpatched design escapes that.

pub mod cluster;
pub mod config;
pub mod db;
pub mod error;
pub mod failpoint;
pub mod net;
pub mod query;
pub mod replication;
#[cfg(feature = "s3")]
pub mod s3;
pub mod shard;
pub mod storage;
pub(crate) mod sync;
pub mod vfs;

pub use error::{Error, Result};

/// Re-exported so callers bind against the exact SQLite this crate links.
///
/// Replication is byte-level, so a caller linking a different rusqlite (and therefore a
/// different bundled SQLite) would be a divergence source.
pub use rusqlite;
