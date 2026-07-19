//! WAL frame capture through a pass-through SQLite VFS.
//!
//! This is the mechanism physical replication depends on: followers apply captured page
//! frames rather than re-executing SQL, so non-deterministic functions and per-machine
//! errors cannot make a replica diverge from its primary.

pub mod passthrough;
pub mod wal;

pub use passthrough::{VFS_NAME, capture_for, capture_for_with_limit, register};
pub use wal::{CaptureStats, CommittedTxn, Frame, WalCapture, apply_to_db_file};
