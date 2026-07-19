//! Per-shard schema versions, and the guard that keeps a half-migrated cluster honest.
//!
//! # Why DDL cannot be atomic here
//!
//! There is no atomic commit across shards — that is permanent, and it is why cross-shard
//! transactions are unavailable. A schema change therefore lands on one shard at a time, and
//! between the first and the last the cluster genuinely disagrees with itself: some shards
//! have the new column, some do not.
//!
//! Three ways to live with that:
//!
//! - **Pause every shard** for the duration. Correct, and a cluster-wide write outage that
//!   grows with shard count and data size.
//! - **Roll shard by shard** and let the application cope. What large sharded systems do; it
//!   pushes the problem onto every reader.
//! - **Roll shard by shard, and refuse cross-shard reads while versions disagree.** No
//!   outage for single-shard work, and the window is impossible to read wrongly.
//!
//! The third is what this implements, because a half-migrated answer is not a slower answer,
//! it is a wrong one — and returning it silently is the failure this project refuses.
//!
//! # Why `user_version`
//!
//! SQLite stores `user_version` in the **database header page**. Replication here is
//! physical — pages, not statements — so the version travels with the data automatically and
//! cannot drift from the schema it describes. A side table would be a second thing to keep
//! in step; the header is one thing.
//!
//! It also means a follower's version is correct the instant the frames land, with no
//! bookkeeping of our own to get wrong.

use rusqlite::Connection;

use crate::error::{Error, Result};
use crate::shard::ShardId;

/// A shard's schema generation. Starts at 0 and rises with each applied change.
pub type SchemaVersion = i64;

/// Read a shard's schema version.
pub fn version_of(conn: &Connection) -> Result<SchemaVersion> {
    conn.query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(Error::Sqlite)
}

/// Set a shard's schema version.
///
/// `user_version` takes no parameters — it is a pragma, not a statement — so the value is
/// formatted in. It is an `i64` from our own code and never user input, so there is nothing
/// to inject.
pub fn set_version(conn: &Connection, v: SchemaVersion) -> Result<()> {
    conn.execute_batch(&format!("PRAGMA user_version = {v}"))
        .map_err(Error::Sqlite)
}

/// What a set of shards agrees on, if anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Agreement {
    /// Every shard is on the same version.
    Agreed(SchemaVersion),
    /// A schema change is part-applied. Cross-shard reads cannot be answered correctly.
    Disagreed {
        lowest: SchemaVersion,
        highest: SchemaVersion,
        /// A shard sitting at each end, so an operator knows where to look rather than
        /// having to scan every shard by hand.
        behind: ShardId,
        ahead: ShardId,
    },
}

impl Agreement {
    /// Decide from `(shard, version)` pairs.
    ///
    /// An empty set agrees vacuously at version 0: a cluster with no shards has no schema to
    /// disagree about, and refusing there would break the trivial case for no reason.
    pub fn of(versions: &[(ShardId, SchemaVersion)]) -> Self {
        let Some(&(first_shard, first)) = versions.first() else {
            return Agreement::Agreed(0);
        };
        let mut lowest = (first_shard, first);
        let mut highest = (first_shard, first);
        for &(shard, v) in versions {
            if v < lowest.1 {
                lowest = (shard, v);
            }
            if v > highest.1 {
                highest = (shard, v);
            }
        }
        if lowest.1 == highest.1 {
            Agreement::Agreed(lowest.1)
        } else {
            Agreement::Disagreed {
                lowest: lowest.1,
                highest: highest.1,
                behind: lowest.0,
                ahead: highest.0,
            }
        }
    }

    /// `Err` when a cross-shard read cannot be answered correctly.
    pub fn check(&self) -> Result<SchemaVersion> {
        match self {
            Agreement::Agreed(v) => Ok(*v),
            Agreement::Disagreed {
                lowest,
                highest,
                behind,
                ahead,
            } => Err(Error::SchemaSkew {
                lowest: *lowest,
                highest: *highest,
                behind: behind.to_string(),
                ahead: ahead.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_versions_agree() {
        let a = Agreement::of(&[(ShardId(0), 3), (ShardId(1), 3), (ShardId(2), 3)]);
        assert_eq!(a, Agreement::Agreed(3));
        assert_eq!(a.check().unwrap(), 3);
    }

    #[test]
    fn a_part_applied_change_is_refused_not_answered() {
        // The whole point. A cross-shard count over a half-migrated schema is not a slower
        // answer, it is a wrong one, and returning it silently is the failure this refuses.
        let a = Agreement::of(&[(ShardId(0), 4), (ShardId(1), 3), (ShardId(2), 4)]);
        match &a {
            Agreement::Disagreed {
                lowest,
                highest,
                behind,
                ahead,
            } => {
                assert_eq!((*lowest, *highest), (3, 4));
                assert_eq!(*behind, ShardId(1));
                assert_eq!(*ahead, ShardId(0));
            }
            other => panic!("expected disagreement, got {other:?}"),
        }
        let err = a.check().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("shard_1"), "{msg}");
        assert!(
            msg.contains('3') && msg.contains('4'),
            "the message must name both versions: {msg}"
        );
    }

    #[test]
    fn no_shards_agree_vacuously() {
        assert_eq!(Agreement::of(&[]), Agreement::Agreed(0));
        assert!(Agreement::of(&[]).check().is_ok());
    }

    #[test]
    fn one_shard_always_agrees_with_itself() {
        assert_eq!(Agreement::of(&[(ShardId(7), 9)]), Agreement::Agreed(9));
    }

    #[test]
    fn the_version_survives_a_round_trip_through_the_header() {
        // It lives in the database header page, which is what makes it replicate for free
        // under physical replication rather than needing bookkeeping of its own.
        let conn = Connection::open_in_memory().unwrap();
        assert_eq!(
            version_of(&conn).unwrap(),
            0,
            "a fresh database starts at 0"
        );
        set_version(&conn, 7).unwrap();
        assert_eq!(version_of(&conn).unwrap(), 7);
    }

    #[test]
    fn the_version_is_a_property_of_the_file_not_the_connection() {
        // If it were per-connection, a follower would report whatever its own connection last
        // set rather than what the replicated pages say.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("v.db");
        {
            let conn = Connection::open(&path).unwrap();
            set_version(&conn, 42).unwrap();
        }
        let reopened = Connection::open(&path).unwrap();
        assert_eq!(version_of(&reopened).unwrap(), 42);
    }
}
