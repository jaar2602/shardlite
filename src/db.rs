//! A facade over a sharded database: classify a statement, then route it.
//!
//! There is exactly one storage path — [`crate::shard::ShardManager`]. A single-database
//! deployment is simply `shard_count = 1`, not a second implementation.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rusqlite::Connection;

use crate::config::PragmaProfile;
use crate::error::{Error, Result};
use crate::shard::manifest::Manifest;
use crate::shard::reader_fleet::ReaderFleetStats;
use crate::shard::writer_fleet::WriterFleetStats;
use crate::shard::{ShardConfig, ShardId, ShardManager};
use crate::storage::exec::Outcome;

pub struct Db {
    shards: ShardManager,
    /// Used only to ask SQLite whether a statement is read-only, never to run one.
    ///
    /// Classification must happen *before* routing, so it cannot borrow a fleet connection
    /// — that would occupy a reader for every write too. Points at shard 0, whose schema
    /// matches every other shard because DDL is applied to all of them.
    classifier: Mutex<Connection>,
    dir: PathBuf,
}

impl Db {
    /// Open (or create) a meshdb data directory.
    ///
    /// `shard_count` is recorded in the manifest on creation and refused if it disagrees
    /// with existing data.
    pub fn open(dir: &Path, shard_count: u32) -> Result<Self> {
        let cfg = ShardConfig {
            shard_count,
            ..ShardConfig::floor()
        };
        let shards = ShardManager::open(dir, cfg)?;

        // The classifier is read-only and so cannot create its file; make sure shard 0
        // exists first.
        shards.ensure_open(ShardId::FIRST)?;
        let path = ShardId::FIRST.path(dir);
        let classifier =
            crate::storage::open::open_reader_existing(&path, &PragmaProfile::classifier_floor())?;

        Ok(Self {
            shards,
            classifier: Mutex::new(classifier),
            dir: dir.to_path_buf(),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn manifest(&self) -> &Manifest {
        self.shards.manifest()
    }

    pub fn shard_count(&self) -> u32 {
        self.shards.config().shard_count
    }

    pub fn shards(&self) -> &ShardManager {
        &self.shards
    }

    pub fn writer_stats(&self) -> WriterFleetStats {
        self.shards.writer_stats()
    }

    pub fn reader_stats(&self) -> ReaderFleetStats {
        self.shards.reader_stats()
    }

    /// Execute one statement against `shard`, routed by whether SQLite considers it
    /// read-only.
    ///
    /// DDL is **not** routed here — a schema change must reach every shard, so use
    /// [`Self::run_all`].
    pub fn run_on(&self, shard: ShardId, sql: &str) -> Result<Outcome> {
        reject_unsupported(sql)?;

        // PRAGMA reports as a write even when it only reads, so route it to a reader
        // explicitly. `query_only = ON` there means a PRAGMA that tries to *set* something
        // fails with a clear error rather than silently applying to one connection nobody
        // else uses.
        if first_keyword(sql) == "PRAGMA" {
            return self.shards.query(shard, sql);
        }

        if self.is_readonly(sql)? {
            self.shards.query(shard, sql)
        } else {
            self.shards.execute_one(shard, sql)
        }
    }

    /// Apply a statement to **every** shard. This is how DDL is propagated.
    ///
    /// There is no atomicity across shards, so a partial failure leaves shards with
    /// differing schemas. Per-shard outcomes are returned rather than collapsed for
    /// exactly that reason.
    pub fn run_all(&self, sql: &str) -> Result<Vec<(ShardId, Outcome)>> {
        reject_unsupported(sql)?;
        self.shards.execute_all_shards(sql)
    }

    /// True when a statement changes schema and therefore belongs on every shard.
    pub fn is_ddl(sql: &str) -> bool {
        matches!(first_keyword(sql).as_str(), "CREATE" | "DROP" | "ALTER")
    }

    /// Ask SQLite to classify the statement rather than pattern-matching its text.
    ///
    /// A read-only connection can still *prepare* a write statement — `query_only` only
    /// blocks execution — which is what makes classification-before-routing possible. A
    /// prepare failure is a genuine error (syntax, unknown table); route it to the writer
    /// so the failure is reported through the normal path with its real message.
    fn is_readonly(&self, sql: &str) -> Result<bool> {
        let conn = self
            .classifier
            .lock()
            .map_err(|_| Error::ClassifierPoisoned)?;
        Ok(match conn.prepare(sql) {
            Ok(stmt) => stmt.readonly(),
            Err(_) => false,
        })
    }
}

/// The leading keyword, upper-cased.
///
/// Deliberately crude: this is a routing hint and a guard, not a parser. The real statement
/// guard is an authorizer callback (step 8), which sees what SQLite actually resolved
/// rather than what the text looks like. Notably this does not see past a leading comment.
pub fn first_keyword(sql: &str) -> String {
    sql.trim_start()
        .split(|c: char| c.is_whitespace() || c == '(' || c == ';')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase()
}

/// Reject statements this design cannot honour, with a reason.
///
/// Transaction control is the important case, and it is a trap rather than an oversight:
/// SQLite reports `BEGIN` as **read-only**, so without this guard it would route to a
/// reader connection, appear to succeed, and do nothing at all.
fn reject_unsupported(sql: &str) -> Result<()> {
    let reason = match first_keyword(sql).as_str() {
        "BEGIN" | "COMMIT" | "END" | "ROLLBACK" | "SAVEPOINT" | "RELEASE" => {
            "the writer owns transaction boundaries. A client-held transaction would pin \
             the writer thread for the duration of a client round trip and defeat group \
             commit. Send statements individually."
        }
        "ATTACH" | "DETACH" => {
            "attached databases are not supported. WAL mode gives no atomic commit across \
             attached databases, so a transaction spanning them would not be atomic."
        }
        "VACUUM" => {
            "VACUUM cannot run inside a transaction, and every batch is wrapped in one. \
             It will be offered as a maintenance operation instead."
        }
        _ => return Ok(()),
    };
    Err(Error::Unsupported(format!(
        "{}: {reason}",
        first_keyword(sql)
    )))
}
