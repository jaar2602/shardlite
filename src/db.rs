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
    /// Open (or create) a shardlite data directory.
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

    /// Answer a read across every shard, refusing shapes that cannot be combined correctly.
    pub fn query_all(&self, sql: &str) -> Result<crate::storage::exec::QueryResult> {
        reject_unsupported(sql)?;
        self.shards.query_all_shards(sql)
    }

    /// Rebuild a shard, reclaiming free pages.
    ///
    /// Needs roughly twice the shard's size in free disk, and rewrites every page — so with
    /// capture on it produces a replication stream the size of the whole shard.
    pub fn vacuum(&self, shard: ShardId) -> Result<()> {
        self.shards.vacuum(shard)
    }

    /// True when SQLite considers this statement read-only.
    ///
    /// The CLI uses this to decide whether to fan out. Fanning out a write would hand it to
    /// the planner, which refuses non-SELECT statements — so the write would be reported as
    /// unsupported and silently never run.
    pub fn is_read(&self, sql: &str) -> Result<bool> {
        if first_keyword(sql) == "PRAGMA" {
            return Ok(true);
        }
        self.is_readonly(sql)
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
pub(crate) fn reject_unsupported(sql: &str) -> Result<()> {
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
             Use the maintenance path instead — `.vacuum` in the shell, or \
             ShardManager::vacuum."
        }
        // ALTER TABLE with a form SQLite does not support (a MySQL/Postgres habit). SQLite would
        // reject it with a bare "near \"MODIFY\": syntax error"; give the actual limitation and the
        // recreate-table workaround instead.
        "ALTER" => match alter_unsupported_reason(sql) {
            Some(reason) => reason,
            None => return Ok(()),
        },
        _ => return Ok(()),
    };
    Err(Error::Unsupported(format!(
        "{}: {reason}",
        first_keyword(sql)
    )))
}

const ALTER_LIMITATION: &str = "SQLite's ALTER TABLE supports only RENAME TO, RENAME COLUMN, ADD COLUMN and DROP COLUMN — not \
     MODIFY/CHANGE/ALTER COLUMN, and not ADD CONSTRAINT/PRIMARY KEY/FOREIGN KEY/UNIQUE/CHECK/INDEX. \
     To change a column's type or default, or add/drop a constraint or the primary key, recreate \
     the table: CREATE a new table with the schema you want, INSERT ... SELECT the rows across, DROP \
     the old table, then ALTER TABLE ... RENAME the new one into place";

/// If `sql` is an `ALTER TABLE` whose action SQLite cannot perform, the limitation to report;
/// `None` for the four forms SQLite supports (RENAME TO, RENAME COLUMN, ADD COLUMN, DROP COLUMN).
///
/// Checks the action keyword at its position (after `ALTER TABLE [IF EXISTS] <name>`) rather than
/// scanning the whole statement, so a column merely *named* `modify`/`change` is not misread.
fn alter_unsupported_reason(sql: &str) -> Option<&'static str> {
    let tokens = ddl_tokens(sql);
    if tokens.first().map(String::as_str) != Some("ALTER")
        || tokens.get(1).map(String::as_str) != Some("TABLE")
    {
        return None;
    }
    // SQLite's ALTER TABLE is `ALTER TABLE <name> <action> …` — no IF EXISTS, and (ATTACH being
    // refused) no schema-qualified name. So the name is one token and the action is the next.
    match tokens.get(3).map(String::as_str)? {
        "RENAME" | "DROP" => None,
        "ADD" => match tokens.get(4).map(String::as_str) {
            Some("CONSTRAINT" | "PRIMARY" | "FOREIGN" | "UNIQUE" | "CHECK" | "INDEX" | "KEY") => {
                Some(ALTER_LIMITATION)
            }
            _ => None, // ADD [COLUMN] <column-def>
        },
        _ => Some(ALTER_LIMITATION), // MODIFY, CHANGE, ALTER, …
    }
}

/// Upper-cased identifier/keyword tokens outside string and identifier quotes — a crude tokenizer
/// for the DDL guards. A quoted identifier (`"…"`, `` `…` ``, `[…]`) collapses to one placeholder so
/// token positions (the table name after `ALTER TABLE`, the action after it) stay stable; a string
/// literal (`'…'`) contributes nothing.
fn ddl_tokens(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        match c {
            '\'' | '"' | '`' => {
                i += 1;
                while i < bytes.len() && bytes[i] as char != c {
                    i += 1;
                }
                i += 1; // past the closing quote
                if c != '\'' {
                    tokens.push("\0".to_string()); // a quoted identifier is a name token
                }
            }
            '[' => {
                i += 1;
                while i < bytes.len() && bytes[i] as char != ']' {
                    i += 1;
                }
                i += 1;
                tokens.push("\0".to_string());
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric() || bytes[i] as char == '_')
                {
                    i += 1;
                }
                tokens.push(sql[start..i].to_ascii_uppercase());
            }
            _ => i += 1,
        }
    }
    tokens
}

#[cfg(test)]
mod alter_tests {
    use super::{alter_unsupported_reason, reject_unsupported};

    #[test]
    fn sqlite_supported_alter_forms_pass() {
        for sql in [
            "ALTER TABLE t ADD COLUMN b INTEGER",
            "ALTER TABLE t ADD b INTEGER",
            "ALTER TABLE t DROP COLUMN b",
            "ALTER TABLE t DROP b",
            "ALTER TABLE t RENAME TO u",
            "ALTER TABLE t RENAME COLUMN a TO a2",
            "ALTER TABLE \"my table\" ADD COLUMN x INT",
            // A column literally named after a foreign keyword must NOT trip the guard.
            "ALTER TABLE t ADD COLUMN modify TEXT",
            "ALTER TABLE t ADD change TEXT",
            // A table named after a keyword is fine.
            "ALTER TABLE modify ADD COLUMN x INT",
        ] {
            assert!(
                alter_unsupported_reason(sql).is_none(),
                "should be supported: {sql}"
            );
            assert!(reject_unsupported(sql).is_ok(), "should be allowed: {sql}");
        }
    }

    #[test]
    fn foreign_alter_forms_are_refused_with_guidance() {
        for sql in [
            "ALTER TABLE t MODIFY COLUMN a VARCHAR(50)",
            "ALTER TABLE t CHANGE a a2 TEXT",
            "ALTER TABLE t ALTER COLUMN b SET DEFAULT 0",
            "ALTER TABLE t ADD CONSTRAINT chk CHECK (b > 0)",
            "ALTER TABLE t ADD PRIMARY KEY (a)",
            "ALTER TABLE t ADD FOREIGN KEY (a) REFERENCES u(id)",
            "ALTER TABLE t ADD UNIQUE (a)",
            "ALTER TABLE t ADD INDEX idx (a)",
        ] {
            assert!(
                alter_unsupported_reason(sql).is_some(),
                "should be refused: {sql}"
            );
            let err = reject_unsupported(sql).unwrap_err().to_string();
            assert!(
                err.contains("recreate the table"),
                "message should guide the fix: {err}"
            );
        }
    }
}
