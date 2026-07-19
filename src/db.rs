//! A single-database facade: routes each statement to the writer or to the reader pool.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rusqlite::Connection;

use crate::config::{PragmaProfile, ReaderPoolConfig};
use crate::error::{Error, Result};
use crate::storage::exec::Outcome;
use crate::storage::open::open_reader;
use crate::storage::reader::{ReaderPool, ReaderStats};
use crate::storage::writer::{Writer, WriterHandle, WriterStats};

pub struct Db {
    writer: Writer,
    readers: ReaderPool,
    /// Used only to ask SQLite whether a statement is read-only, never to run one.
    ///
    /// It cannot come from the pool: classification has to happen *before* routing, and
    /// borrowing a pool connection to decide where to send the work would occupy a reader
    /// for every write too. Behind a `Mutex` so `Db` stays `Sync` — `Connection` is not.
    classifier: Mutex<Connection>,
    path: PathBuf,
}

impl Db {
    /// Open `path` with the floor profile: one writer thread and a small reader pool.
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with(path, &ReaderPoolConfig::floor())
    }

    pub fn open_with(path: &Path, pool: &ReaderPoolConfig) -> Result<Self> {
        // Order matters and is enforced by the token: the writer materializes the
        // -wal/-shm sidecars that read-only connections cannot create.
        let (writer, token) = Writer::spawn(path, &PragmaProfile::writer_floor())?;
        let readers = ReaderPool::spawn(path, &PragmaProfile::reader_floor(), pool, &token)?;
        let classifier = open_reader(path, &PragmaProfile::classifier_floor(), &token)?;

        Ok(Self {
            writer,
            readers,
            classifier: Mutex::new(classifier),
            path: path.to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn writer_stats(&self) -> WriterStats {
        self.writer.stats()
    }

    pub fn reader_stats(&self) -> ReaderStats {
        self.readers.stats()
    }

    pub fn handle(&self) -> WriterHandle {
        self.writer.handle()
    }

    /// Execute one statement, routing it by whether SQLite considers it read-only.
    pub fn run(&self, sql: &str) -> Result<Outcome> {
        reject_unsupported(sql)?;

        // PRAGMA reports as a write even when it only reads, so route it to a reader
        // explicitly. `query_only = ON` there means a PRAGMA that tries to *set*
        // something fails with a clear error rather than silently applying to one
        // connection nobody else uses.
        if first_keyword(sql) == "PRAGMA" {
            return self.readers.query(sql);
        }

        if self.is_readonly(sql)? {
            self.readers.query(sql)
        } else {
            self.writer.handle().execute_one(sql)
        }
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
/// Deliberately crude: this is a routing hint and a guard, not a parser. The real
/// statement guard is an authorizer callback (step 8), which sees what SQLite actually
/// resolved rather than what the text looks like. Notably this does not see past a
/// leading comment.
fn first_keyword(sql: &str) -> String {
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
