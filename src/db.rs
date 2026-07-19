//! A single-database facade: routes each statement to the writer or to a reader.
//!
//! The reader here is one connection. The bounded reader *pool* is step 3; this is enough
//! to run SQL end to end and to exercise the writer.

use std::path::{Path, PathBuf};

use rusqlite::Connection;

use crate::config::PragmaProfile;
use crate::error::{Error, Result};
use crate::storage::exec::{self, Outcome, SqlError};
use crate::storage::open::open_reader;
use crate::storage::writer::{Writer, WriterHandle, WriterStats};

pub struct Db {
    writer: Writer,
    reader: Connection,
    path: PathBuf,
}

impl Db {
    /// Open `path`, starting its writer thread and one reader connection.
    pub fn open(path: &Path) -> Result<Self> {
        let (writer, token) = Writer::spawn(path, &PragmaProfile::writer_floor())?;
        let reader = open_reader(path, &PragmaProfile::reader_floor(), &token)?;
        Ok(Self {
            writer,
            reader,
            path: path.to_path_buf(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn stats(&self) -> WriterStats {
        self.writer.stats()
    }

    pub fn handle(&self) -> WriterHandle {
        self.writer.handle()
    }

    /// Execute one statement, routing it by whether SQLite considers it read-only.
    pub fn run(&self, sql: &str) -> Result<Outcome> {
        reject_unsupported(sql)?;

        // PRAGMA reports as a write even when it only reads, so route it to the reader
        // explicitly. `query_only = ON` there means a PRAGMA that tries to *set*
        // something fails with a clear error instead of silently applying to a connection
        // nobody else uses.
        if first_keyword(sql) == "PRAGMA" {
            return self.read(sql);
        }

        // Ask SQLite to classify rather than pattern-matching SQL text. A read-only
        // connection can still *prepare* a write statement, which is what makes this
        // possible before routing. A prepare failure is a real error (syntax, unknown
        // table); let the writer path report it properly.
        let readonly = match self.reader.prepare(sql) {
            Ok(stmt) => stmt.readonly(),
            Err(_) => false,
        };

        if readonly {
            self.read(sql)
        } else {
            self.writer.handle().execute_one(sql)
        }
    }

    fn read(&self, sql: &str) -> Result<Outcome> {
        match exec::run(&self.reader, sql) {
            Ok(executed) => Ok(Outcome::Ok(executed)),
            Err(SqlError::Logic(msg)) => Ok(Outcome::Rejected(msg)),
            Err(SqlError::Fatal(e)) => Err(Error::Sqlite(e)),
        }
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
