//! Statement execution, result types, and error classification.

use rusqlite::Connection;
use rusqlite::types::ValueRef;

/// A SQLite value, owned so it can cross a channel between threads or a network.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    fn from_ref(v: ValueRef<'_>) -> Self {
        match v {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(i) => Value::Integer(i),
            ValueRef::Real(f) => Value::Real(f),
            ValueRef::Text(t) => Value::Text(String::from_utf8_lossy(t).into_owned()),
            ValueRef::Blob(b) => Value::Blob(b.to_vec()),
        }
    }

    /// Human-readable rendering for the CLI.
    pub fn render(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Integer(i) => i.to_string(),
            Value::Real(f) => f.to_string(),
            Value::Text(t) => t.clone(),
            Value::Blob(b) => format!("<blob {} bytes>", b.len()),
        }
    }
}

impl rusqlite::ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::ToSqlOutput;
        Ok(match self {
            Value::Null => ToSqlOutput::Borrowed(ValueRef::Null),
            Value::Integer(i) => ToSqlOutput::Borrowed(ValueRef::Integer(*i)),
            Value::Real(f) => ToSqlOutput::Borrowed(ValueRef::Real(*f)),
            Value::Text(t) => ToSqlOutput::Borrowed(ValueRef::Text(t.as_bytes())),
            Value::Blob(b) => ToSqlOutput::Borrowed(ValueRef::Blob(b)),
        })
    }
}

/// A statement and its bound parameters.
///
/// Parameters are **bound**, never interpolated into the SQL text. Two reasons, and the
/// second is the one that is easy to overlook:
///
/// 1. String-formatted values are an injection hazard the moment any input is untrusted.
/// 2. Replication needs to rewrite non-deterministic calls — `random()`, `datetime('now')`
///    — into fixed values decided once by the primary. Binding is how those values travel
///    without re-parsing or re-quoting the statement, so the parameter channel has to exist
///    before that work can start.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct Statement {
    pub sql: String,
    pub params: Vec<Value>,
}

impl Statement {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
        }
    }

    pub fn with_params(sql: impl Into<String>, params: Vec<Value>) -> Self {
        Self {
            sql: sql.into(),
            params,
        }
    }
}

impl From<&str> for Statement {
    fn from(sql: &str) -> Self {
        Self::new(sql)
    }
}

impl From<String> for Statement {
    fn from(sql: String) -> Self {
        Self::new(sql)
    }
}

impl From<&String> for Statement {
    fn from(sql: &String) -> Self {
        Self::new(sql.clone())
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WriteOutcome {
    pub rows_affected: u64,
    pub last_insert_rowid: i64,
}

/// What a single successfully-executed statement produced.
///
/// Both arms can come from a write: `INSERT ... RETURNING` yields rows.
#[derive(Debug, Clone)]
pub enum Executed {
    Rows(QueryResult),
    Changed(WriteOutcome),
}

/// The result of one request within a batch.
#[derive(Debug, Clone)]
pub enum Outcome {
    Ok(Executed),
    /// A deterministic failure — bad SQL, constraint violation. The request is rolled
    /// back to its savepoint; the rest of the batch still commits.
    Rejected(String),
}

/// Why the distinction matters: a [`SqlError::Logic`] failure rolls back one savepoint
/// and the batch continues, while a [`SqlError::Fatal`] failure aborts the whole
/// transaction. Misclassifying a fatal error as logic would commit a batch on a machine
/// whose disk is full or whose database is corrupt.
#[derive(Debug)]
pub enum SqlError {
    Logic(String),
    Fatal(rusqlite::Error),
}

/// Classify a rusqlite error as a property of the *data* (logic) or of the *machine* (fatal).
///
/// Every known code is listed explicitly rather than grouped by a catch-all, so the
/// intent of each is reviewable. `ErrorCode` is `#[non_exhaustive]`, so a wildcard arm is
/// mandatory and the compiler cannot force a decision when SQLite adds a code — the
/// wildcard therefore **fails closed to `Fatal`**. Treating an unrecognized error as a
/// machine fault costs an aborted batch; treating it as a logic error would commit one.
pub fn classify(e: rusqlite::Error) -> SqlError {
    use rusqlite::ffi::ErrorCode as C;

    let code = match &e {
        rusqlite::Error::SqliteFailure(err, _) => err.code,
        // Non-SQLite-level errors (bad parameter count, invalid column type, statement
        // returned rows where none expected). All deterministic given the same input.
        _ => return SqlError::Logic(e.to_string()),
    };

    match code {
        // Deterministic functions of the schema and data: every node given the same
        // database and the same statement produces these identically.
        //
        // `Unknown` is SQLITE_ERROR despite the name — syntax errors, "no such table",
        // "no such column" all arrive here. It is the single most common logic error.
        C::Unknown
        | C::ConstraintViolation
        | C::TypeMismatch
        | C::TooBig
        | C::ParameterOutOfRange
        | C::AuthorizationForStatementDenied => SqlError::Logic(e.to_string()),

        // Properties of this machine or process, not of the data. Another node with more
        // disk, more memory, or an uncorrupted file would succeed. Committing a batch
        // after one of these is how a replica silently stops matching its primary.
        C::InternalMalfunction
        | C::PermissionDenied
        | C::OperationAborted
        | C::DatabaseBusy
        | C::DatabaseLocked
        | C::OutOfMemory
        | C::ReadOnly
        | C::OperationInterrupted
        | C::SystemIoFailure
        | C::DatabaseCorrupt
        | C::NotFound
        | C::DiskFull
        | C::CannotOpen
        | C::FileLockingProtocolFailed
        | C::SchemaChanged
        | C::ApiMisuse
        | C::NoLargeFileSupport
        | C::NotADatabase => SqlError::Fatal(e),

        // Fail closed: an unrecognized code is assumed to be a machine fault.
        _ => SqlError::Fatal(e),
    }
}

/// Execute one statement, returning either its rows or its change counts.
pub fn run(conn: &Connection, statement: &Statement) -> Result<Executed, SqlError> {
    let mut stmt = conn.prepare(&statement.sql).map_err(classify)?;
    let params = rusqlite::params_from_iter(statement.params.iter());

    if stmt.column_count() == 0 {
        stmt.execute(params).map_err(classify)?;
        return Ok(Executed::Changed(WriteOutcome {
            rows_affected: conn.changes(),
            last_insert_rowid: conn.last_insert_rowid(),
        }));
    }

    let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
    let width = columns.len();

    let mut rows = Vec::new();
    let mut cursor = stmt.query(params).map_err(classify)?;
    while let Some(row) = cursor.next().map_err(classify)? {
        let mut out = Vec::with_capacity(width);
        for i in 0..width {
            out.push(Value::from_ref(row.get_ref(i).map_err(classify)?));
        }
        rows.push(out);
    }

    Ok(Executed::Rows(QueryResult { columns, rows }))
}
