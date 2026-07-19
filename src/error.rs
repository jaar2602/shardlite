use crate::config::Role;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("opening database at {path}: {source}")]
    Open {
        path: String,
        #[source]
        source: rusqlite::Error,
    },

    /// Silently falling back to a rollback journal would break replication entirely,
    /// so this is checked at open and fails startup rather than surfacing later.
    #[error(
        "`PRAGMA journal_mode = WAL` returned `{got}` instead of `wal` for {path}. \
         WAL mode is required. This usually means the file is on a filesystem whose \
         locking SQLite cannot use for WAL (NFS, SMB, or some container volume drivers) \
         — move the database to local disk."
    )]
    WalUnavailable { path: String, got: String },

    /// Several PRAGMAs silently no-op instead of erroring — a read-only connection
    /// cannot change `journal_mode`, and `auto_vacuum` cannot change once tables exist.
    /// Every setting is therefore read back after being applied.
    #[error(
        "PRAGMA {pragma} did not take effect on the {role} connection: expected \
         `{expected}`, got `{actual}`"
    )]
    PragmaMismatch {
        role: Role,
        pragma: &'static str,
        expected: String,
        actual: String,
    },

    #[error("the writer thread is no longer running")]
    WriterGone,

    /// Deliberate backpressure: the bounded write queue is full. Retry, or shed load.
    #[error("writer is busy, the submission queue is full")]
    WriterBusy,

    /// The whole transaction was rolled back, so no request in the batch took effect —
    /// including requests that had already succeeded against their savepoints.
    #[error("transaction aborted, no writes in this batch were applied: {reason}")]
    BatchAborted { reason: String },

    #[error("unsupported statement: {0}")]
    Unsupported(String),

    /// Deliberate backpressure: the bounded read queue is full. Retry, or shed load.
    #[error("reader pool is busy, the query queue is full")]
    ReaderPoolBusy,

    #[error("the reader pool is no longer running")]
    ReaderPoolGone,

    #[error("query exceeded the {timeout:?} time limit and was cancelled")]
    QueryTimeout { timeout: std::time::Duration },

    #[error("the statement classifier is poisoned; a previous caller panicked holding it")]
    ClassifierPoisoned,

    #[error("registering the capture VFS: {0}")]
    VfsRegistration(String),

    #[error("shard configuration: {0}")]
    ShardConfig(String),

    #[error("{0}")]
    Manifest(String),
}
