use crate::config::Role;

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// A cheap stand-in when one error must be reported to several waiting callers.
    ///
    /// `Error` is not `Clone` because `rusqlite::Error` is not; this preserves the message.
    pub fn clone_shallow(&self) -> Error {
        match self {
            Error::CaptureOverflow { shard, retained } => Error::CaptureOverflow {
                shard: shard.clone(),
                retained: *retained,
            },
            other => Error::BatchAborted {
                reason: other.to_string(),
            },
        }
    }
}

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

    /// A follower was handed frames it cannot place in its stream.
    #[error(
        "replication gap on {shard}: expected LSN {expected}, got {got} ({detail}). \
         Applying across a gap writes pages without their predecessors, which produces a \
         database that is silently wrong rather than obviously broken, so the shard must be \
         re-bootstrapped instead."
    )]
    ReplicationGap {
        shard: String,
        expected: u64,
        got: u64,
        detail: String,
    },

    #[error(
        "snapshot of {shard} was invalidated: its WAL grew past the hold limit during the \
         copy, so checkpointing had to resume and the file changed underneath. Retake it."
    )]
    SnapshotInvalidated { shard: String },

    /// The replication stream is incomplete; the node must stop accepting writes.
    #[error(
        "capture for {shard} exceeded its retention limit with {retained} bytes unconsumed. \
         Frames are never dropped, so the replication stream is now incomplete and this node \
         would commit data no replica will receive. Writes are refused until the sink \
         recovers and the shard is reopened."
    )]
    CaptureOverflow { shard: String, retained: usize },
}
