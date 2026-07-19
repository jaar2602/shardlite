use std::fmt;
use std::time::Duration;

/// Which kind of connection a [`PragmaProfile`] configures.
///
/// The two roles are not symmetric: only the writer may set persistent database
/// properties, and only the writer can create the `-wal`/`-shm` sidecar files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Writer,
    Reader,
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Role::Writer => write!(f, "writer"),
            Role::Reader => write!(f, "reader"),
        }
    }
}

/// `PRAGMA synchronous` levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Synchronous {
    Off = 0,
    Normal = 1,
    Full = 2,
    Extra = 3,
}

impl Synchronous {
    pub fn as_i64(self) -> i64 {
        self as i64
    }

    pub(crate) fn as_keyword(self) -> &'static str {
        match self {
            Synchronous::Off => "OFF",
            Synchronous::Normal => "NORMAL",
            Synchronous::Full => "FULL",
            Synchronous::Extra => "EXTRA",
        }
    }
}

/// Sizing for the reader pool.
#[derive(Debug, Clone)]
pub struct ReaderPoolConfig {
    /// Reader threads, each owning one connection.
    ///
    /// Not `num_cpus`: WAL reads are CPU and page-cache bound, so oversubscribing buys
    /// context switches and multiplies `cache_size` by the thread count. On the floor
    /// profile — one core shared by three containers — a floor of 4 would be pure churn.
    pub threads: usize,

    /// Bounded queue depth. When it fills, queries are **rejected** rather than queued,
    /// so overload surfaces as a fast error instead of unbounded memory and latency.
    pub queue_capacity: usize,

    /// Per-query wall-clock cap.
    ///
    /// Bounding reader lifetime is not just about latency: a checkpoint cannot advance
    /// past the end mark of any active reader, so a pathological query left running can
    /// stall checkpointing and grow the WAL without limit (step 4).
    pub query_timeout: Duration,
}

impl ReaderPoolConfig {
    pub fn floor() -> Self {
        Self {
            threads: 2,
            queue_capacity: 256,
            query_timeout: Duration::from_secs(30),
        }
    }
}

/// Thresholds for the WAL checkpoint escalation ladder.
///
/// The limits are cut well below what a normal-hardware design would use, to fit the
/// floor profile's disk and page-cache budget.
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Below this WAL file size, don't checkpoint at all.
    pub soft_limit_bytes: u64,

    /// Above this, and with passive attempts stalling, escalate to a blocking `TRUNCATE`.
    pub hard_limit_bytes: u64,

    /// Consecutive passive attempts that failed to fully drain before escalating.
    pub stall_threshold: u32,

    /// Amortizes the `stat` syscall — only check every Nth batch.
    pub check_every_batches: u64,

    /// Cap on the exponential backoff applied while checkpoints are stalling.
    ///
    /// A stalled `PASSIVE` attempt still costs O(WAL size) to discover it cannot advance,
    /// so retrying at a fixed interval while a reader pins a snapshot burns CPU in
    /// proportion to a WAL that is itself growing. Backing off keeps that bounded.
    pub max_stall_backoff: u64,
}

impl CheckpointConfig {
    pub fn floor() -> Self {
        Self {
            soft_limit_bytes: 16 * 1024 * 1024,
            hard_limit_bytes: 128 * 1024 * 1024,
            stall_threshold: 5,
            check_every_batches: 16,
            max_stall_backoff: 64,
        }
    }
}

/// The complete PRAGMA configuration for one connection.
///
/// Numeric values here are starting points to be moved by benchmark data. The durable
/// content is *which* settings are configured and why — see `storage::pragma`.
#[derive(Debug, Clone)]
pub struct PragmaProfile {
    pub role: Role,

    /// Passed to `PRAGMA cache_size` verbatim. A negative value means KiB rather than
    /// pages, which is what we want — page counts make the memory budget depend on
    /// `page_size`.
    pub cache_size: i64,

    /// `PRAGMA mmap_size`. Kept at 0: in a container, memory-mapped pages count against
    /// the cgroup limit, so mmap that looks free on a bare host is not free here. It
    /// also turns I/O errors into SIGBUS rather than a returned error code.
    pub mmap_size: i64,

    pub busy_timeout_ms: i64,

    /// Foreign key enforcement changes whether a statement succeeds, so it must be
    /// identical on every connection on every node. Treat it as a schema property
    /// rather than a tuning knob.
    pub foreign_keys: bool,

    pub synchronous: Synchronous,

    /// `PRAGMA soft_heap_limit`, in bytes. `0` means "leave it alone".
    ///
    /// Despite being spelled as a PRAGMA this is **process-global**, not per-connection,
    /// so only the writer profile sets it — otherwise every connection would fight over
    /// one value, last writer winning. It exists to bound `temp_store = MEMORY`: without
    /// it a large `CREATE INDEX` or an unbounded `ORDER BY` builds its temp b-tree in RAM
    /// and OOMs the container.
    pub soft_heap_limit: i64,
}

impl PragmaProfile {
    /// Writer profile for the floor deployment: ~150 MB container sharing one CPU.
    ///
    /// `synchronous = FULL` because the WAL is the only durable record of a commit.
    /// Under `NORMAL`, a power loss can drop a transaction that was already shipped to
    /// replicas, leaving the primary *behind* its own followers.
    pub fn writer_floor() -> Self {
        Self {
            role: Role::Writer,
            cache_size: -8_192, // 8 MiB
            mmap_size: 0,
            busy_timeout_ms: 5_000,
            foreign_keys: true,
            synchronous: Synchronous::Full,
            soft_heap_limit: 48 * 1024 * 1024,
        }
    }

    /// Profile for the statement-classification connection.
    ///
    /// This connection only ever calls `prepare` to ask SQLite whether a statement is
    /// read-only; it never steps a query. So it gets a deliberately tiny page cache —
    /// paying a full reader's 4 MiB for something that never reads a data page would be
    /// waste at a 150 MB budget.
    pub fn classifier_floor() -> Self {
        Self {
            cache_size: -256, // 256 KiB
            ..Self::reader_floor()
        }
    }

    /// Reader profile for the floor deployment. Sized per connection — multiply by the
    /// reader thread count when budgeting.
    pub fn reader_floor() -> Self {
        Self {
            role: Role::Reader,
            cache_size: -4_096, // 4 MiB
            mmap_size: 0,
            busy_timeout_ms: 5_000,
            foreign_keys: true,
            synchronous: Synchronous::Full,
            soft_heap_limit: 0, // process-global; the writer owns it
        }
    }
}
