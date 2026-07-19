use std::fmt;

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
