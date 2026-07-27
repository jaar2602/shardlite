//! Virtual shards: routing, sizing, and bounded connection management.
//!
//! # Why sharding exists here
//!
//! Its headline job is multi-write — many shards, each single-writer, so a cluster accepts
//! concurrent writes on several nodes. But the job that matters more at scale is **bounding
//! the size of every operational unit**. A follower joining with a 300 GB database needs a
//! streaming copy taking hours, during which the primary must retain every WAL frame since
//! the copy began. Per-shard, bootstrap, checkpointing, verification, and backup all stay
//! minutes-scale no matter how large the total.
//!
//! # Legacy fixed routing and dynamic linear routing
//!
//! Legacy modulo directories record an immutable active shard count; changing it would re-route
//! every key and is refused. Dynamic directories use the manifest count as a local allocation
//! ceiling and keep the active `LinearV1` state in the quorum catalog. They grow one shard at a
//! time by splitting exactly one linear-hash bucket; whole-shard rebalancing moves the resulting
//! files between nodes without changing key routes.
//!
//! # Memory: an LRU multiplies caches
//!
//! Only recently-used shards hold open connections. The consequence is easy to miss:
//! resident page cache is `per_connection_cache × lru_capacity × threads`, not
//! `per_connection_cache`. At the single-database profile's 8 MiB writer cache, 16 open
//! connections would be 128 MB — the whole container budget. Sharded profiles therefore use
//! much smaller per-connection caches; see [`crate::config::PragmaProfile::writer_shard`].

pub mod classes;
pub mod lru;
pub mod manifest;
pub mod mode;
pub mod reader_fleet;
pub mod routing;
pub mod txn;
pub mod writer_fleet;

use std::path::{Path, PathBuf};

pub use manifest::Manifest;
pub use reader_fleet::ReaderFleet;
pub use routing::{LinearV1, ModuloV1, Routing, hash_key};
pub use writer_fleet::{WriterFleet, WriterFleetStats};

/// Permission to write, checked before every batch commits.
///
/// A trait rather than a direct dependency on `cluster::Fence`, so the dependency points one
/// way only — cluster knows about shards, shards do not know about cluster. It also lets a
/// standalone deployment pass nothing at all rather than construct a cluster it does not have.
pub trait WriteGate: Send + Sync {
    /// `Err` means this node may not write **this shard**. Checked before the transaction,
    /// not after.
    ///
    /// Per shard rather than per node because a node leads some shards and follows others —
    /// that is what multi-write means. A node-wide answer would let a node that leads any
    /// shard write every shard.
    fn check_may_write(&self, shard: ShardId) -> crate::Result<()>;
}

/// Identifies one shard — one SQLite file, with one writer.
///
/// `transparent` so it is indistinguishable from a bare `u32` on the wire, matching the
/// `shard: u32` fields the protocol already uses. The newtype is for the type system, not
/// for the byte stream.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct ShardId(pub u32);

impl ShardId {
    pub const FIRST: ShardId = ShardId(0);

    /// `shard_<n>.db` under the data directory.
    pub fn path(self, dir: &Path) -> PathBuf {
        dir.join(format!("shard_{}.db", self.0))
    }
}

impl std::fmt::Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shard_{}", self.0)
    }
}

/// Map a routing key to a shard.
///
/// FNV-1a, implemented here rather than taken from `DefaultHasher`, because
/// `DefaultHasher` is explicitly **not** stable across Rust releases. A hash change would
/// silently re-route every key to a different shard, which for a database is
/// indistinguishable from losing the data.
///
/// This function must never change. If it must, that is a migration, not an edit.
pub fn shard_of(key: &[u8], shard_count: u32) -> ShardId {
    ModuloV1::new(shard_count)
        .expect("shard_count must be at least one")
        .route(key)
}

/// Create a table in the coordinator connection carrying `result`'s columns (dynamically typed,
/// like any SQLite table) and insert its rows. Duplicate column names are disambiguated so
/// `CREATE TABLE` accepts them.
fn load_central_table(
    conn: &rusqlite::Connection,
    table: &str,
    result: &crate::storage::exec::QueryResult,
) -> crate::Result<()> {
    let mut used: Vec<String> = Vec::new();
    let cols: Vec<String> = result
        .columns
        .iter()
        .map(|c| {
            let mut name = c.clone();
            let mut i = 1;
            while used.iter().any(|u| u.eq_ignore_ascii_case(&name)) {
                name = format!("{c}_{i}");
                i += 1;
            }
            used.push(name.clone());
            quote_ident(&name)
        })
        .collect();

    let create = format!("CREATE TABLE {} ({})", quote_ident(table), cols.join(", "));
    conn.execute(&create, [])?;

    if result.rows.is_empty() {
        return Ok(());
    }
    let placeholders = vec!["?"; result.columns.len()].join(", ");
    let insert = format!("INSERT INTO {} VALUES ({placeholders})", quote_ident(table));
    let mut stmt = conn.prepare(&insert)?;
    for row in &result.rows {
        stmt.execute(rusqlite::params_from_iter(row.iter()))?;
    }
    Ok(())
}

/// Wrap an identifier in double quotes so any name is a valid SQL identifier.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Fold several write outcomes (a split INSERT, or an all-shard write) into one: sum the rows
/// affected, and surface the first rejection rather than reporting a partial success as a whole one.
fn combine_writes(
    outs: impl Iterator<Item = crate::storage::exec::Outcome>,
) -> crate::storage::exec::Outcome {
    use crate::storage::exec::{Executed, Outcome, WriteOutcome};
    let mut rows_affected = 0u64;
    let mut last_insert_rowid = 0i64;
    for o in outs {
        match o {
            Outcome::Ok(Executed::Changed(w)) => {
                rows_affected += w.rows_affected;
                last_insert_rowid = w.last_insert_rowid;
            }
            Outcome::Ok(Executed::Rows(_)) => {}
            Outcome::Rejected(m) => return Outcome::Rejected(m),
        }
    }
    Outcome::Ok(Executed::Changed(WriteOutcome {
        rows_affected,
        last_insert_rowid,
    }))
}

/// Marker file recording that this database was created in strict mode.
///
/// Kept on disk rather than in [`ShardConfig`] so the setting belongs to the *database*, not to
/// whoever opened it. Acceptance may differ between databases; it must never differ between two
/// connections to the same one.
fn strict_path(dir: &Path) -> PathBuf {
    dir.join("strict")
}

fn load_strict(dir: &Path) -> bool {
    strict_path(dir).exists()
}

/// Record that `dir` is a strict database, before any shard is opened.
///
/// Used by `shardlite init --strict`, so the setting is in place for the first `CREATE TABLE`
/// rather than being applied to a schema already written without it.
pub fn mark_strict(dir: &Path) -> crate::Result<()> {
    std::fs::create_dir_all(dir)
        .map_err(|e| crate::Error::Protocol(format!("creating {}: {e}", dir.display())))?;
    std::fs::write(strict_path(dir), b"strict\n")
        .map_err(|e| crate::Error::Protocol(format!("recording strict mode: {e}")))
}

fn shard_keys_path(dir: &Path) -> PathBuf {
    dir.join("shard_keys.txt")
}

/// Load declared shard keys from `shard_keys.txt` (whitespace-separated `table column` lines,
/// `#` comments). A missing or malformed file yields an empty declaration — the safe default,
/// where joins are refused.
fn load_shard_keys(dir: &Path) -> crate::query::ShardKeys {
    let mut keys = crate::query::ShardKeys::new();
    if let Ok(content) = std::fs::read_to_string(shard_keys_path(dir)) {
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((table, column)) = line.split_once(char::is_whitespace) {
                keys.insert(
                    table.trim().to_ascii_lowercase(),
                    column.trim().to_ascii_lowercase(),
                );
            }
        }
    }
    keys
}

/// Write shard keys atomically (temp file then rename), so a crash cannot leave a torn file.
fn persist_shard_keys(dir: &Path, keys: &crate::query::ShardKeys) -> crate::Result<()> {
    let mut out = String::from("# shardlite co-partitioning: <table> <shard-key column>\n");
    let mut entries: Vec<_> = keys.iter().collect();
    entries.sort();
    for (table, column) in entries {
        out.push_str(table);
        out.push('\t');
        out.push_str(column);
        out.push('\n');
    }
    let path = shard_keys_path(dir);
    let tmp = path.with_extension("txt.tmp");
    std::fs::write(&tmp, out.as_bytes())
        .map_err(|e| crate::Error::Protocol(format!("writing shard keys: {e}")))?;
    std::fs::rename(&tmp, &path)
        .map_err(|e| crate::Error::Protocol(format!("renaming shard keys: {e}")))?;
    Ok(())
}

/// Sizing for a sharded database.
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// Local shard-ID allocation ceiling, immutable in the manifest. It is also the active count
    /// for legacy modulo directories; dynamic catalog mode may activate any prefix up to it.
    pub shard_count: u32,

    /// Create the database in strict mode: refuse statements where shardlite would otherwise
    /// choose on the user's behalf. Recorded on disk at creation and sticky thereafter, so it
    /// belongs to the database rather than to whoever opened it.
    pub strict: bool,

    /// Writer threads. Shards are assigned to threads by `id % writer_threads`, so each
    /// shard still has exactly one writer and single-writer-per-file is preserved while the
    /// thread count stays bounded independent of shard count.
    pub writer_threads: usize,

    /// Open writer connections **per thread**. Multiply by `writer_threads` when budgeting.
    pub open_writers_per_thread: usize,

    pub reader_threads: usize,

    /// Open reader connections **per thread**. Multiply by `reader_threads`.
    pub open_readers_per_thread: usize,

    /// Depth of each writer thread's submission queue.
    ///
    /// Bounded for the same reason the read queue is: an unbounded channel does not remove
    /// a backlog, it holds it in memory and turns overload into climbing latency and an
    /// eventual OOM.
    ///
    /// Note what this does **not** currently protect against. Callers block until their
    /// batch commits, so in-flight requests can never exceed the number of concurrent
    /// callers — a 1024-deep queue cannot fill from 32 threads. The bound only starts
    /// doing real work when callers stop blocking, which is what a network server will do.
    /// It is here now because adding it later would change the API of every write path.
    pub write_queue_depth: usize,

    /// Capture committed WAL frames and hand them to a [`crate::replication::FrameSink`].
    ///
    /// **Off by default.** Capture costs ~2.7% throughput (measured) and, more importantly,
    /// retains frames in memory until a sink consumes them — roughly 1.5x the data volume.
    /// A single-node deployment with no replication target gains nothing and pays both, so
    /// it stays off until there is something to replicate to.
    pub capture: bool,

    /// Per-shard cap on frames a sink has not yet consumed. 0 disables the cap.
    pub max_retained_bytes: usize,

    /// How large a shard's WAL may grow while a snapshot copy holds off checkpointing.
    ///
    /// The hold is what freezes the file being copied, but it cannot last forever: with
    /// checkpointing suspended the WAL grows unbounded, and filling the disk is worse than
    /// a failed snapshot. Past this the hold is broken and the snapshot invalidated.
    pub snapshot_hold_max_wal: u64,

    /// Upper bound on statements merged into one shard's transaction.
    ///
    /// Bounds memory and worst-case latency; it is **not** a throughput tuning knob — group
    /// commit sizes itself against the fsync cost. Setting it to 1 disables batching, which
    /// exists so a benchmark can attribute the win to batching rather than merely to
    /// serializing writers.
    pub max_batch: usize,

    /// Upper bound on the partial group rows the coordinator will hold while merging a
    /// cross-shard `GROUP BY`. A pathological high-cardinality grouping — grouping by a
    /// near-unique column — would otherwise materialise a group per row on the coordinator;
    /// past this it refuses loudly rather than exhausting memory. Counted during fan-out so the
    /// rows are never collected in the first place. Applies only to grouped fan-outs, and only
    /// on the coordinator; a single-shard query is unaffected.
    pub max_grouped_rows: usize,
}

impl ShardConfig {
    pub const MIN_SHARDS: u32 = 1;
    pub const MAX_SHARDS: u32 = 256;

    /// Sizing for the floor deployment: ~150 MB container sharing one CPU.
    ///
    /// 2 writer + 2 reader threads: with a third of a CPU there is no parallelism to win,
    /// and more threads would only add context switching.
    pub fn floor() -> Self {
        Self {
            shard_count: 64,
            strict: false,
            writer_threads: 2,
            open_writers_per_thread: 8,
            reader_threads: 2,
            open_readers_per_thread: 8,
            write_queue_depth: 1024,
            max_batch: 64,
            max_grouped_rows: 1_000_000,
            capture: false,
            max_retained_bytes: crate::vfs::wal::DEFAULT_MAX_RETAINED_BYTES,
            snapshot_hold_max_wal: 256 * 1024 * 1024,
        }
    }

    /// Which writer thread owns a shard.
    pub fn thread_of(&self, shard: ShardId) -> usize {
        shard.0 as usize % self.writer_threads
    }

    pub fn validate(&self) -> crate::Result<()> {
        if !(Self::MIN_SHARDS..=Self::MAX_SHARDS).contains(&self.shard_count) {
            return Err(crate::Error::ShardConfig(format!(
                "shard_count {} is outside {}..={}",
                self.shard_count,
                Self::MIN_SHARDS,
                Self::MAX_SHARDS
            )));
        }
        if self.writer_threads == 0 || self.reader_threads == 0 {
            return Err(crate::Error::ShardConfig(
                "writer_threads and reader_threads must be at least 1".into(),
            ));
        }
        if self.max_batch == 0 {
            return Err(crate::Error::ShardConfig(
                "max_batch must be at least 1".into(),
            ));
        }
        if self.write_queue_depth == 0 {
            return Err(crate::Error::ShardConfig(
                "write_queue_depth must be at least 1".into(),
            ));
        }
        if self.open_writers_per_thread == 0 || self.open_readers_per_thread == 0 {
            return Err(crate::Error::ShardConfig(
                "per-thread open-connection limits must be at least 1".into(),
            ));
        }
        Ok(())
    }
}

/// A sharded database: routing, writers, and readers over a directory of shard files.
/// Lets a fan-out reach shards this node does not own, so cross-shard reads and routed writes work
/// across hosts. Installed by the networking layer via [`ShardManager::set_peer_router`]; without
/// one — the embedded CLI, or a standalone node — every shard is local and nothing forwards.
///
/// It is deliberately defined here rather than in `net`: `ShardManager` cannot depend on the
/// networking layer (that would be a cycle), so the seam is a trait the net layer implements.
/// Called when this node takes ownership of a shard (a placement handover / failover), so it can
/// recover the shard's data from an archive when it has none locally. The cluster only knows to
/// call it; the deployment layer wires an implementation (e.g. S3 recovery). Implementations must
/// return promptly — do slow work (an S3 download) on their own thread — because this is called
/// from the placement-apply path.
pub trait ShardRecovery: Send + Sync {
    fn on_take_ownership(&self, shard: ShardId);
}

pub trait PeerRouter: Send + Sync {
    /// Whether this node owns `shard` and should run it against its local file.
    fn is_local(&self, shard: ShardId) -> bool;
    /// Run a read on `shard`'s owner node and return its outcome.
    fn query_remote(
        &self,
        shard: ShardId,
        statement: crate::storage::exec::Statement,
    ) -> crate::Result<crate::storage::exec::Outcome>;
    /// Run a write on `shard`'s owner node and return its outcome.
    fn execute_remote(
        &self,
        shard: ShardId,
        statement: crate::storage::exec::Statement,
    ) -> crate::Result<crate::storage::exec::Outcome>;

    /// Apply versioned DDL on one remote shard and return its new schema version.
    ///
    /// This is deliberately separate from an ordinary remote write. SQLite would accept DDL
    /// through that path, but it would not advance shardlite's per-shard schema version and a
    /// partial roll would become invisible to the cross-shard agreement guard.
    fn apply_ddl_remote(
        &self,
        shard: ShardId,
        statement: crate::storage::exec::Statement,
    ) -> crate::Result<i64>;

    /// Run `statement` on each of `shards` (none owned locally), **batched one request per owner
    /// node** rather than one per shard, and return the results aligned with `shards`. This is what
    /// keeps a wide fan-out to one round trip per node instead of one per shard.
    fn fan_out(
        &self,
        shards: &[ShardId],
        statement: &crate::storage::exec::Statement,
        write: bool,
    ) -> Vec<crate::Result<crate::storage::exec::Outcome>>;

    /// Read the schema version of each of `shards` (none owned locally) from its owner, batched
    /// one request per owner node, aligned with `shards`.
    ///
    /// Required rather than defaulted. A default would have to either invent a version — which
    /// makes a cross-node skew look like agreement, the exact failure this exists to catch — or
    /// error, which would break every fan-out on any implementor that forgot it. Both are worse
    /// than a compile error.
    fn schema_versions(&self, shards: &[ShardId]) -> Vec<crate::Result<i64>>;
}

pub struct ShardManager {
    writers: std::sync::Arc<WriterFleet>,
    readers: ReaderFleet,
    cfg: ShardConfig,
    dir: PathBuf,
    manifest: Manifest,
    /// Active logical routing. The writer fleet may be provisioned for more shard IDs than are
    /// currently visible; only this committed routing decides which shards CRUD may address.
    routing: std::sync::RwLock<ActiveRouting>,
    modes: std::sync::Arc<mode::ShardModes>,
    /// Declared co-partitioning: table → shard-key column. Loaded from `shard_keys.json` and used
    /// to allow co-located joins in cross-shard reads.
    shard_keys: std::sync::RwLock<crate::query::ShardKeys>,
    /// Declared table classes: table → sharded / global. Loaded from `table_classes.txt`. A class
    /// is fixed at CREATE TABLE and means the same thing at every shard count, which is what stops
    /// a split from changing what a constraint does.
    table_classes: std::sync::RwLock<classes::TableClasses>,
    /// Refuse statements where shardlite would otherwise choose on the user's behalf. Fixed for
    /// the life of the database — see [`strict_path`].
    strict: bool,
    /// Next cross-shard transaction id, chosen above anything left on disk by a previous run.
    next_txn: std::sync::atomic::AtomicU64,
    /// Serialises cross-shard transactions.
    ///
    /// Two overlapping ones could each hold a shard the other needs to prepare, and neither would
    /// ever finish. Ordering prepares by shard id removes the cycle *between* participants; this
    /// removes it between transactions. The cost is that multi-shard writes do not overlap each
    /// other — single-shard writes, which is nearly all of them, are untouched.
    txn_lock: std::sync::Mutex<()>,
    /// Set once by the networking layer to forward work for shards owned by other nodes. Absent on
    /// a standalone node or the embedded CLI, where every shard is local.
    peer: std::sync::OnceLock<std::sync::Arc<dyn PeerRouter>>,
}

#[derive(Debug, Clone, Copy)]
struct ActiveRouting {
    routing: Routing,
    epoch: u64,
}

impl ShardManager {
    pub fn open(dir: &Path, cfg: ShardConfig) -> crate::Result<Self> {
        Self::open_with_profiles(
            dir,
            cfg,
            crate::config::PragmaProfile::writer_shard(),
            crate::config::PragmaProfile::reader_shard(),
        )
    }

    /// Open with explicit connection profiles.
    ///
    /// Exists so tuning knobs that live on the profile — `synchronous` above all — can be
    /// varied without a second `ShardManager` constructor per experiment.
    /// Open with a replication sink for captured frames.
    pub fn open_with_sink(
        dir: &Path,
        cfg: ShardConfig,
        sink: Option<std::sync::Arc<dyn crate::replication::FrameSink>>,
    ) -> crate::Result<Self> {
        Self::open_full(
            dir,
            cfg,
            crate::config::PragmaProfile::writer_shard(),
            crate::config::PragmaProfile::reader_shard(),
            sink,
            None,
            None,
        )
    }

    /// Open as a cluster member: frames are captured, writes wait for a quorum to hold them,
    /// and a write is refused outright unless this node currently holds leadership.
    ///
    /// Both are `Option` because a standalone deployment has neither, and must not be made to
    /// wait for a quorum of one that never reports or consult a gate that nothing opens.
    pub fn open_clustered(
        dir: &Path,
        cfg: ShardConfig,
        sink: Option<std::sync::Arc<dyn crate::replication::FrameSink>>,
        acks: Option<std::sync::Arc<crate::replication::AckTracker>>,
        gate: Option<std::sync::Arc<dyn WriteGate>>,
    ) -> crate::Result<Self> {
        Self::open_full(
            dir,
            cfg,
            crate::config::PragmaProfile::writer_shard(),
            crate::config::PragmaProfile::reader_shard(),
            sink,
            acks,
            gate,
        )
    }

    pub fn open_with_profiles(
        dir: &Path,
        cfg: ShardConfig,
        writer: crate::config::PragmaProfile,
        reader: crate::config::PragmaProfile,
    ) -> crate::Result<Self> {
        Self::open_full(dir, cfg, writer, reader, None, None, None)
    }

    #[allow(clippy::too_many_arguments)]
    fn open_full(
        dir: &Path,
        cfg: ShardConfig,
        writer: crate::config::PragmaProfile,
        reader: crate::config::PragmaProfile,
        sink: Option<std::sync::Arc<dyn crate::replication::FrameSink>>,
        acks: Option<std::sync::Arc<crate::replication::AckTracker>>,
        gate: Option<std::sync::Arc<dyn WriteGate>>,
    ) -> crate::Result<Self> {
        cfg.validate()?;
        let modes = std::sync::Arc::new(mode::ShardModes::new());
        // Before anything is opened: refuse a shard count that disagrees with the data
        // already on disk. Discovering that later means rows silently unreachable.
        let manifest = Manifest::open_or_create(dir, cfg.shard_count)?;
        let writers = std::sync::Arc::new(WriterFleet::spawn_with_sink(
            dir,
            cfg.clone(),
            writer,
            crate::config::CheckpointConfig::floor(),
            sink,
            acks,
            gate,
            std::sync::Arc::clone(&modes),
        )?);
        let readers = ReaderFleet::spawn(
            dir,
            &cfg,
            &crate::config::ReaderPoolConfig::floor(),
            reader,
            std::sync::Arc::downgrade(&writers),
            std::sync::Arc::clone(&modes),
        )?;
        let shard_keys = std::sync::RwLock::new(load_shard_keys(dir));
        let table_classes = std::sync::RwLock::new(classes::load(dir));
        // Strict is sticky: once a database is strict it stays strict, so a later open cannot
        // quietly relax what the schema was written against.
        let strict = load_strict(dir) || cfg.strict;
        if strict && !load_strict(dir) {
            std::fs::write(strict_path(dir), b"strict\n")
                .map_err(|e| crate::Error::Protocol(format!("recording strict mode: {e}")))?;
        }
        let routing =
            Routing::ModuloV1(ModuloV1::new(cfg.shard_count).expect("validated shard count"));
        let manager = Self {
            writers,
            readers,
            cfg,
            dir: dir.to_path_buf(),
            manifest,
            routing: std::sync::RwLock::new(ActiveRouting { routing, epoch: 0 }),
            modes,
            shard_keys,
            table_classes,
            strict,
            next_txn: std::sync::atomic::AtomicU64::new(txn::next_id_after_recovery(dir)),
            txn_lock: std::sync::Mutex::new(()),
            peer: std::sync::OnceLock::new(),
        };

        // Before serving anything. A cross-shard transaction interrupted by an unclean exit is
        // finished or dropped here, so no read can observe a half-applied one.
        match manager.recover_transactions() {
            Ok(0) => {}
            Ok(n) => tracing::info!(
                resolved = n,
                "recovered interrupted cross-shard transactions"
            ),
            Err(e) => {
                tracing::error!(error = %e, "recovering cross-shard transactions failed");
                return Err(e);
            }
        }

        Ok(manager)
    }

    /// Install the peer router so fan-outs and routed statements reach shards owned by other nodes.
    /// Called once during networked startup; a second call is ignored (the router does not change
    /// over a node's life).
    pub fn set_peer_router(&self, peer: std::sync::Arc<dyn PeerRouter>) {
        let _ = self.peer.set(peer);
    }

    /// Hand `shard`'s file to the replication path.
    ///
    /// Closes every cached connection **before** marking the shard followed, and in that
    /// order: marking first would still leave live handles open, and it is the surviving
    /// handle — not the mark — that corrupts.
    pub fn follow(&self, shard: ShardId) -> crate::Result<()> {
        self.modes.set(shard, mode::ShardMode::Followed);
        // Both fleets. Readers especially: a read-only handle kept across the follower's
        // file rename holds the deleted inode, and once the shard is promoted again that
        // cached connection becomes usable and serves a database frozen at the moment of
        // replacement.
        let closed = self.writers.quiesce(shard)? + self.readers.quiesce(shard)?;
        self.modes.record_handover(closed);
        Ok(())
    }

    /// Take ownership of `shard`'s file back from the replication path.
    ///
    /// The caller must already have stopped replicating this shard. Nothing here can verify
    /// that — a follower mid-apply holds no lock SQLite can see — so it is the promotion
    /// sequence's job, not this method's.
    pub fn lead(&self, shard: ShardId) {
        self.modes.set(shard, mode::ShardMode::Led);
    }

    pub fn mode(&self, shard: ShardId) -> mode::ShardMode {
        self.modes.mode(shard)
    }

    pub fn modes(&self) -> &std::sync::Arc<mode::ShardModes> {
        &self.modes
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Snapshot `shard` to `dest`, returning the `(epoch, lsn)` it represents.
    ///
    /// The copy runs on the calling thread; the writer is occupied only long enough to
    /// checkpoint and freeze the file.
    pub fn snapshot(&self, shard: ShardId, dest: &Path) -> crate::Result<(u64, u64)> {
        self.writers.snapshot(shard, dest)
    }

    /// Rebuild `shard`, reclaiming free pages. See [`WriterFleet::vacuum`] for its cost.
    pub fn vacuum(&self, shard: ShardId) -> crate::Result<()> {
        self.writers.vacuum(shard)
    }

    /// Force a WAL checkpoint on `shard` now, returning `(busy, log_pages, checkpointed)`.
    /// Checkpointing is otherwise automatic; this is an operator's manual trigger.
    pub fn checkpoint(&self, shard: ShardId) -> crate::Result<(i64, i64, i64)> {
        self.writers.checkpoint(shard)
    }

    /// Recover `shard`'s data from S3 into this node's **local** file, then serve it normally — the
    /// failover path for a node that owns a shard it has no (or stale) local data for. It rebuilds
    /// the complete current image from S3 (latest snapshot + replayed change-log) in memory, then
    /// installs it with the same discipline as a snapshot install: write a temp file, close every
    /// cached connection, and swap it in under the shard's apply guard (which bumps the generation
    /// so no reader keeps the old inode) while removing any stale WAL. Returns `(epoch,
    /// snapshot_lsn, change_log_pages)`. Reconstruct-to-local — never a second live view of the
    /// shard, so it cannot diverge from a surviving owner.
    #[cfg(feature = "s3")]
    pub fn recover_shard_from_s3(
        &self,
        shard: ShardId,
        client: &crate::s3::S3Client,
        prefix: &str,
    ) -> crate::Result<(u64, u64, usize)> {
        let (db, epoch, snap_lsn, pages) =
            crate::s3::failover::recover_bytes(client, prefix, shard)?;

        let dest = shard.path(&self.dir);
        let tmp = dest.with_extension("db.s3recover");
        std::fs::write(&tmp, &db)
            .map_err(|e| crate::Error::Protocol(format!("writing recovered shard: {e}")))?;

        // Close every cached connection first — a handle kept across the rename holds the deleted
        // inode and would serve the pre-recovery database forever — then swap under the apply guard.
        let closed = self.writers.quiesce(shard)? + self.readers.quiesce(shard)?;
        self.modes.access(shard).apply(|| -> crate::Result<()> {
            std::fs::rename(&tmp, &dest)
                .map_err(|e| crate::Error::Protocol(format!("installing recovered shard: {e}")))?;
            let _ = std::fs::remove_file(crate::storage::checkpoint::wal_path_for(&dest));
            Ok(())
        })?;
        self.modes.record_handover(closed);

        tracing::info!(%shard, epoch, snap_lsn, pages, "recovered shard from S3");
        Ok((epoch, snap_lsn, pages))
    }

    /// Freeze `shard`'s main file, returning `(epoch, lsn, path)`. Pair with
    /// [`Self::end_snapshot`].
    pub fn begin_snapshot(&self, shard: ShardId) -> crate::Result<(u64, u64, PathBuf)> {
        self.writers.begin_snapshot(shard)
    }

    /// Release the freeze; `false` means the snapshot must be retaken.
    pub fn end_snapshot(&self, shard: ShardId) -> crate::Result<bool> {
        self.writers.end_snapshot(shard)
    }

    /// The highest position handed out for `shard`, or 0 if none.
    pub fn last_lsn(&self, shard: ShardId) -> u64 {
        self.writers
            .positions()
            .map(|p| p.last_allocated(shard))
            .unwrap_or(0)
    }

    /// Attach, replace, or detach (`None`) the frame sink at runtime — e.g. an operator turning S3
    /// archival on from the console. Requires the node to have been opened capture-ready
    /// ([`Self::capture_enabled`]); with capture off there are no frames for a sink to receive.
    pub fn set_sink(&self, sink: Option<std::sync::Arc<dyn crate::replication::FrameSink>>) {
        self.writers.set_sink(sink);
    }

    /// Whether frame capture is on — the precondition for attaching a sink at runtime.
    pub fn capture_enabled(&self) -> bool {
        self.writers.capture_enabled()
    }

    /// Whether a frame sink is currently attached.
    pub fn has_sink(&self) -> bool {
        self.writers.has_sink()
    }

    /// Number of active logical shards, which may be below the local writer fleet's capacity.
    pub fn shard_count(&self) -> u32 {
        self.routing().shard_count()
    }

    pub fn routing(&self) -> Routing {
        self.routing.read().expect("routing lock").routing
    }

    pub fn routing_epoch(&self) -> u64 {
        self.routing.read().expect("routing lock").epoch
    }

    /// Install a committed routing state.
    ///
    /// The local manifest remains the allocation ceiling for this process. Publishing routing
    /// beyond it is refused before any request can reach an unsupported shard ID.
    pub fn set_routing(&self, routing: Routing) -> crate::Result<()> {
        if routing.shard_count() > self.cfg.shard_count {
            return Err(crate::Error::ShardConfig(format!(
                "routing activates {} shards but this directory was provisioned for at most {}",
                routing.shard_count(),
                self.cfg.shard_count
            )));
        }
        self.routing.write().expect("routing lock").routing = routing;
        Ok(())
    }

    /// Install one committed catalog routing snapshot. Epoch and routing move under one lock so a
    /// request cannot observe a new route carrying an old epoch (or the reverse).
    pub fn set_catalog_routing(&self, routing: Routing, epoch: u64) -> crate::Result<()> {
        if epoch == 0 {
            return Err(crate::Error::ShardConfig(
                "catalog routing epoch must be at least one".into(),
            ));
        }
        if routing.shard_count() > self.cfg.shard_count {
            return Err(crate::Error::ShardConfig(format!(
                "routing activates {} shards but this directory was provisioned for at most {}",
                routing.shard_count(),
                self.cfg.shard_count
            )));
        }
        let mut current = self.routing.write().expect("routing lock");
        if epoch < current.epoch {
            return Err(crate::Error::ShardConfig(format!(
                "routing epoch regressed from {} to {epoch}",
                current.epoch
            )));
        }
        if epoch == current.epoch && routing != current.routing {
            return Err(crate::Error::ShardConfig(format!(
                "routing epoch {epoch} has conflicting routing state"
            )));
        }
        current.routing = routing;
        current.epoch = epoch;
        Ok(())
    }

    /// The primary's stream epoch, if capture is on.
    pub fn epoch(&self) -> Option<u64> {
        self.writers.positions().map(|p| p.epoch())
    }

    /// Bring a shard's file into existence if it does not yet exist.
    pub fn ensure_open(&self, shard: ShardId) -> crate::Result<()> {
        self.writers.ensure_open(shard)
    }

    /// Wait behind all already queued writes for `shard`, then close its writer handles.
    ///
    /// Used after a transfer fence closes the gate: requests already committing finish, requests
    /// that have not begun fail the gate, and completion of this barrier makes `last_lsn` final.
    pub(crate) fn drain_writes(&self, shard: ShardId) -> crate::Result<()> {
        self.writers.quiesce(shard).map(|_| ())
    }

    pub fn checkpoint_stats(&self) -> crate::storage::CheckpointStats {
        let c = self.writers.checkpoint_counters();
        use std::sync::atomic::Ordering::Relaxed;
        crate::storage::CheckpointStats {
            passive: c.passive.load(Relaxed),
            truncated: c.truncated.load(Relaxed),
            stalls: c.stalls.load(Relaxed),
            failures: c.failures.load(Relaxed),
            wal_bytes: c.wal_bytes.load(Relaxed),
        }
    }

    pub fn config(&self) -> &ShardConfig {
        &self.cfg
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Route a key to its shard.
    pub fn route(&self, key: &[u8]) -> ShardId {
        self.routing().route(key)
    }

    pub fn execute(
        &self,
        shard: ShardId,
        statements: Vec<crate::storage::exec::Statement>,
    ) -> crate::Result<Vec<crate::storage::exec::Outcome>> {
        self.reject_shard_key_updates(&statements)?;
        self.writers.execute(shard, statements)
    }

    /// Apply `statements` as one atomic transaction on `shard`: all commit or none do. The
    /// COMMIT of a client-held transaction.
    pub fn execute_txn(
        &self,
        shard: ShardId,
        statements: Vec<crate::storage::exec::Statement>,
    ) -> crate::Result<Vec<crate::storage::exec::Outcome>> {
        self.reject_shard_key_updates(&statements)?;
        self.writers.execute_atomic(shard, statements)
    }

    /// Stream a read query's rows without materialising the whole result.
    ///
    /// Returns a receiver yielding [`reader_fleet::StreamMsg`]. The memory-safe path for large
    /// results — the caller drains rows as they arrive, and the bounded channel throttles the
    /// reader to match.
    pub fn query_stream(
        &self,
        shard: ShardId,
        sql: impl Into<crate::storage::exec::Statement>,
        depth: usize,
    ) -> crate::Result<std::sync::mpsc::Receiver<reader_fleet::StreamMsg>> {
        self.readers.stream(shard, sql, depth)
    }

    pub fn execute_one(
        &self,
        shard: ShardId,
        sql: impl Into<crate::storage::exec::Statement>,
    ) -> crate::Result<crate::storage::exec::Outcome> {
        let statement = sql.into();
        self.reject_shard_key_updates(std::slice::from_ref(&statement))?;
        // A shard this node does not own is run on its owner. This one seam makes every fan-out
        // built on execute_one (execute_all_shards, run_routed) work across hosts.
        if let Some(peer) = self.peer.get()
            && !peer.is_local(shard)
        {
            return peer.execute_remote(shard, statement);
        }
        self.writers.execute_one(shard, statement)
    }

    fn reject_shard_key_updates(
        &self,
        statements: &[crate::storage::exec::Statement],
    ) -> crate::Result<()> {
        let keys = self.shard_keys();
        for statement in statements {
            if let Some(key) = crate::query::route::shard_key_update(&statement.sql, &keys) {
                return Err(crate::Error::Unsupported(format!(
                    "the shard key column `{key}` cannot be updated; changing it would move a row \
                     between SQLite files outside one transaction"
                )));
            }
        }
        Ok(())
    }

    pub fn query(
        &self,
        shard: ShardId,
        sql: impl Into<crate::storage::exec::Statement>,
    ) -> crate::Result<crate::storage::exec::Outcome> {
        let statement = sql.into();
        // Likewise for reads: a non-owned shard is read from its owner, so query_all_shards and the
        // point reads in run_routed gather the right rows across hosts.
        if let Some(peer) = self.peer.get()
            && !peer.is_local(shard)
        {
            return peer.query_remote(shard, statement);
        }
        self.readers.query(shard, statement)
    }

    /// Answer a read across every shard.
    ///
    /// Refuses anything that cannot be combined correctly rather than approximating it —
    /// see [`crate::query::plan`] for the shapes and the reasoning.
    ///
    /// **Not a consistent snapshot.** Each shard is read at its own moment, so a fan-out can
    /// see a write on one shard and miss a concurrent one on another. There is no
    /// cross-shard atomicity in this design, so that is a property of the answer rather than
    /// a gap to be closed.
    pub fn query_all_shards(&self, sql: &str) -> crate::Result<crate::storage::exec::QueryResult> {
        use crate::query::{merge_results, plan_with};
        use crate::storage::exec::{Executed, Outcome};

        // Checked before a single row is read. A schema change lands one shard at a time, so
        // there is a window where shards disagree — and rows combined across two schemas are
        // not a slower answer, they are a wrong one. Refusing is the only honest response.
        self.schema_agreement()?.check()?;

        self.check_strict(sql)?;

        // Declared co-partitioning is consulted so a co-located join is allowed.
        let plan = {
            let shard_keys = self.shard_keys.read().unwrap();
            plan_with(sql, &shard_keys).map_err(|u| crate::Error::Unsupported(u.to_string()))?
        };

        if self.strict {
            crate::query::strict::check_plan(&plan).map_err(crate::Error::Unsupported)?;
        }

        // A scalar / IN / EXISTS subquery is evaluated globally on its own, its result substituted
        // in, and the rewritten (subquery-free) query re-run. Nested subqueries substitute
        // bottom-up.
        if matches!(&plan, crate::query::Plan::Subqueries) {
            let substituted = crate::query::substitute_subqueries(sql, |subsql| {
                self.query_all_shards(subsql).map_err(|e| e.to_string())
            })
            .map_err(crate::Error::Unsupported)?;
            return self.query_all_shards(&substituted);
        }

        // A set operation evaluates each branch as its own full fan-out, then combines the
        // results on the coordinator — so a branch may itself be an aggregate or grouped query.
        if let crate::query::Plan::SetOp(set_op) = &plan {
            return self.eval_set_op(set_op);
        }

        // A FROM that cannot be pushed down (a derived table or non-co-located join) is answered by
        // materialising each source on the coordinator and running the query there.
        if let crate::query::Plan::Central(central) = &plan {
            return self.eval_central(central);
        }

        // Grouped and post-processed (DISTINCT / OFFSET) queries run a *rewritten* query on each
        // shard; every other plan runs the caller's SQL unchanged.
        let grouped = matches!(&plan, crate::query::Plan::Grouped(_));
        let shard_sql = match &plan {
            crate::query::Plan::Grouped(g) => g.shard_sql.as_str(),
            crate::query::Plan::PostProcess(p) => p.shard_sql.as_str(),
            _ => sql,
        };

        // Fan out the (identical) shard SQL to every shard — remote shards batched one request per
        // owner node — then apply the same per-shard handling to the results in shard order.
        let stmt = crate::storage::exec::Statement::new(shard_sql);
        let mut parts = Vec::with_capacity(self.shard_count() as usize);
        let mut grouped_rows = 0usize;
        for (s, outcome) in self.fan_out_shards(&stmt, false).into_iter().enumerate() {
            match outcome? {
                Outcome::Ok(Executed::Rows(r)) => {
                    // Bound coordinator memory: refuse a runaway grouping before its partial rows
                    // are collected, rather than merging them and risking an OOM.
                    if grouped {
                        grouped_rows += r.rows.len();
                        if grouped_rows > self.cfg.max_grouped_rows {
                            return Err(crate::Error::Unsupported(format!(
                                "the grouped query produced more than {} partial group rows \
                                 across shards, more than the coordinator will hold in memory; \
                                 add a WHERE filter, reduce the group cardinality, or target a \
                                 single shard",
                                self.cfg.max_grouped_rows
                            )));
                        }
                    }
                    parts.push(r);
                }
                // A shard that has not been written to has no table yet; it contributes
                // nothing rather than failing the whole query.
                Outcome::Rejected(msg) if msg.contains("no such table") => {}
                Outcome::Rejected(msg) => {
                    return Err(crate::Error::Unsupported(format!(
                        "shard_{s} rejected the query: {msg}"
                    )));
                }
                Outcome::Ok(Executed::Changed(_)) => {
                    return Err(crate::Error::Unsupported(
                        "only reads can be fanned out across shards".into(),
                    ));
                }
            }
        }

        Ok(merge_results(&plan, parts))
    }

    /// Run `statement` on every shard, returning the outcomes in shard order. Shards this node owns
    /// run locally; shards it does not are gathered from their owners in **one batched request per
    /// owner node** (via the peer router). Without a peer router — standalone — every shard is
    /// local. This is the single place the per-shard-forward cost of a fan-out is collapsed.
    fn fan_out_shards(
        &self,
        statement: &crate::storage::exec::Statement,
        write: bool,
    ) -> Vec<crate::Result<crate::storage::exec::Outcome>> {
        let n = self.shard_count();
        let run_local = |id: ShardId| {
            if write {
                self.writers.execute_one(id, statement.clone())
            } else {
                self.readers.query(id, statement.clone())
            }
        };
        let Some(peer) = self.peer.get() else {
            return (0..n).map(|s| run_local(ShardId(s))).collect();
        };
        let remote: Vec<ShardId> = (0..n)
            .map(ShardId)
            .filter(|&id| !peer.is_local(id))
            .collect();
        let mut remote_results: std::collections::HashMap<
            u32,
            crate::Result<crate::storage::exec::Outcome>,
        > = remote
            .iter()
            .map(|s| s.0)
            .zip(peer.fan_out(&remote, statement, write))
            .collect();
        (0..n)
            .map(|s| {
                let id = ShardId(s);
                if peer.is_local(id) {
                    run_local(id)
                } else {
                    remote_results.remove(&s).unwrap_or_else(|| {
                        Err(crate::Error::Unsupported(format!(
                            "no result for shard {s}"
                        )))
                    })
                }
            })
            .collect()
    }

    /// Evaluate a set operation: fan out each branch independently (so an aggregate or grouped
    /// branch is computed globally), then combine the branch results with SQLite set semantics and
    /// apply the outer ORDER BY / OFFSET / LIMIT. Recursion is one level deep — branches are plain
    /// leaf queries, never themselves set operations.
    fn eval_set_op(
        &self,
        set_op: &crate::query::SetOp,
    ) -> crate::Result<crate::storage::exec::QueryResult> {
        let mut branch_results = Vec::with_capacity(set_op.branches.len());
        for branch in &set_op.branches {
            branch_results.push(self.query_all_shards(branch)?);
        }
        let columns = branch_results
            .first()
            .map(|r| r.columns.clone())
            .unwrap_or_default();

        let mut rows = crate::query::evaluate_set_tree(&set_op.tree, &branch_results);
        crate::query::finalize_rows(&mut rows, &set_op.order_by, set_op.offset, set_op.limit);
        Ok(crate::storage::exec::QueryResult { columns, rows })
    }

    /// Answer a query whose FROM cannot be pushed down: materialise each source (fanned out on its
    /// own, and capped) into a fresh in-memory SQLite, then run the query there. This holds whole
    /// sub-results on the coordinator — the memory-heavy fallback — so each source is bounded by the
    /// same cap as a grouped result.
    fn eval_central(
        &self,
        central: &crate::query::Central,
    ) -> crate::Result<crate::storage::exec::QueryResult> {
        use crate::storage::exec::{self, Executed, SqlError, Statement};

        let conn = rusqlite::Connection::open_in_memory()?;
        for source in &central.sources {
            let result = self.query_all_shards(&source.sql)?;
            if result.rows.len() > self.cfg.max_grouped_rows {
                return Err(crate::Error::Unsupported(format!(
                    "materialising `{}` for central execution exceeded {} rows, too many to hold \
                     on the coordinator; narrow the query or target a single shard",
                    source.table, self.cfg.max_grouped_rows
                )));
            }
            load_central_table(&conn, &source.table, &result)?;
        }

        match exec::run(&conn, &Statement::from(central.central_sql.as_str())) {
            Ok(Executed::Rows(rows)) => Ok(rows),
            Ok(Executed::Changed(_)) => Err(crate::Error::Unsupported(
                "the central query did not return rows".into(),
            )),
            Err(SqlError::Logic(msg)) => Err(crate::Error::Unsupported(msg)),
            Err(SqlError::Fatal(e)) => Err(crate::Error::Sqlite(e)),
        }
    }

    /// Declare a table's shard-key column — the column its rows are routed by. Two tables declared
    /// on their shard keys may be joined in a cross-shard read, because a matching pair
    /// (`a.key = b.key`) then routes to the same shard. This is the app **asserting** how it routes
    /// those tables; shardlite trusts it, since it cannot verify placement. Persisted to
    /// `shard_keys.txt` in the data directory.
    pub fn declare_shard_key(&self, table: &str, column: &str) -> crate::Result<()> {
        let mut keys = self.shard_keys.write().unwrap();
        keys.insert(table.to_ascii_lowercase(), column.to_ascii_lowercase());
        persist_shard_keys(&self.dir, &keys)
    }

    /// Declare how a table is distributed. Fixed once, at `CREATE TABLE` time, and honoured
    /// identically at every shard count — which is what stops a split from changing what the
    /// table's constraints mean. Persisted to `table_classes.txt`.
    ///
    /// Declare this **before** creating the table: the constraint gate consults it, and a `global`
    /// table is exempt because all its rows live in one SQLite file.
    pub fn declare_table_class(
        &self,
        table: &str,
        class: classes::TableClass,
    ) -> crate::Result<()> {
        let mut declared = self.table_classes.write().unwrap();
        declared.insert(table.to_ascii_lowercase(), class);
        classes::persist(&self.dir, &declared)
    }

    /// Apply a write that touches several shards as one transaction: all commit, or none do.
    ///
    /// Every shard prepares — applying its statements but holding the transaction open — before
    /// any shard commits. A rejection anywhere is therefore discovered while everything is still
    /// undoable, which is the case that matters: a constraint violation used to leave the earlier
    /// shards written and still report failure, so a retrying client double-inserted.
    ///
    /// **The decision is fsynced before the first commit.** That single ordering is what lets
    /// recovery tell "nothing committed" from "some may have", and it is why the durable record is
    /// written between the two phases rather than before or after them.
    ///
    /// One participant needs none of this: SQLite's own transaction is already all-or-nothing, so
    /// the coordinator log would buy nothing and cost an fsync. Uniform semantics, not uniform
    /// mechanism.
    fn run_atomic(
        &self,
        participants: Vec<(ShardId, Vec<crate::storage::exec::Statement>)>,
        ddl: bool,
    ) -> crate::Result<crate::storage::exec::Outcome> {
        use crate::shard::writer_fleet::PrepareReply;
        use crate::storage::exec::Outcome;

        let mut participants = participants;
        // Ascending shard order, so two coordinators cannot each hold what the other wants.
        participants.sort_by_key(|(shard, _)| shard.0);
        participants.retain(|(_, statements)| !statements.is_empty());

        match participants.len() {
            0 => {
                return Ok(Outcome::Ok(crate::storage::exec::Executed::Changed(
                    crate::storage::exec::WriteOutcome::default(),
                )));
            }
            1 => {
                let (shard, statements) = participants.pop().expect("one participant");
                let outcomes = if ddl {
                    self.apply_ddl_to(
                        shard,
                        statements.into_iter().next().expect("one statement"),
                    )?;
                    vec![Outcome::Ok(crate::storage::exec::Executed::Changed(
                        crate::storage::exec::WriteOutcome::default(),
                    ))]
                } else {
                    self.writers.execute_atomic(shard, statements)?
                };
                return Ok(combine_writes(outcomes.into_iter()));
            }
            _ => {}
        }

        // Prepare parks a transaction on *this* node's writer fleet. A shard owned by another node
        // cannot be parked without a Prepare/Commit RPC, which is the protocol work this plan
        // defers (see docs/cross-shard-atomicity-plan.md, Phase 3). Until that lands, a fan-out
        // that leaves this node keeps the per-shard path it has always had — non-atomic, and the
        // one place the one-behaviour rule is not yet satisfied.
        if !self.all_participants_local(&participants) {
            tracing::debug!(
                "cross-shard write spans nodes; applying per shard without two-phase commit"
            );
            return self.run_per_shard(participants, ddl);
        }

        let _serialised = self.txn_lock.lock().expect("transaction lock");
        let id = self
            .next_txn
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Phase one. A prepare that is rejected has already rolled itself back, so only the shards
        // that reported Prepared need aborting — but every participant is told, because a shard
        // that was never parked ignores the decision and that is cheaper than tracking which.
        let mut outcomes = Vec::new();
        let mut refusal = None;
        for (shard, statements) in &participants {
            match self.writers.prepare(*shard, id, statements.clone(), ddl) {
                Ok(PrepareReply::Prepared(shard_outcomes)) => outcomes.extend(shard_outcomes),
                Ok(PrepareReply::Rejected(message)) => {
                    refusal = Some(message);
                    break;
                }
                Err(e) => {
                    self.abort_all(&participants);
                    return Err(e);
                }
            }
        }
        if let Some(message) = refusal {
            self.abort_all(&participants);
            return Ok(Outcome::Rejected(message));
        }

        // The ordering invariant: durable decision, then commits, never the other way round.
        let record = txn::TxnRecord {
            id,
            participants: participants
                .iter()
                .map(|(shard, statements)| {
                    (
                        shard.0,
                        statements.iter().map(|s| s.sql.clone()).collect::<Vec<_>>(),
                    )
                })
                .collect(),
            ddl,
            decision: Some(txn::Decision::Commit),
        };
        txn::put(&self.dir, &record)?;

        // A commit that fails here is recoverable, not lost: the decision is on disk, so recovery
        // re-applies wherever the marker is missing.
        let mut commit_error = None;
        for (shard, _) in &participants {
            if let Err(e) = self.writers.decide(*shard, true) {
                tracing::error!(%shard, txn = id, error = %e, "commit of a prepared shard failed");
                commit_error = Some(e);
            }
        }
        if let Some(e) = commit_error {
            return Err(e);
        }

        txn::remove(&self.dir, id)?;
        // "Rows affected" is meaningless for DDL, and summing it over participants would make the
        // reported number depend on the shard count — a divergence in the answer, not just in the
        // work done. One shard or fifty, a schema change reports the same thing.
        if ddl {
            return Ok(Outcome::Ok(crate::storage::exec::Executed::Changed(
                crate::storage::exec::WriteOutcome::default(),
            )));
        }
        Ok(combine_writes(outcomes.into_iter()))
    }

    /// Whether every participating shard's files are on this node.
    fn all_participants_local(
        &self,
        participants: &[(ShardId, Vec<crate::storage::exec::Statement>)],
    ) -> bool {
        match self.peer.get() {
            None => true,
            Some(peer) => participants.iter().all(|(shard, _)| peer.is_local(*shard)),
        }
    }

    /// The pre-two-phase path: apply to each participant in turn. Kept only for fan-outs that
    /// span nodes, and deliberately unchanged so multi-node behaviour is exactly what it was.
    fn run_per_shard(
        &self,
        participants: Vec<(ShardId, Vec<crate::storage::exec::Statement>)>,
        ddl: bool,
    ) -> crate::Result<crate::storage::exec::Outcome> {
        let mut outcomes = Vec::new();
        for (shard, statements) in participants {
            for statement in statements {
                if ddl {
                    self.apply_ddl_to_any(shard, statement)?;
                    outcomes.push(crate::storage::exec::Outcome::Ok(
                        crate::storage::exec::Executed::Changed(
                            crate::storage::exec::WriteOutcome::default(),
                        ),
                    ));
                } else {
                    outcomes.push(self.execute_one(shard, statement)?);
                }
            }
        }
        if ddl {
            return Ok(crate::storage::exec::Outcome::Ok(
                crate::storage::exec::Executed::Changed(
                    crate::storage::exec::WriteOutcome::default(),
                ),
            ));
        }
        Ok(combine_writes(outcomes.into_iter()))
    }

    /// Apply DDL to a shard wherever it lives.
    fn apply_ddl_to_any(
        &self,
        shard: ShardId,
        statement: crate::storage::exec::Statement,
    ) -> crate::Result<()> {
        if let Some(peer) = self.peer.get()
            && !peer.is_local(shard)
        {
            peer.apply_ddl_remote(shard, statement)?;
            return Ok(());
        }
        self.apply_ddl_to(shard, statement)?;
        Ok(())
    }

    fn abort_all(&self, participants: &[(ShardId, Vec<crate::storage::exec::Statement>)]) {
        for (shard, _) in participants {
            if let Err(e) = self.writers.decide(*shard, false) {
                tracing::error!(%shard, error = %e, "rolling back a prepared shard failed");
            }
        }
    }

    /// Finish cross-shard transactions interrupted by an unclean exit.
    ///
    /// A record with no durable decision means no shard committed, because the decision is fsynced
    /// first — so it is dropped. A decided record may have committed anywhere, and SQLite has
    /// already rolled back whatever was merely prepared, so it is **re-applied** wherever the
    /// marker table says it did not land. That is why the record carries the statements.
    pub fn recover_transactions(&self) -> crate::Result<usize> {
        let mut resolved = 0;
        for record in txn::pending(&self.dir) {
            match record.decision {
                None => {
                    tracing::info!(
                        txn = record.id,
                        "undecided transaction dropped; nothing had committed"
                    );
                }
                Some(txn::Decision::Commit) => {
                    for shard in record.shards() {
                        if self.has_applied(shard, record.id)? {
                            continue;
                        }
                        let Some(statements) = record.statements_for(shard) else {
                            continue;
                        };
                        tracing::warn!(%shard, txn = record.id, "re-applying a committed transaction");
                        self.run_atomic(vec![(shard, statements)], record.ddl)?;
                        // The single-participant path does not write the marker, so record it now:
                        // a second recovery must not re-apply this shard again.
                        self.writers.execute_one(
                            shard,
                            crate::storage::exec::Statement::new(format!(
                                "INSERT OR IGNORE INTO {} (id) VALUES ({})",
                                crate::storage::apply::TXN_TABLE,
                                record.id
                            )),
                        )?;
                    }
                }
            }
            txn::remove(&self.dir, record.id)?;
            resolved += 1;
        }
        Ok(resolved)
    }

    /// Whether `shard` already carries the marker for `txn`. A shard with no marker table has
    /// applied nothing through the two-phase path.
    fn has_applied(&self, shard: ShardId, txn: u64) -> crate::Result<bool> {
        let sql = format!(
            "SELECT COUNT(*) FROM {} WHERE id = {txn}",
            crate::storage::apply::TXN_TABLE
        );
        match self
            .readers
            .query(shard, crate::storage::exec::Statement::new(&sql))
        {
            Ok(crate::storage::exec::Outcome::Ok(crate::storage::exec::Executed::Rows(rows))) => {
                Ok(matches!(
                    rows.rows.first().and_then(|r| r.first()),
                    Some(crate::storage::Value::Integer(n)) if *n > 0
                ))
            }
            _ => Ok(false),
        }
    }

    /// Whether this database refuses statements that would leave a choice to shardlite.
    pub fn is_strict(&self) -> bool {
        self.strict
    }

    /// Cross-shard transactions that have not been resolved.
    ///
    /// Normally empty: a record exists only between the durable decision and the last participant's
    /// commit. One that persists means recovery has not finished, and until it does a multi-shard
    /// write may be visible on some shards and not others — which is worth an operator seeing
    /// rather than having to infer from row counts.
    pub fn pending_transactions(&self) -> Vec<txn::TxnRecord> {
        txn::pending(&self.dir)
    }

    /// Apply the strict rules, if this database is strict. A no-op otherwise.
    fn check_strict(&self, sql: &str) -> crate::Result<()> {
        if !self.strict {
            return Ok(());
        }
        crate::query::strict::check(sql, &self.shard_keys(), &self.table_classes())
            .map_err(crate::Error::Unsupported)
    }

    /// Every declared table class.
    pub fn table_classes(&self) -> classes::TableClasses {
        self.table_classes.read().unwrap().clone()
    }

    /// How `table` is distributed, defaulting to sharded.
    pub fn table_class(&self, table: &str) -> classes::TableClass {
        classes::class_of(&self.table_classes.read().unwrap(), table)
    }

    /// The declared shard key for a table, if any.
    pub fn shard_key(&self, table: &str) -> Option<String> {
        self.shard_keys
            .read()
            .unwrap()
            .get(&table.to_ascii_lowercase())
            .cloned()
    }

    /// A snapshot of every declared shard key (table → column).
    pub fn shard_keys(&self) -> crate::query::ShardKeys {
        self.shard_keys.read().unwrap().clone()
    }

    /// Run a statement against **every** shard.
    ///
    /// This is how DDL is applied: a schema change must reach all shards, and there is no
    /// atomicity across them — a partial failure leaves shards with differing schemas and
    /// must be reported, not swallowed.
    /// What every shard on this node says its schema version is.
    pub fn schema_agreement(&self) -> crate::Result<crate::storage::schema::Agreement> {
        let peer = self.peer.get();
        let is_remote = |id: ShardId| peer.is_some_and(|p| !p.is_local(id));

        let mut versions = Vec::with_capacity(self.shard_count() as usize);
        for s in 0..self.shard_count() {
            let id = ShardId(s);
            // Owned by another node: asked of that node below, in one batched call.
            if is_remote(id) {
                continue;
            }
            // Neither led here nor owned elsewhere — a replica copy this node holds. Its
            // owner enforces its own agreement, and a follower's version is whatever the
            // frames have delivered so far, which is not an opinion about the schema.
            if self.modes.mode(id) != mode::ShardMode::Led {
                continue;
            }
            versions.push((id, self.writers.schema(id, None)?));
        }

        // The other half of the cluster. Without this the check sees only the shards this node
        // leads, finds them in perfect agreement with each other, and lets a fan-out merge rows
        // from a node whose schema has moved on — producing a result whose rows do not even
        // agree on their own arity.
        let remote: Vec<ShardId> = (0..self.shard_count())
            .map(ShardId)
            .filter(|&id| is_remote(id))
            .collect();
        if let Some(p) = peer
            && !remote.is_empty()
        {
            for (id, version) in remote.iter().zip(p.schema_versions(&remote)) {
                versions.push((*id, version?));
            }
        }

        // `Agreement::of` takes the min and max, so the order shards were collected in does not
        // matter.
        Ok(crate::storage::schema::Agreement::of(&versions))
    }

    /// A shard's schema version.
    ///
    /// A shard this node only *follows* has no readable connection — the replication path owns the
    /// file — so the answer has to come from its owner. Without that forward, this reports a shard
    /// as broken on every node but one, which breaks the promise the whole design rests on: that a
    /// client may ask any node anything. [`Self::execute_one`] and [`Self::query`] already forward
    /// through the same seam.
    pub fn schema_version(&self, shard: ShardId) -> crate::Result<i64> {
        if let Some(peer) = self.peer.get()
            && !peer.is_local(shard)
        {
            return peer
                .schema_versions(std::slice::from_ref(&shard))
                .pop()
                .unwrap_or_else(|| {
                    Err(crate::Error::Unsupported(format!(
                        "no schema version returned for {shard}"
                    )))
                });
        }
        self.writers.schema(shard, None)
    }

    /// Hash a shard's logical contents, for comparison against another node's copy.
    ///
    /// Refuses a shard being followed: the replication path rewrites those files behind
    /// SQLite's back, and a hash taken across that describes a state that never existed.
    /// Verify a follower by pausing its pull loop and using
    /// [`crate::storage::verify::hash_file`].
    pub fn content_hash(
        &self,
        shard: ShardId,
    ) -> crate::Result<crate::storage::verify::ContentHash> {
        self.modes.check_may_open(shard)?;
        crate::storage::verify::hash_file(&shard.path(&self.dir))
    }

    /// Apply a schema change to one shard.
    ///
    /// The unit a cluster-wide roll is built from: each shard's owner applies its own, so no
    /// node needs to hold every shard to change the schema everywhere.
    pub fn apply_ddl_to(
        &self,
        shard: ShardId,
        ddl: impl Into<crate::storage::exec::Statement>,
    ) -> crate::Result<i64> {
        self.writers.schema(shard, Some(ddl.into()))
    }

    /// Apply a schema change to every shard this node leads, one at a time.
    ///
    /// **Rolling, not atomic** — there is no cross-shard atomicity and there never will be.
    /// Each shard's change takes its turn in that shard's write queue, so writes already in
    /// flight complete before it and later ones queue behind it; no extra pausing is needed
    /// because a serialized writer already does exactly that.
    ///
    /// While the roll is in progress the shards disagree, and
    /// [`Self::query_all_shards`] refuses for the duration rather than combining rows from
    /// two schemas. Single-shard reads and writes are unaffected, which is the point of
    /// rolling rather than pausing the cluster.
    ///
    /// Returns each shard's new version. A partial failure is reported as such: the shards
    /// already changed stay changed, because rolling forward from a known point is
    /// recoverable and silently rolling back is not.
    pub fn apply_ddl(
        &self,
        ddl: impl Into<crate::storage::exec::Statement>,
    ) -> crate::Result<Vec<(ShardId, i64)>> {
        let ddl = ddl.into();
        let mut applied = Vec::new();
        for s in 0..self.shard_count() {
            let id = ShardId(s);
            if self.modes.mode(id) != mode::ShardMode::Led {
                continue;
            }
            match self.writers.schema(id, Some(ddl.clone())) {
                Ok(v) => applied.push((id, v)),
                Err(e) => {
                    tracing::error!(
                        shard = %id,
                        error = %e,
                        applied = applied.len(),
                        "schema change failed part way through; earlier shards keep the change"
                    );
                    return Err(e);
                }
            }
        }
        Ok(applied)
    }

    /// Apply DDL to a single shard, for tests that need to construct a half-applied roll.
    ///
    /// Public because the interesting states are the partial ones, and there is no other way
    /// to reach them deliberately from outside this crate.
    #[doc(hidden)]
    pub fn writer_schema_for_test(&self, shard: ShardId, ddl: &str) {
        self.writers
            .schema(shard, Some(crate::storage::exec::Statement::new(ddl)))
            .expect("applying test DDL");
    }

    pub fn execute_all_shards(
        &self,
        sql: impl Into<crate::storage::exec::Statement>,
    ) -> crate::Result<Vec<(ShardId, crate::storage::exec::Outcome)>> {
        let statement = sql.into();
        if crate::db::Db::is_ddl(&statement.sql) {
            // Judged against the rules for *many* shards, whatever the current count, so the
            // schema means the same thing before and after a split. See `crate::query::ddl`.
            if let Err(why) =
                crate::query::ddl::check_create_table(&statement.sql, &self.table_classes())
            {
                return Err(crate::Error::Unsupported(why));
            }
            // Do not send DDL through the ordinary write fan-out: schema_op is what advances
            // each shard's durable version and makes an interrupted roll observable. This is
            // one request per shard today; schema changes are infrequent and correctness is more
            // important than disguising them as a generic batched write.
            let peer = self.peer.get();
            let mut out = Vec::with_capacity(self.shard_count() as usize);
            for s in 0..self.shard_count() {
                let id = ShardId(s);
                if let Some(peer) = peer
                    && !peer.is_local(id)
                {
                    peer.apply_ddl_remote(id, statement.clone())?;
                } else {
                    self.apply_ddl_to(id, statement.clone())?;
                }
                out.push((
                    id,
                    crate::storage::exec::Outcome::Ok(crate::storage::exec::Executed::Changed(
                        crate::storage::exec::WriteOutcome::default(),
                    )),
                ));
            }
            self.adopt_shard_key_from_ddl(&statement.sql)?;
            return Ok(out);
        }

        // Ordinary writes are batched one request per owner node.
        let mut out = Vec::with_capacity(self.shard_count() as usize);
        for (s, outcome) in self
            .fan_out_shards(&statement, true)
            .into_iter()
            .enumerate()
        {
            out.push((ShardId(s as u32), outcome?));
        }

        self.adopt_shard_key_from_ddl(&statement.sql)?;

        Ok(out)
    }

    /// If `sql` is a `CREATE TABLE` with a single-column primary key, adopt that column as the
    /// table's shard key, so writes route by it automatically with no separate `shardkey`
    /// declaration. An explicit prior declaration wins (it may shard by a non-PK column), so it is
    /// never overwritten; a non-CREATE or a composite/absent PK is a no-op.
    ///
    /// This must run on **every** node that applies the CREATE, not just the coordinator: the shard
    /// key is per-node metadata (`shard_keys.txt`), and a node that does not learn it mis-routes
    /// keyed writes (they fall back to shard 0) and point reads for that table. The coordinator calls
    /// it from [`Self::execute_all_shards`]; a node receiving the DDL as a forwarded `ShardBatch`
    /// calls it from the server handler. Because the key is derived from the CREATE text itself,
    /// every node computes the same one.
    pub fn adopt_shard_key_from_ddl(&self, sql: &str) -> crate::Result<()> {
        if let Some((table, column)) = crate::query::route::primary_key_of(sql)
            && self.shard_key(&table).is_none()
        {
            self.declare_shard_key(&table, &column)?;
        }
        Ok(())
    }

    /// Run one client statement, routing it to the shard(s) that hold its rows — the "just connect
    /// and run SQL" entry point, so a client never names a shard. DDL reaches every shard (and a
    /// CREATE TABLE adopts its primary key as the shard key); a keyed write lands on its shard (a
    /// multi-row INSERT is split); a keyless write that must touch every shard does; a point read
    /// hits one shard, any other read fans out. A statement that cannot be routed safely is refused.
    ///
    /// This operates on the shards this node holds locally, exactly like [`Self::query_all_shards`]
    /// and [`Self::execute_all_shards`] — it does not forward to a remote primary, so it is correct
    /// for a node that holds all shards (standalone, or a full replica), which is the same scope the
    /// cross-shard read layer already assumes.
    pub fn run_routed(
        &self,
        statement: impl Into<crate::storage::exec::Statement>,
    ) -> crate::Result<crate::storage::exec::Outcome> {
        use crate::query::Route;
        use crate::storage::exec::{Executed, Outcome};

        let statement = statement.into();
        crate::db::reject_unsupported(&statement.sql)?;
        self.check_strict(&statement.sql)?;
        let kw = crate::db::first_keyword(&statement.sql);

        // DDL reaches every shard, atomically: a broadcast that failed partway used to leave the
        // earlier shards carrying the new schema and the later ones without it, detected by
        // `schema_agreement` but repairable only by hand.
        if matches!(kw.as_str(), "CREATE" | "DROP" | "ALTER") {
            if let Err(why) =
                crate::query::ddl::check_create_table(&statement.sql, &self.table_classes())
            {
                return Err(crate::Error::Unsupported(why));
            }
            let participants = (0..self.shard_count())
                .map(|s| (ShardId(s), vec![statement.clone()]))
                .collect();
            let outcome = self.run_atomic(participants, true)?;
            self.adopt_shard_key_from_ddl(&statement.sql)?;
            return Ok(outcome);
        }

        let is_write = matches!(kw.as_str(), "INSERT" | "UPDATE" | "DELETE" | "REPLACE");
        match crate::query::route_statement_with(
            &statement.sql,
            &self.shard_keys(),
            &self.table_classes(),
            self.routing(),
        ) {
            Route::One(s) => {
                if is_write {
                    self.execute_one(ShardId(s), statement)
                } else {
                    self.query(ShardId(s), statement)
                }
            }
            // A multi-row INSERT split across shards is one transaction, not a loop of
            // independent writes. See `run_atomic`.
            Route::Split(parts) => self.run_atomic(
                parts
                    .into_iter()
                    .map(|(s, sub)| (ShardId(s), vec![crate::storage::exec::Statement::new(&sub)]))
                    .collect(),
                false,
            ),
            // A keyless UPDATE or DELETE touches every shard and has the identical
            // partial-application problem, so it takes the identical path.
            Route::All => self.run_atomic(
                (0..self.shard_count())
                    .map(|s| (ShardId(s), vec![statement.clone()]))
                    .collect(),
                false,
            ),
            Route::Passthrough => {
                if is_write {
                    // A write to a table with no shard key goes to one shard deterministically
                    // rather than being duplicated across all of them.
                    self.execute_one(ShardId(0), statement)
                } else {
                    let rows = self.query_all_shards(&statement.sql)?;
                    Ok(Outcome::Ok(Executed::Rows(rows)))
                }
            }
            Route::Refuse(msg) => Err(crate::Error::Unsupported(msg)),
        }
    }

    pub fn writer_stats(&self) -> WriterFleetStats {
        self.writers.stats()
    }

    pub fn reader_stats(&self) -> reader_fleet::ReaderFleetStats {
        self.readers.stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_is_stable_and_spread() {
        // Golden values recorded from this implementation. They are not an independent
        // oracle — their job is to fail loudly if the hash ever changes, because that
        // would silently re-route every key in every existing deployment.
        assert_eq!(shard_of(b"user:1", 64), ShardId(43));
        assert_eq!(shard_of(b"user:2", 64), ShardId(30));
        assert_eq!(shard_of(b"", 64), ShardId(37));

        // Distribution should be broad enough to be useful.
        let mut hit = std::collections::HashSet::new();
        for i in 0..5_000 {
            hit.insert(shard_of(format!("key-{i}").as_bytes(), 64));
        }
        assert_eq!(hit.len(), 64, "every shard should receive some keys");
    }

    #[test]
    fn every_shard_maps_to_a_thread_and_threads_are_used() {
        let cfg = ShardConfig::floor();
        let mut per_thread = vec![0usize; cfg.writer_threads];
        for s in 0..cfg.shard_count {
            per_thread[cfg.thread_of(ShardId(s))] += 1;
        }
        assert!(per_thread.iter().all(|n| *n > 0));
        assert_eq!(per_thread.iter().sum::<usize>(), cfg.shard_count as usize);
    }

    #[test]
    fn rejects_an_out_of_range_shard_count() {
        let bad = ShardConfig {
            shard_count: 512,
            ..ShardConfig::floor()
        };
        assert!(bad.validate().is_err());
        assert!(ShardConfig::floor().validate().is_ok());
    }
}
