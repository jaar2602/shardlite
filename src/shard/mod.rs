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
//! # Virtual shards, fixed at creation
//!
//! Shards are over-provisioned up front and never split. Rebalancing moves whole shards
//! between nodes, so data is never rehashed — that is what lets a deployment start at
//! 100 MB and grow to a few hundred GB without a resharding event.
//!
//! **The shard count is immutable.** Changing it re-routes every key. It is stored in the
//! cluster manifest at creation and refused if it disagrees later.
//!
//! # Memory: an LRU multiplies caches
//!
//! Only recently-used shards hold open connections. The consequence is easy to miss:
//! resident page cache is `per_connection_cache × lru_capacity × threads`, not
//! `per_connection_cache`. At the single-database profile's 8 MiB writer cache, 16 open
//! connections would be 128 MB — the whole container budget. Sharded profiles therefore use
//! much smaller per-connection caches; see [`crate::config::PragmaProfile::writer_shard`].

pub mod lru;
pub mod manifest;
pub mod mode;
pub mod reader_fleet;
pub mod writer_fleet;

use std::path::{Path, PathBuf};

pub use manifest::Manifest;
pub use reader_fleet::ReaderFleet;
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
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut h = OFFSET;
    for b in key {
        h ^= *b as u64;
        h = h.wrapping_mul(PRIME);
    }
    ShardId((h % shard_count as u64) as u32)
}

/// Sizing for a sharded database.
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// Immutable after creation; caps ultimate scale. 64 covers roughly 100 MB to 1 TB.
    pub shard_count: u32,

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
pub struct ShardManager {
    writers: std::sync::Arc<WriterFleet>,
    readers: ReaderFleet,
    cfg: ShardConfig,
    dir: PathBuf,
    manifest: Manifest,
    modes: std::sync::Arc<mode::ShardModes>,
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
        Ok(Self {
            writers,
            readers,
            cfg,
            dir: dir.to_path_buf(),
            manifest,
            modes,
        })
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

    /// The primary's stream epoch, if capture is on.
    /// How many shards this manager owns. Fixed at creation.
    pub fn shard_count(&self) -> u32 {
        self.cfg.shard_count
    }

    pub fn epoch(&self) -> Option<u64> {
        self.writers.positions().map(|p| p.epoch())
    }

    /// Bring a shard's file into existence if it does not yet exist.
    pub fn ensure_open(&self, shard: ShardId) -> crate::Result<()> {
        self.writers.ensure_open(shard)
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
        shard_of(key, self.cfg.shard_count)
    }

    pub fn execute(
        &self,
        shard: ShardId,
        statements: Vec<crate::storage::exec::Statement>,
    ) -> crate::Result<Vec<crate::storage::exec::Outcome>> {
        self.writers.execute(shard, statements)
    }

    /// Apply `statements` as one atomic transaction on `shard`: all commit or none do. The
    /// COMMIT of a client-held transaction.
    pub fn execute_txn(
        &self,
        shard: ShardId,
        statements: Vec<crate::storage::exec::Statement>,
    ) -> crate::Result<Vec<crate::storage::exec::Outcome>> {
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
        self.writers.execute_one(shard, sql)
    }

    pub fn query(
        &self,
        shard: ShardId,
        sql: impl Into<crate::storage::exec::Statement>,
    ) -> crate::Result<crate::storage::exec::Outcome> {
        self.readers.query(shard, sql)
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
        use crate::query::{merge_results, plan};
        use crate::storage::exec::{Executed, Outcome};

        // Checked before a single row is read. A schema change lands one shard at a time, so
        // there is a window where shards disagree — and rows combined across two schemas are
        // not a slower answer, they are a wrong one. Refusing is the only honest response.
        self.schema_agreement()?.check()?;

        let plan = plan(sql).map_err(|u| crate::Error::Unsupported(u.to_string()))?;

        // A grouped query runs a *rewritten* partial-aggregation query on each shard; every other
        // plan runs the caller's SQL unchanged.
        let grouped = matches!(&plan, crate::query::Plan::Grouped(_));
        let shard_sql = match &plan {
            crate::query::Plan::Grouped(g) => g.shard_sql.as_str(),
            _ => sql,
        };

        let mut parts = Vec::with_capacity(self.cfg.shard_count as usize);
        let mut grouped_rows = 0usize;
        for s in 0..self.cfg.shard_count {
            match self.query(ShardId(s), shard_sql)? {
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

    /// Run a statement against **every** shard.
    ///
    /// This is how DDL is applied: a schema change must reach all shards, and there is no
    /// atomicity across them — a partial failure leaves shards with differing schemas and
    /// must be reported, not swallowed.
    /// What every shard on this node says its schema version is.
    pub fn schema_agreement(&self) -> crate::Result<crate::storage::schema::Agreement> {
        let mut versions = Vec::with_capacity(self.cfg.shard_count as usize);
        for s in 0..self.cfg.shard_count {
            let id = ShardId(s);
            // A shard this node does not own cannot be asked, and its version is not this
            // node's business — the owner enforces its own agreement.
            if self.modes.mode(id) != mode::ShardMode::Led {
                continue;
            }
            versions.push((id, self.writers.schema(id, None)?));
        }
        Ok(crate::storage::schema::Agreement::of(&versions))
    }

    /// A shard's schema version.
    pub fn schema_version(&self, shard: ShardId) -> crate::Result<i64> {
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
        for s in 0..self.cfg.shard_count {
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
        let mut out = Vec::with_capacity(self.cfg.shard_count as usize);
        for s in 0..self.cfg.shard_count {
            let id = ShardId(s);
            out.push((id, self.writers.execute_one(id, statement.clone())?));
        }
        Ok(out)
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
