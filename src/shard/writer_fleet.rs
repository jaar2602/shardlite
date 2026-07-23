//! Writer threads serving many shards through a bounded connection cache.
//!
//! A shard is assigned to a thread by `id % writer_threads`, so every shard still has
//! exactly one writer — single-writer-per-file is preserved — while the thread count stays
//! bounded no matter how many shards exist. Each thread holds an LRU of open connections,
//! so 64 shards cost the memory of a handful of connections rather than 64.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};
use std::sync::{Mutex, RwLock};
use std::thread::JoinHandle;

use rusqlite::Connection;

use super::lru::Lru;
use super::{ShardConfig, ShardId};
use crate::config::{CheckpointConfig, PragmaProfile};
use crate::error::{Error, Result};
use crate::replication::{FrameSink, Lsn, StreamPositions, StreamTxn};
use crate::storage::apply;
use crate::storage::checkpoint::{Checkpointer, Counters as CheckpointCounters};
use crate::storage::exec::{Outcome, Statement};
use crate::storage::open::open_writer_vfs;
use crate::vfs::WalCapture;

struct Pending {
    shard: ShardId,
    statements: Vec<Statement>,
    /// All-or-nothing: one failing statement voids the whole set. A client transaction; see
    /// [`super::super::storage::apply::batch_with`].
    atomic: bool,
    reply: SyncSender<Result<Vec<Outcome>>>,
}

enum Job {
    Write(Pending),
    /// Ensure a shard's database file exists, so a reader may open it.
    ///
    /// Measured on 3.53.2, and not what was first assumed here:
    ///
    /// - A read-only connection **cannot create the database file**. A shard that has never
    ///   been written to has no file, so a reader alone simply fails. This is the case that
    ///   makes `EnsureOpen` load-bearing.
    /// - A *clean* close checkpoints the WAL into the main file and removes the
    ///   `-wal`/`-shm` sidecars, and a read-only connection opens that file happily. So an
    ///   ordinary LRU eviction does **not** break readers, contrary to the first guess.
    /// - An *unclean* close leaves a `-wal` behind, and a read-only connection may then be
    ///   unable to create the `-shm` it needs. Rarer, but the same fix covers it.
    EnsureOpen {
        shard: ShardId,
        reply: SyncSender<Result<()>>,
    },
    /// Read a shard's schema version, and optionally apply a DDL statement that bumps it.
    ///
    /// Runs on the writer thread like any other write, so it takes its turn in the same FIFO:
    /// everything queued ahead completes first, the change applies, and everything behind
    /// follows. Draining is not extra machinery here — it is what a serialized writer already
    /// does.
    Schema {
        shard: ShardId,
        /// `None` reads the version without changing anything.
        ddl: Option<Statement>,
        reply: SyncSender<Result<i64>>,
    },
    /// Close whatever is cached for a shard, so its file can be handed to the replication
    /// path. A connection left open across raw page writes serves stale rows with no error,
    /// so the handover must close it rather than merely stop using it.
    Quiesce {
        shard: ShardId,
        reply: SyncSender<bool>,
    },
    /// Freeze a shard's main file and report the position it represents.
    BeginSnapshot {
        shard: ShardId,
        reply: SyncSender<Result<(u64, u64, PathBuf)>>,
    },
    /// Release the freeze. Reports whether the snapshot is still valid.
    EndSnapshot {
        shard: ShardId,
        reply: SyncSender<Result<bool>>,
    },
    /// Rebuild a shard, reclaiming free pages.
    Vacuum {
        shard: ShardId,
        reply: SyncSender<Result<()>>,
    },
    /// Force a WAL checkpoint now, reporting `(busy, log_pages, checkpointed)`.
    Checkpoint {
        shard: ShardId,
        reply: SyncSender<Result<(i64, i64, i64)>>,
    },
    Shutdown,
}

/// One open shard: its writer connection, its checkpointer, and its frame capture.
struct OpenShard {
    conn: Connection,
    checkpointer: Checkpointer,
    /// `None` when capture is disabled.
    ///
    /// Survives eviction: the capture registry is keyed by path, so reopening a shard
    /// returns the same buffer. A WAL reset on reopen bumps the generation and discards
    /// uncommitted frames, which is correct — they were never committed.
    capture: Option<Arc<WalCapture>>,
    /// True while a snapshot copy is in flight.
    ///
    /// Checkpointing is suspended for the duration. That is what freezes the main database
    /// file: in WAL mode, committed pages only reach the main file via a checkpoint, so with
    /// checkpointing suspended every subsequent write lands in the WAL and the bytes being
    /// copied cannot change underneath the copy.
    snapshot_hold: bool,
    /// Set if the hold had to be broken to stop the WAL growing without bound. The snapshot
    /// in flight is then unusable, because checkpointing resumed under it.
    snapshot_invalidated: bool,
}

#[derive(Debug, Default)]
struct Counters {
    batches: AtomicU64,
    requests: AtomicU64,
    max_batch: AtomicU64,
    shard_opens: AtomicU64,
    shard_evictions: AtomicU64,
    open_now: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterFleetStats {
    pub batches: u64,
    pub requests: u64,
    pub max_batch: u64,
    pub shard_opens: u64,
    pub shard_evictions: u64,
    /// Writer connections currently resident across all threads.
    pub open_now: u64,
    pub threads: usize,
}

impl WriterFleetStats {
    /// Statements per transaction. Above 1 under load is the signature of group commit
    /// working; pinned at 1 means batching is not happening.
    pub fn mean_batch(&self) -> f64 {
        if self.batches == 0 {
            0.0
        } else {
            self.requests as f64 / self.batches as f64
        }
    }
}

/// The active frame sink, shared by every writer thread and swappable at runtime. A batch reads it
/// under a short read lock in [`drain_capture`]; [`WriterFleet::set_sink`] takes the write lock to
/// attach/detach a sink (e.g. an operator turning S3 archival on from the console) without a
/// restart. Capture must already be on for there to be frames to hand it.
type SharedSink = Arc<RwLock<Option<Arc<dyn FrameSink>>>>;

pub struct WriterFleet {
    senders: Vec<SyncSender<Job>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    counters: Arc<Counters>,
    checkpoint_counters: Arc<CheckpointCounters>,
    cfg: ShardConfig,
    positions: Option<Arc<StreamPositions>>,
    sink: SharedSink,
}

impl WriterFleet {
    pub fn spawn(
        dir: &Path,
        cfg: ShardConfig,
        profile: PragmaProfile,
        checkpoint: CheckpointConfig,
    ) -> Result<Self> {
        Self::spawn_with_sink(
            dir,
            cfg,
            profile,
            checkpoint,
            None,
            None,
            None,
            Arc::new(crate::shard::mode::ShardModes::new()),
        )
    }

    /// Start the fleet, optionally streaming captured frames to `sink`.
    ///
    /// The sink is called on the writer thread after each batch commits. That is deliberate:
    /// a slow sink slows the writer, the bounded write queue fills behind it, and callers
    /// get `WriterBusy`. Backpressure reaches clients through the mechanism that already
    /// exists rather than a second one built for replication.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_with_sink(
        dir: &Path,
        cfg: ShardConfig,
        profile: PragmaProfile,
        checkpoint: CheckpointConfig,
        sink: Option<Arc<dyn FrameSink>>,
        acks: Option<Arc<crate::replication::AckTracker>>,
        gate: Option<Arc<dyn crate::shard::WriteGate>>,
        modes: Arc<crate::shard::mode::ShardModes>,
    ) -> Result<Self> {
        cfg.validate()?;
        let positions = if cfg.capture {
            crate::vfs::register()?;
            Some(Arc::new(StreamPositions::load_or_init(dir)?))
        } else {
            None
        };
        std::fs::create_dir_all(dir).map_err(|e| {
            Error::ShardConfig(format!("creating data directory {}: {e}", dir.display()))
        })?;

        let counters = Arc::new(Counters::default());
        let checkpoint_counters = Arc::new(CheckpointCounters::default());
        let mut senders = Vec::with_capacity(cfg.writer_threads);
        let mut threads = Vec::with_capacity(cfg.writer_threads);
        // One shared, swappable sink for every writer thread, so it can be attached/detached later.
        let sink: SharedSink = Arc::new(RwLock::new(sink));

        for i in 0..cfg.writer_threads {
            let (tx, rx) = sync_channel(cfg.write_queue_depth);
            let ctx = ThreadCtx {
                dir: dir.to_path_buf(),
                profile: profile.clone(),
                checkpoint: checkpoint.clone(),
                capacity: cfg.open_writers_per_thread,
                max_batch: cfg.max_batch,
                capture: cfg.capture,
                max_retained_bytes: cfg.max_retained_bytes,
                snapshot_hold_max_wal: cfg.snapshot_hold_max_wal,
                sink: Arc::clone(&sink),
                acks: acks.clone(),
                gate: gate.clone(),
                modes: modes.clone(),
                positions: positions.clone(),
                counters: Arc::clone(&counters),
                checkpoint_counters: Arc::clone(&checkpoint_counters),
            };
            let handle = std::thread::Builder::new()
                .name(format!("meshdb-writer-{i}"))
                .spawn(move || writer_loop(rx, ctx))
                .expect("spawning a writer thread");
            senders.push(tx);
            threads.push(handle);
        }

        Ok(Self {
            senders,
            threads: Mutex::new(threads),
            counters,
            checkpoint_counters,
            cfg,
            positions,
            sink,
        })
    }

    /// Attach, replace, or detach (`None`) the frame sink at runtime. Takes effect on the next
    /// batch drain on every writer thread. Only meaningful when capture is on; with capture off
    /// there are no frames to hand a sink.
    pub fn set_sink(&self, sink: Option<Arc<dyn FrameSink>>) {
        *self.sink.write().expect("sink lock") = sink;
    }

    /// Whether frame capture is on — the precondition for a sink to receive anything.
    pub fn capture_enabled(&self) -> bool {
        self.cfg.capture
    }

    /// Whether a sink is currently attached.
    pub fn has_sink(&self) -> bool {
        self.sink.read().expect("sink lock").is_some()
    }

    fn sender(&self, shard: ShardId) -> Result<&SyncSender<Job>> {
        if shard.0 >= self.cfg.shard_count {
            return Err(Error::ShardConfig(format!(
                "{shard} is outside the configured {} shards",
                self.cfg.shard_count
            )));
        }
        Ok(&self.senders[self.cfg.thread_of(shard)])
    }

    /// Submit statements to one shard and block until its batch commits.
    ///
    /// Returns [`Error::WriterBusy`] immediately if the queue is full, rather than blocking
    /// — the same load-shedding contract the reader fleet offers.
    pub fn execute(&self, shard: ShardId, statements: Vec<Statement>) -> Result<Vec<Outcome>> {
        self.submit(shard, statements, false)
    }

    /// Apply `statements` as one atomic transaction: all commit, or none do. The COMMIT of a
    /// client-held transaction.
    pub fn execute_atomic(
        &self,
        shard: ShardId,
        statements: Vec<Statement>,
    ) -> Result<Vec<Outcome>> {
        self.submit(shard, statements, true)
    }

    fn submit(
        &self,
        shard: ShardId,
        statements: Vec<Statement>,
        atomic: bool,
    ) -> Result<Vec<Outcome>> {
        let (reply_tx, reply_rx) = sync_channel(1);
        self.sender(shard)?
            .try_send(Job::Write(Pending {
                atomic,
                shard,
                statements,
                reply: reply_tx,
            }))
            .map_err(|e| match e {
                std::sync::mpsc::TrySendError::Full(_) => Error::WriterBusy,
                std::sync::mpsc::TrySendError::Disconnected(_) => Error::WriterGone,
            })?;
        reply_rx.recv().map_err(|_| Error::WriterGone)?
    }

    pub fn execute_one(&self, shard: ShardId, sql: impl Into<Statement>) -> Result<Outcome> {
        let mut out = self.execute(shard, vec![sql.into()])?;
        Ok(out.pop().expect("one request yields one outcome"))
    }

    /// Copy `shard` to `dest`, returning the `(epoch, lsn)` it represents.
    ///
    /// **The copy runs on the calling thread, not the writer thread.** The writer is only
    /// occupied long enough to checkpoint and record the position; copying a multi-gigabyte
    /// shard inline would stall every other shard sharing that thread for the duration,
    /// which at scale is minutes of frozen writes.
    ///
    /// The follower installs this and resumes from `lsn + 1`.
    pub fn snapshot(&self, shard: ShardId, dest: &Path) -> Result<(u64, u64)> {
        let (epoch, lsn, source) = self.begin_snapshot(shard)?;

        let copy = std::fs::copy(&source, dest).map_err(|e| {
            Error::Manifest(format!(
                "snapshotting {} to {}: {e}",
                source.display(),
                dest.display()
            ))
        });

        // Release the freeze even if the copy failed, or checkpointing stays suspended and
        // the WAL grows without bound.
        let still_valid = self.end_snapshot(shard);
        copy?;

        match still_valid? {
            true => Ok((epoch, lsn)),
            false => Err(Error::SnapshotInvalidated {
                shard: shard.to_string(),
            }),
        }
    }

    /// Rebuild `shard`, reclaiming free pages.
    ///
    /// Runs on the writer thread **outside** a transaction — `VACUUM` cannot run inside
    /// one, which is why it is rejected on the ordinary statement path.
    ///
    /// Expensive in two ways worth knowing before scheduling it: it needs roughly twice the
    /// shard's size in free disk, and it rewrites every page, so with capture on it
    /// produces a replication stream the size of the whole shard.
    /// Read `shard`'s schema version, or apply `ddl` and bump it.
    pub fn schema(&self, shard: ShardId, ddl: Option<Statement>) -> Result<i64> {
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.sender(shard)?
            .send(Job::Schema { shard, ddl, reply })
            .map_err(|_| Error::WriterGone)?;
        rx.recv().map_err(|_| Error::WriterGone)?
    }

    /// Close any cached connection for `shard` on every writer thread.
    pub fn quiesce(&self, shard: ShardId) -> Result<usize> {
        // Sent to every thread, not just the shard's owner: hash affinity decides which
        // thread *serves* a shard, but an earlier configuration may have left an entry
        // elsewhere, and a missed handle is exactly the one that corrupts.
        let mut closed = 0;
        for tx in &self.senders {
            let (reply, rx) = std::sync::mpsc::sync_channel(1);
            if tx.send(Job::Quiesce { shard, reply }).is_err() {
                continue;
            }
            if rx.recv().map_err(|_| Error::WriterGone)? {
                closed += 1;
            }
        }
        Ok(closed)
    }

    pub fn vacuum(&self, shard: ShardId) -> Result<()> {
        let (reply_tx, reply_rx) = sync_channel(1);
        self.sender(shard)?
            .send(Job::Vacuum {
                shard,
                reply: reply_tx,
            })
            .map_err(|_| Error::WriterGone)?;
        reply_rx.recv().map_err(|_| Error::WriterGone)?
    }

    /// Force a WAL checkpoint on `shard` now, returning `(busy, log_pages, checkpointed)`.
    pub fn checkpoint(&self, shard: ShardId) -> Result<(i64, i64, i64)> {
        let (reply_tx, reply_rx) = sync_channel(1);
        self.sender(shard)?
            .send(Job::Checkpoint {
                shard,
                reply: reply_tx,
            })
            .map_err(|_| Error::WriterGone)?;
        reply_rx.recv().map_err(|_| Error::WriterGone)?
    }

    /// Freeze `shard`'s main file and report `(epoch, lsn, path)`.
    ///
    /// The file at `path` will not change until [`Self::end_snapshot`]. Callers must pair
    /// the two, or checkpointing stays suspended.
    pub fn begin_snapshot(&self, shard: ShardId) -> Result<(u64, u64, PathBuf)> {
        let (reply_tx, reply_rx) = sync_channel(1);
        self.sender(shard)?
            .send(Job::BeginSnapshot {
                shard,
                reply: reply_tx,
            })
            .map_err(|_| Error::WriterGone)?;
        reply_rx.recv().map_err(|_| Error::WriterGone)?
    }

    /// Release the freeze. `false` means the snapshot was invalidated and must be retaken.
    pub fn end_snapshot(&self, shard: ShardId) -> Result<bool> {
        let (reply_tx, reply_rx) = sync_channel(1);
        self.sender(shard)?
            .send(Job::EndSnapshot {
                shard,
                reply: reply_tx,
            })
            .map_err(|_| Error::WriterGone)?;
        reply_rx.recv().map_err(|_| Error::WriterGone)?
    }

    /// Open the shard's writer connection if it is not already open.
    ///
    /// Readers call this before opening their own connection for a cold shard.
    pub fn ensure_open(&self, shard: ShardId) -> Result<()> {
        let (reply_tx, reply_rx) = sync_channel(1);
        // Blocking `send`, unlike a write: this is a rare cold-path call that must
        // succeed, and the queue is bounded so waiting cannot grow memory.
        self.sender(shard)?
            .send(Job::EnsureOpen {
                shard,
                reply: reply_tx,
            })
            .map_err(|_| Error::WriterGone)?;
        reply_rx.recv().map_err(|_| Error::WriterGone)?
    }

    pub fn stats(&self) -> WriterFleetStats {
        WriterFleetStats {
            batches: self.counters.batches.load(Ordering::Relaxed),
            requests: self.counters.requests.load(Ordering::Relaxed),
            max_batch: self.counters.max_batch.load(Ordering::Relaxed),
            shard_opens: self.counters.shard_opens.load(Ordering::Relaxed),
            shard_evictions: self.counters.shard_evictions.load(Ordering::Relaxed),
            open_now: self.counters.open_now.load(Ordering::Relaxed),
            threads: self.cfg.writer_threads,
        }
    }

    /// The primary's stream identity, if capture is on.
    pub fn positions(&self) -> Option<&Arc<StreamPositions>> {
        self.positions.as_ref()
    }

    pub fn checkpoint_counters(&self) -> &Arc<CheckpointCounters> {
        &self.checkpoint_counters
    }

    pub fn checkpoint_stats(&self) -> crate::storage::CheckpointStats {
        let c = &self.checkpoint_counters;
        crate::storage::CheckpointStats {
            passive: c.passive.load(Ordering::Relaxed),
            truncated: c.truncated.load(Ordering::Relaxed),
            stalls: c.stalls.load(Ordering::Relaxed),
            failures: c.failures.load(Ordering::Relaxed),
            wal_bytes: c.wal_bytes.load(Ordering::Relaxed),
        }
    }
}

impl Drop for WriterFleet {
    fn drop(&mut self) {
        // Record the stream position before the threads stop. Without this the next run
        // cannot trust its LSNs, bumps the epoch, and forces every follower to re-bootstrap.
        if let Some(p) = &self.positions
            && let Err(e) = p.mark_clean()
        {
            tracing::error!(
                error = %e,
                "could not persist the stream position; the next start will bump the epoch \
                 and every follower will re-bootstrap"
            );
        }
        for tx in self.senders.drain(..) {
            let _ = tx.send(Job::Shutdown);
        }
        if let Ok(mut threads) = self.threads.lock() {
            for t in threads.drain(..) {
                let _ = t.join();
            }
        }
    }
}

struct ThreadCtx {
    dir: PathBuf,
    profile: PragmaProfile,
    checkpoint: CheckpointConfig,
    capacity: usize,
    max_batch: usize,
    capture: bool,
    max_retained_bytes: usize,
    snapshot_hold_max_wal: u64,
    sink: SharedSink,
    /// Quorum confirmation. `None` on a standalone node, which must not wait for reports
    /// that will never come.
    acks: Option<Arc<crate::replication::AckTracker>>,
    /// Leadership check. `None` on a standalone node, which always may write.
    gate: Option<Arc<dyn crate::shard::WriteGate>>,
    /// Which shards this node owns the files of. Consulted before any connection is opened.
    modes: Arc<crate::shard::mode::ShardModes>,
    positions: Option<Arc<StreamPositions>>,
    counters: Arc<Counters>,
    checkpoint_counters: Arc<CheckpointCounters>,
}

fn writer_loop(rx: Receiver<Job>, ctx: ThreadCtx) {
    let mut open: Lru<OpenShard> = Lru::new(ctx.capacity);

    loop {
        let first = match rx.recv() {
            Ok(job) => job,
            Err(_) => return,
        };

        let mut pending: Vec<Pending> = Vec::new();
        let mut stopping = false;

        match first {
            Job::Write(p) => pending.push(p),
            Job::EnsureOpen { shard, reply } => {
                let _ = reply.send(ensure_shard(&mut open, &ctx, shard).map(|_| ()));
                continue;
            }
            Job::BeginSnapshot { shard, reply } => {
                let _ = reply.send(begin_snapshot(&mut open, &ctx, shard));
                continue;
            }
            Job::EndSnapshot { shard, reply } => {
                let _ = reply.send(end_snapshot(&mut open, &ctx, shard));
                continue;
            }
            Job::Vacuum { shard, reply } => {
                let _ = reply.send(vacuum_shard(&mut open, &ctx, shard));
                continue;
            }
            Job::Checkpoint { shard, reply } => {
                let _ = reply.send(checkpoint_shard(&mut open, &ctx, shard));
                continue;
            }
            Job::Schema { shard, ddl, reply } => {
                let _ = reply.send(schema_op(&mut open, &ctx, shard, ddl.as_ref()));
                continue;
            }
            Job::Quiesce { shard, reply } => {
                // Dropping the entry closes the connection. Nothing else is needed: the
                // point is only that no handle survives into the replication path's writes.
                let closed = open.remove(shard).is_some();
                let _ = reply.send(closed);
                continue;
            }
            Job::Shutdown => return,
        }

        // Drain whatever else has already queued. This is what produces group commit:
        // everything that arrived while the previous COMMIT was fsyncing gets absorbed.
        let mut requests = pending[0].statements.len();
        while requests < ctx.max_batch {
            match rx.try_recv() {
                Ok(Job::Write(p)) => {
                    requests += p.statements.len();
                    pending.push(p);
                }
                Ok(Job::EnsureOpen { shard, reply }) => {
                    let _ = reply.send(ensure_shard(&mut open, &ctx, shard).map(|_| ()));
                }
                Ok(Job::BeginSnapshot { shard, reply }) => {
                    let _ = reply.send(begin_snapshot(&mut open, &ctx, shard));
                }
                Ok(Job::EndSnapshot { shard, reply }) => {
                    let _ = reply.send(end_snapshot(&mut open, &ctx, shard));
                }
                Ok(Job::Vacuum { shard, reply }) => {
                    let _ = reply.send(vacuum_shard(&mut open, &ctx, shard));
                }
                Ok(Job::Checkpoint { shard, reply }) => {
                    let _ = reply.send(checkpoint_shard(&mut open, &ctx, shard));
                }
                Ok(Job::Schema { shard, ddl, reply }) => {
                    let _ = reply.send(schema_op(&mut open, &ctx, shard, ddl.as_ref()));
                }
                Ok(Job::Quiesce { shard, reply }) => {
                    let closed = open.remove(shard).is_some();
                    let _ = reply.send(closed);
                }
                Ok(Job::Shutdown) => {
                    // Honour it only after the batch in hand commits — dropping it would
                    // lose writes callers are still blocked on.
                    stopping = true;
                    break;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    stopping = true;
                    break;
                }
            }
        }

        ctx.counters.batches.fetch_add(1, Ordering::Relaxed);
        ctx.counters
            .requests
            .fetch_add(requests as u64, Ordering::Relaxed);
        ctx.counters
            .max_batch
            .fetch_max(requests as u64, Ordering::Relaxed);

        // A transaction cannot span files, so the batch is split by shard and each shard
        // commits its own transaction.
        let mut by_shard: std::collections::BTreeMap<ShardId, Vec<Pending>> = Default::default();
        for p in pending {
            by_shard.entry(p.shard).or_default().push(p);
        }

        for (shard, group) in by_shard {
            apply_shard_batch(&mut open, &ctx, shard, group);
        }

        if stopping {
            return;
        }
    }
}

fn apply_shard_batch(
    open: &mut Lru<OpenShard>,
    ctx: &ThreadCtx,
    shard: ShardId,
    group: Vec<Pending>,
) {
    let entry = match ensure_shard(open, ctx, shard) {
        Ok(e) => e,
        Err(e) => {
            for p in group {
                let _ = p.reply.send(Err(Error::BatchAborted {
                    reason: e.to_string(),
                }));
            }
            return;
        }
    };

    // Leadership is checked *before* the transaction opens, not after it commits. A deposed
    // leader that discovers its state only afterwards has already written to a file another
    // node may now own.
    if let Some(gate) = &ctx.gate
        && let Err(e) = gate.check_may_write(shard)
    {
        for p in group {
            let _ = p.reply.send(Err(e.clone_shallow()));
        }
        return;
    }

    let groups: Vec<Vec<Statement>> = group.iter().map(|p| p.statements.clone()).collect();
    let atomic: Vec<bool> = group.iter().map(|p| p.atomic).collect();

    match apply::batch_with(&mut entry.conn, &groups, &atomic) {
        Ok(results) => {
            // Drain to the sink BEFORE replying. A caller told its write succeeded must be
            // able to assume the frames reached the replication stream; replying first
            // would acknowledge data the sink may never receive.
            let highest = match drain_capture(entry, ctx, shard) {
                Ok(h) => h,
                Err(e) => {
                    for p in group {
                        let _ = p.reply.send(Err(e.clone_shallow()));
                    }
                    return;
                }
            };

            // And wait for a majority to hold it before replying. This is what makes the
            // acknowledgement mean something a future leader will still have: without it a
            // leader that dies in this window has acknowledged a write nobody else received,
            // and no election can recover it.
            //
            // One wait per batch, not per write — group commit amortises the round trip
            // across everything queued behind the running transaction.
            if let (Some(acks), Some(lsn)) = (&ctx.acks, highest)
                && let Err(e) = acks.wait_for_quorum(shard, lsn)
            {
                for p in group {
                    let _ = p.reply.send(Err(e.clone_shallow()));
                }
                return;
            }

            for (p, outcomes) in group.into_iter().zip(results) {
                let _ = p.reply.send(Ok(outcomes));
            }
            // Between transactions, never inside one — and never while a snapshot copy is
            // in flight, because checkpointing is what would change the file being copied.
            if entry.snapshot_hold {
                // The hold cannot be honoured indefinitely: with checkpointing suspended the
                // WAL grows without bound, and running out of disk is worse than a failed
                // snapshot. Break the hold, and mark the snapshot unusable so the caller
                // retakes it rather than shipping a file that changed underneath it.
                let wal = crate::storage::checkpoint::wal_path_for(&shard.path(&ctx.dir));
                let wal_bytes = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
                if wal_bytes >= ctx.snapshot_hold_max_wal {
                    tracing::warn!(
                        %shard,
                        wal_bytes,
                        "WAL grew past the snapshot hold limit; breaking the hold and \
                         invalidating the snapshot in flight"
                    );
                    entry.snapshot_hold = false;
                    entry.snapshot_invalidated = true;
                    entry.checkpointer.after_batch(&entry.conn);
                }
            } else {
                entry.checkpointer.after_batch(&entry.conn);
            }
        }
        Err(reason) => {
            for p in group {
                let _ = p.reply.send(Err(Error::BatchAborted {
                    reason: reason.clone(),
                }));
            }
        }
    }
}

/// Freeze a shard's main file at a known stream position. Fast: no copying happens here.
fn begin_snapshot(
    open: &mut Lru<OpenShard>,
    ctx: &ThreadCtx,
    shard: ShardId,
) -> Result<(u64, u64, PathBuf)> {
    let entry = ensure_shard(open, ctx, shard)?;

    // Order matters. Ship everything captured so far, so the frames the follower receives
    // begin exactly after this snapshot; then fold the WAL into the main file, so the bytes
    // about to be copied contain all of it.
    drain_capture(entry, ctx, shard)?;
    entry
        .conn
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            r.get::<_, i64>(0)
        })
        .map_err(crate::Error::Sqlite)?;

    // From here until the hold is released, checkpointing is suspended, so committed pages
    // stay in the WAL and the main file cannot change under the copy.
    entry.snapshot_hold = true;
    entry.snapshot_invalidated = false;

    let (epoch, lsn) = match &ctx.positions {
        Some(p) => (p.epoch(), p.last_allocated(shard)),
        None => (0, 0),
    };
    Ok((epoch, lsn, shard.path(&ctx.dir)))
}

/// Rebuild a shard. Must not run inside a transaction, hence its own job.
/// Force a WAL checkpoint (TRUNCATE) on `shard` now, returning SQLite's `(busy, log_pages,
/// checkpointed)`. `busy = 1` means a long-running reader held the checkpoint back — not an error.
fn checkpoint_shard(
    open: &mut Lru<OpenShard>,
    ctx: &ThreadCtx,
    shard: ShardId,
) -> Result<(i64, i64, i64)> {
    let entry = ensure_shard(open, ctx, shard)?;
    if entry.snapshot_hold {
        return Err(Error::Unsupported(format!(
            "{shard} is frozen for a snapshot; checkpointing is suspended for the duration"
        )));
    }
    entry
        .conn
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(crate::Error::Sqlite)
}

fn vacuum_shard(open: &mut Lru<OpenShard>, ctx: &ThreadCtx, shard: ShardId) -> Result<()> {
    let entry = ensure_shard(open, ctx, shard)?;
    if entry.snapshot_hold {
        return Err(Error::Unsupported(format!(
            "{shard} is frozen for a snapshot; VACUUM would rewrite the file being copied"
        )));
    }
    tracing::info!(%shard, "vacuum starting");
    entry
        .conn
        .execute_batch("VACUUM")
        .map_err(crate::Error::Sqlite)?;
    // VACUUM leaves everything in the WAL; fold it back so the space is actually returned.
    entry
        .conn
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            r.get::<_, i64>(0)
        })
        .map_err(crate::Error::Sqlite)?;
    drain_capture(entry, ctx, shard)?;
    tracing::info!(%shard, "vacuum complete");
    Ok(())
}

/// Release the freeze; `false` if it had to be broken and the snapshot is unusable.
fn end_snapshot(open: &mut Lru<OpenShard>, ctx: &ThreadCtx, shard: ShardId) -> Result<bool> {
    let entry = ensure_shard(open, ctx, shard)?;
    entry.snapshot_hold = false;
    Ok(!entry.snapshot_invalidated)
}

/// Read, or advance, a shard's schema version.
///
/// The DDL and the version bump go in **one transaction**. Separately, a crash between them
/// leaves a shard whose schema and version disagree — and the version is what everything else
/// trusts to decide whether a cross-shard read is safe, so a wrong one is worse than none.
fn schema_op(
    open: &mut Lru<OpenShard>,
    ctx: &ThreadCtx,
    shard: ShardId,
    ddl: Option<&Statement>,
) -> Result<i64> {
    let entry = ensure_shard(open, ctx, shard)?;
    let Some(ddl) = ddl else {
        return crate::storage::schema::version_of(&entry.conn);
    };

    let next = crate::storage::schema::version_of(&entry.conn)? + 1;
    let tx = entry
        .conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(Error::Sqlite)?;
    tx.execute_batch(&ddl.sql).map_err(Error::Sqlite)?;
    // Inside the transaction, so schema and version move together or not at all.
    tx.execute_batch(&format!("PRAGMA user_version = {next}"))
        .map_err(Error::Sqlite)?;
    tx.commit().map_err(Error::Sqlite)?;

    // The DDL produced frames like any other write; they must reach the stream.
    drain_capture(entry, ctx, shard)?;
    tracing::info!(%shard, version = next, "schema advanced");
    Ok(next)
}

/// Hand this shard's committed frames to the sink.
fn drain_capture(entry: &mut OpenShard, ctx: &ThreadCtx, shard: ShardId) -> Result<Option<Lsn>> {
    let Some(capture) = &entry.capture else {
        return Ok(None);
    };

    // Drain first, then check. Frames are never dropped, so an overflow is a backlog rather
    // than a loss — and draining it is the recovery. Checking before draining would leave a
    // shard permanently refusing writes even after its sink came back.
    let txns = capture.drain_committed();
    if txns.is_empty() {
        return Ok(None);
    }

    // Positions are assigned here, on the writer thread, so they are dense and ordered by
    // construction — the follower's ability to detect loss depends on that density.
    let (epoch, first) = match &ctx.positions {
        Some(p) => (p.epoch(), p.allocate(shard, txns.len())),
        None => (0, 1),
    };
    let positioned: Vec<StreamTxn> = txns
        .into_iter()
        .enumerate()
        .map(|(i, txn)| StreamTxn {
            lsn: first + i as u64,
            txn,
        })
        .collect();

    let count = positioned.len();
    // Read the current sink under a short lock, so an operator can attach/detach one at runtime.
    let current = ctx.sink.read().expect("sink lock").clone();
    let result = match &current {
        Some(sink) => sink.accept(shard, epoch, positioned.clone()),
        // Capture on with no sink: drop them, having proven they were captured. Only
        // sensible for measuring capture's cost, which is why `NullSink` is named as it is.
        None => Ok(()),
    };

    if let Err(e) = result {
        // The sink refused, so these frames reached nobody. Draining already took them out
        // of the capture, so without giving them back they would be lost outright — never
        // delivered, never retained, and their place in the stream consumed. Put both back.
        tracing::warn!(%shard, error = %e, count, "sink refused a batch; requeueing it");
        capture.requeue(positioned.into_iter().map(|p| p.txn).collect());
        if let Some(p) = &ctx.positions {
            p.rollback(shard, count);
        }

        // Which failure to report matters. A refusal on its own is transient and the sink's
        // own error says most about it. Once the requeued backlog passes the retention cap
        // the binding constraint has changed: the node can no longer accept writes at all,
        // whatever this particular batch did, and saying so is more useful than repeating
        // the sink error.
        if capture.overflowed() {
            tracing::error!(
                %shard,
                retained_bytes = capture.retained_bytes(),
                "capture backlog passed its limit while the sink was refusing; refusing writes"
            );
            return Err(Error::CaptureOverflow {
                shard: shard.to_string(),
                retained: capture.retained_bytes(),
            });
        }
        return Err(e);
    }

    // Still over the cap after draining means the backlog cannot be shifted — a sink that
    // is not consuming, or one transaction too large to ever fit. Refusing writes is the
    // only honest response: committing locally what no replica will receive is the
    // divergence physical replication exists to prevent.
    if capture.overflowed() {
        tracing::error!(
            %shard,
            retained_bytes = capture.retained_bytes(),
            "capture retention is still over its limit after draining; refusing writes"
        );
        return Err(Error::CaptureOverflow {
            shard: shard.to_string(),
            retained: capture.retained_bytes(),
        });
    }
    // The last position handed to the sink. What a quorum must hold before any caller in this
    // batch is told its write succeeded.
    Ok(Some(first + count as u64 - 1))
}

/// Get the shard's connection, opening it (and evicting a colder one) if needed.
fn ensure_shard<'a>(
    open: &'a mut Lru<OpenShard>,
    ctx: &ThreadCtx,
    shard: ShardId,
) -> Result<&'a mut OpenShard> {
    // Checked before anything is opened. A followed shard's file is rewritten behind
    // SQLite's back, so a connection on it serves stale rows with no error — the one failure
    // mode that no later check can detect.
    ctx.modes.check_may_open(shard)?;

    let path = shard.path(&ctx.dir);
    let profile = ctx.profile.clone();
    let checkpoint = ctx.checkpoint.clone();
    let checkpoint_counters = Arc::clone(&ctx.checkpoint_counters);

    let use_capture = ctx.capture;
    let max_retained = ctx.max_retained_bytes;

    let (entry, event) = open.get_or_open(shard, move || -> Result<OpenShard> {
        // Registration must precede the open: the VFS consults the registry when the -wal
        // file is first opened, so a capture registered afterwards would miss frames.
        let capture = if use_capture {
            Some(crate::vfs::capture_for_with_limit(&path, max_retained)?)
        } else {
            None
        };
        let vfs = use_capture.then_some(crate::vfs::VFS_NAME);

        // `open_writer_vfs` materializes the -wal/-shm sidecars, which is what re-establishes
        // the writer-opens-first invariant on every reopen after an eviction.
        let conn = open_writer_vfs(&path, &profile, vfs)?;
        let checkpointer = Checkpointer::new(&path, checkpoint, checkpoint_counters);
        Ok(OpenShard {
            conn,
            checkpointer,
            capture,
            snapshot_hold: false,
            snapshot_invalidated: false,
        })
    })?;

    if event.opened {
        ctx.counters.shard_opens.fetch_add(1, Ordering::Relaxed);
        ctx.counters.open_now.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(%shard, "opened shard writer connection");
    }
    if event.evicted > 0 {
        tracing::debug!(%shard, evicted = event.evicted, "evicted colder shards to make room");
        ctx.counters
            .shard_evictions
            .fetch_add(event.evicted, Ordering::Relaxed);
        ctx.counters
            .open_now
            .fetch_sub(event.evicted, Ordering::Relaxed);
    }

    Ok(entry)
}
