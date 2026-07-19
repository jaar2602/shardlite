//! Writer threads serving many shards through a bounded connection cache.
//!
//! A shard is assigned to a thread by `id % writer_threads`, so every shard still has
//! exactly one writer — single-writer-per-file is preserved — while the thread count stays
//! bounded no matter how many shards exist. Each thread holds an LRU of open connections,
//! so 64 shards cost the memory of a handful of connections rather than 64.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};
use std::thread::JoinHandle;

use rusqlite::Connection;

use super::lru::Lru;
use super::{ShardConfig, ShardId};
use crate::config::{CheckpointConfig, PragmaProfile};
use crate::error::{Error, Result};
use crate::replication::FrameSink;
use crate::storage::apply;
use crate::storage::checkpoint::{Checkpointer, Counters as CheckpointCounters};
use crate::storage::exec::{Outcome, Statement};
use crate::storage::open::open_writer_vfs;
use crate::vfs::WalCapture;

struct Pending {
    shard: ShardId,
    statements: Vec<Statement>,
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

pub struct WriterFleet {
    senders: Vec<SyncSender<Job>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    counters: Arc<Counters>,
    checkpoint_counters: Arc<CheckpointCounters>,
    cfg: ShardConfig,
}

impl WriterFleet {
    pub fn spawn(
        dir: &Path,
        cfg: ShardConfig,
        profile: PragmaProfile,
        checkpoint: CheckpointConfig,
    ) -> Result<Self> {
        Self::spawn_with_sink(dir, cfg, profile, checkpoint, None)
    }

    /// Start the fleet, optionally streaming captured frames to `sink`.
    ///
    /// The sink is called on the writer thread after each batch commits. That is deliberate:
    /// a slow sink slows the writer, the bounded write queue fills behind it, and callers
    /// get `WriterBusy`. Backpressure reaches clients through the mechanism that already
    /// exists rather than a second one built for replication.
    pub fn spawn_with_sink(
        dir: &Path,
        cfg: ShardConfig,
        profile: PragmaProfile,
        checkpoint: CheckpointConfig,
        sink: Option<Arc<dyn FrameSink>>,
    ) -> Result<Self> {
        cfg.validate()?;
        if cfg.capture {
            crate::vfs::register()?;
        }
        std::fs::create_dir_all(dir).map_err(|e| {
            Error::ShardConfig(format!("creating data directory {}: {e}", dir.display()))
        })?;

        let counters = Arc::new(Counters::default());
        let checkpoint_counters = Arc::new(CheckpointCounters::default());
        let mut senders = Vec::with_capacity(cfg.writer_threads);
        let mut threads = Vec::with_capacity(cfg.writer_threads);

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
                sink: sink.clone(),
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
        })
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
        let (reply_tx, reply_rx) = sync_channel(1);
        self.sender(shard)?
            .try_send(Job::Write(Pending {
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
    sink: Option<Arc<dyn FrameSink>>,
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

    let groups: Vec<Vec<Statement>> = group.iter().map(|p| p.statements.clone()).collect();

    match apply::batch(&mut entry.conn, &groups) {
        Ok(results) => {
            // Drain to the sink BEFORE replying. A caller told its write succeeded must be
            // able to assume the frames reached the replication stream; replying first
            // would acknowledge data the sink may never receive.
            if let Err(e) = drain_capture(entry, ctx, shard) {
                for p in group {
                    let _ = p.reply.send(Err(e.clone_shallow()));
                }
                return;
            }
            for (p, outcomes) in group.into_iter().zip(results) {
                let _ = p.reply.send(Ok(outcomes));
            }
            // Between transactions, never inside one.
            entry.checkpointer.after_batch(&entry.conn);
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

/// Hand this shard's committed frames to the sink.
fn drain_capture(entry: &mut OpenShard, ctx: &ThreadCtx, shard: ShardId) -> Result<()> {
    let Some(capture) = &entry.capture else {
        return Ok(());
    };

    if capture.overflowed() {
        // Frames are never dropped to stay under the cap, so reaching it means the stream
        // is already incomplete. Failing every write from here is the only honest response:
        // continuing would commit locally what no replica will ever see.
        return Err(Error::CaptureOverflow {
            shard: shard.to_string(),
            retained: capture.retained_bytes(),
        });
    }

    let txns = capture.drain_committed();
    if txns.is_empty() {
        return Ok(());
    }
    match &ctx.sink {
        Some(sink) => sink.accept(shard, txns),
        // Capture on with no sink: drop them, having proven they were captured. Only
        // sensible for measuring capture's cost, which is why `NullSink` is named as it is.
        None => Ok(()),
    }
}

/// Get the shard's connection, opening it (and evicting a colder one) if needed.
fn ensure_shard<'a>(
    open: &'a mut Lru<OpenShard>,
    ctx: &ThreadCtx,
    shard: ShardId,
) -> Result<&'a mut OpenShard> {
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
        })
    })?;

    if event.opened {
        ctx.counters.shard_opens.fetch_add(1, Ordering::Relaxed);
        ctx.counters.open_now.fetch_add(1, Ordering::Relaxed);
    }
    if event.evicted > 0 {
        ctx.counters
            .shard_evictions
            .fetch_add(event.evicted, Ordering::Relaxed);
        ctx.counters
            .open_now
            .fetch_sub(event.evicted, Ordering::Relaxed);
    }

    Ok(entry)
}
