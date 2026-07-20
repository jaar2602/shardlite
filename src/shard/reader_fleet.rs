//! Reader threads serving many shards through a bounded connection cache.
//!
//! Mirrors the writer fleet, except readers are not partitioned by shard: any thread can
//! serve any shard, and they race to take work from one shared queue. Each thread keeps its
//! own LRU of read-only connections, so a thread that repeatedly serves the same shards
//! keeps their caches warm.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Mutex, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use rusqlite::Connection;

use super::lru::Lru;
use super::writer_fleet::WriterFleet;
use super::{ShardConfig, ShardId};
use crate::config::{PragmaProfile, ReaderPoolConfig};
use crate::error::{Error, Result};
use crate::storage::exec::{self, Outcome, SqlError, Statement};
use crate::storage::open::open_reader_existing;

/// One message in a streamed query. Columns arrive once, then rows, then a terminator.
pub enum StreamMsg {
    Columns(Vec<String>),
    Row(Vec<crate::storage::exec::Value>),
    /// The stream finished cleanly.
    Done,
    /// The query failed or timed out mid-stream. Carries the reason.
    Failed(String),
}

enum Job {
    Query {
        shard: ShardId,
        statement: Statement,
        reply: SyncSender<Result<Outcome>>,
    },
    /// A streaming read: rows are pushed onto `tx` as they are produced, so an arbitrarily
    /// large result never materialises in memory. The bounded channel is the backpressure —
    /// a slow consumer blocks the reader thread here rather than piling rows up.
    Stream {
        shard: ShardId,
        statement: Statement,
        tx: SyncSender<StreamMsg>,
    },
    /// Close any cached connection for a shard, so its file can be handed to the replication
    /// path. Readers matter here more than writers: a read-only handle kept across a
    /// follower's file rename holds the deleted inode and serves a frozen database forever,
    /// with no error and a passing integrity check.
    Quiesce {
        shard: ShardId,
        reply: SyncSender<bool>,
    },
}

#[derive(Debug, Default)]
struct Counters {
    queries: AtomicU64,
    rejected_busy: AtomicU64,
    timed_out: AtomicU64,
    shard_opens: AtomicU64,
    shard_evictions: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReaderFleetStats {
    pub queries: u64,
    pub rejected_busy: u64,
    pub timed_out: u64,
    pub shard_opens: u64,
    pub shard_evictions: u64,
    pub threads: usize,
}

pub struct ReaderFleet {
    tx: Option<flume::Sender<Job>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    counters: Arc<Counters>,
    n_threads: usize,
}

impl ReaderFleet {
    /// Start the fleet.
    ///
    /// Holds a [`Weak`] reference to the writer fleet, used only to re-establish the
    /// writer-opens-first invariant when a reader needs a shard nobody has open. Weak so the
    /// fleets can be dropped in either order without a reference cycle keeping threads alive.
    pub fn spawn(
        dir: &Path,
        cfg: &ShardConfig,
        pool: &ReaderPoolConfig,
        profile: PragmaProfile,
        writers: Weak<WriterFleet>,
        modes: Arc<super::mode::ShardModes>,
    ) -> Result<Self> {
        let (tx, rx) = flume::bounded::<Job>(pool.queue_capacity);
        let counters = Arc::new(Counters::default());
        let mut threads = Vec::with_capacity(cfg.reader_threads);

        for i in 0..cfg.reader_threads {
            let ctx = ThreadCtx {
                dir: dir.to_path_buf(),
                profile: profile.clone(),
                capacity: cfg.open_readers_per_thread,
                timeout: pool.query_timeout,
                counters: Arc::clone(&counters),
                writers: writers.clone(),
                modes: Arc::clone(&modes),
            };
            let rx = rx.clone();
            let handle = std::thread::Builder::new()
                .name(format!("meshdb-reader-{i}"))
                .spawn(move || reader_loop(rx, ctx))
                .expect("spawning a reader thread");
            threads.push(handle);
        }

        Ok(Self {
            tx: Some(tx),
            threads: Mutex::new(threads),
            counters,
            n_threads: cfg.reader_threads,
        })
    }

    /// Close any cached connection for `shard` on every reader thread.
    ///
    /// Returns how many were closed. Sent to every thread because a shard may have been
    /// served by more than one over its life, and a missed handle is exactly the one that
    /// later serves stale rows.
    pub fn quiesce(&self, shard: ShardId) -> Result<usize> {
        let tx = self.tx.as_ref().ok_or(Error::ReaderPoolGone)?;
        let mut closed = 0;
        for _ in 0..self.n_threads {
            let (reply, rx) = sync_channel(1);
            tx.send(Job::Quiesce { shard, reply })
                .map_err(|_| Error::ReaderPoolGone)?;
            if rx.recv().map_err(|_| Error::ReaderPoolGone)? {
                closed += 1;
            }
        }
        Ok(closed)
    }

    /// Run a read-only statement against one shard on the next free reader.
    pub fn query(&self, shard: ShardId, sql: impl Into<Statement>) -> Result<Outcome> {
        let (reply_tx, reply_rx) = sync_channel(1);
        let tx = self.tx.as_ref().ok_or(Error::ReaderPoolGone)?;

        // `try_send`, never `send`: blocking would convert backpressure into an unbounded
        // latency queue held in caller threads instead of in memory.
        tx.try_send(Job::Query {
            shard,
            statement: sql.into(),
            reply: reply_tx,
        })
        .map_err(|e| match e {
            flume::TrySendError::Full(_) => {
                self.counters.rejected_busy.fetch_add(1, Ordering::Relaxed);
                Error::ReaderPoolBusy
            }
            flume::TrySendError::Disconnected(_) => Error::ReaderPoolGone,
        })?;

        reply_rx.recv().map_err(|_| Error::ReaderPoolGone)?
    }

    /// Start a streaming read. Returns a receiver the caller drains: `Columns`, then `Row`s,
    /// then `Done` or `Failed`. The channel is bounded, so draining slowly throttles the
    /// reader thread rather than buffering the whole result.
    ///
    /// `depth` is the row-buffer bound. A few hundred keeps memory tiny while giving the
    /// socket writer something to work ahead on.
    pub fn stream(
        &self,
        shard: ShardId,
        sql: impl Into<Statement>,
        depth: usize,
    ) -> Result<std::sync::mpsc::Receiver<StreamMsg>> {
        let (row_tx, row_rx) = sync_channel(depth.max(1));
        let tx = self.tx.as_ref().ok_or(Error::ReaderPoolGone)?;
        tx.try_send(Job::Stream {
            shard,
            statement: sql.into(),
            tx: row_tx,
        })
        .map_err(|e| match e {
            flume::TrySendError::Full(_) => {
                self.counters.rejected_busy.fetch_add(1, Ordering::Relaxed);
                Error::ReaderPoolBusy
            }
            flume::TrySendError::Disconnected(_) => Error::ReaderPoolGone,
        })?;
        Ok(row_rx)
    }

    pub fn stats(&self) -> ReaderFleetStats {
        ReaderFleetStats {
            queries: self.counters.queries.load(Ordering::Relaxed),
            rejected_busy: self.counters.rejected_busy.load(Ordering::Relaxed),
            timed_out: self.counters.timed_out.load(Ordering::Relaxed),
            shard_opens: self.counters.shard_opens.load(Ordering::Relaxed),
            shard_evictions: self.counters.shard_evictions.load(Ordering::Relaxed),
            threads: self.n_threads,
        }
    }
}

impl Drop for ReaderFleet {
    fn drop(&mut self) {
        self.tx.take(); // disconnecting the queue ends every reader loop
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
    capacity: usize,
    timeout: Duration,
    counters: Arc<Counters>,
    writers: Weak<WriterFleet>,
    /// Readers are subject to the same invariant as writers, and for a sharper reason: a
    /// read-only connection held across a follower's file rename keeps the deleted inode and
    /// serves a frozen database forever, with no error anywhere.
    modes: Arc<super::mode::ShardModes>,
}

fn reader_loop(rx: flume::Receiver<Job>, ctx: ThreadCtx) {
    let mut open: Lru<(Connection, u64)> = Lru::new(ctx.capacity);

    while let Ok(job) = rx.recv() {
        match job {
            Job::Query {
                shard,
                statement,
                reply,
            } => {
                ctx.counters.queries.fetch_add(1, Ordering::Relaxed);
                // Held for the whole read, opening included. Without it an apply could land
                // between the staleness check and the query, and the connection this read is
                // using would be looking at pages that no longer exist.
                let access = ctx.modes.access(shard);
                let guard = access.read();
                let result = match ensure_shard(&mut open, &ctx, shard) {
                    Ok(conn) => run_with_deadline(conn, &statement, ctx.timeout, &ctx.counters),
                    Err(e) => Err(e),
                };
                drop(guard);
                let _ = reply.send(result);
            }
            Job::Stream {
                shard,
                statement,
                tx,
            } => {
                ctx.counters.queries.fetch_add(1, Ordering::Relaxed);
                // Same discipline as a plain query: the access guard is held for the whole
                // stream so no apply can land mid-read, and the deadline still bounds a
                // runaway. A long stream holds a reader thread for its duration, which the
                // deadline caps.
                let access = ctx.modes.access(shard);
                let guard = access.read();
                let outcome = match ensure_shard(&mut open, &ctx, shard) {
                    Ok(conn) => {
                        stream_with_deadline(conn, &statement, ctx.timeout, &ctx.counters, &tx)
                    }
                    Err(e) => Err(e),
                };
                drop(guard);
                let _ = match outcome {
                    Ok(()) => tx.send(StreamMsg::Done),
                    Err(e) => tx.send(StreamMsg::Failed(e.to_string())),
                };
            }
            Job::Quiesce { shard, reply } => {
                let closed = open.remove(shard).is_some();
                let _ = reply.send(closed);
            }
        }
    }
}

fn ensure_shard<'a>(
    open: &'a mut Lru<(Connection, u64)>,
    ctx: &ThreadCtx,
    shard: ShardId,
) -> Result<&'a mut Connection> {
    ctx.modes.check_may_read(shard)?;

    // A connection cached before the last apply is looking at pages that have since been
    // rewritten — or, after a snapshot install, at a file that has been unlinked. Closing it
    // is what clears both; nothing else would.
    let access = ctx.modes.access(shard);
    let generation = access.generation();
    if open
        .peek(shard)
        .is_some_and(|(_, cached)| *cached != generation)
    {
        open.remove(shard);
        access.note_reopen();
    }

    if !open.contains(shard) && ctx.modes.mode(shard) == super::mode::ShardMode::Led {
        // Cold shard. A read-only connection cannot *create* the database file, so a shard
        // that has never been written to cannot be opened by a reader at all — ask the
        // writer to bring it into existence first. Warm shards skip this entirely, so the
        // round trip is only paid on a cold open.
        if let Some(writers) = ctx.writers.upgrade() {
            writers.ensure_open(shard)?;
        }
    }

    let path = shard.path(&ctx.dir);
    let profile = ctx.profile.clone();
    let writers = ctx.writers.clone();

    let (entry, event) = open.get_or_open(shard, move || -> Result<(Connection, u64)> {
        match open_reader_existing(&path, &profile) {
            Ok(c) => Ok((c, generation)),
            Err(first) => {
                // The shard may have gone away between `ensure_open` and here — another
                // thread's eviction plus an unclean close can leave a `-wal` a read-only
                // connection cannot attach to. Re-establish and retry once; a second
                // failure is real.
                let Some(w) = writers.upgrade() else {
                    return Err(first);
                };
                w.ensure_open(shard)?;
                open_reader_existing(&path, &profile).map(|c| (c, generation))
            }
        }
    })?;

    if event.opened {
        ctx.counters.shard_opens.fetch_add(1, Ordering::Relaxed);
    }
    if event.evicted > 0 {
        ctx.counters
            .shard_evictions
            .fetch_add(event.evicted, Ordering::Relaxed);
    }

    Ok(&mut entry.0)
}

/// Run a streaming read, pushing rows onto `tx`, under the same deadline as a plain query.
fn stream_with_deadline(
    conn: &Connection,
    statement: &Statement,
    timeout: Duration,
    counters: &Counters,
    tx: &SyncSender<StreamMsg>,
) -> Result<()> {
    use crate::storage::exec::StreamItem;
    let start = Instant::now();
    conn.progress_handler(10_000, Some(move || start.elapsed() > timeout))?;

    let result = exec::run_stream(conn, statement, |item| {
        let msg = match item {
            StreamItem::Columns(c) => StreamMsg::Columns(c),
            StreamItem::Row(r) => StreamMsg::Row(r),
        };
        // A send failure means the consumer dropped the receiver — stop reading.
        tx.send(msg).is_ok()
    });

    conn.progress_handler(10_000, None::<fn() -> bool>)?;

    match result {
        Ok(()) => Ok(()),
        Err(SqlError::Logic(msg)) => {
            let _ = tx.send(StreamMsg::Failed(msg));
            Ok(())
        }
        Err(SqlError::Fatal(e)) => {
            if start.elapsed() >= timeout {
                counters.timed_out.fetch_add(1, Ordering::Relaxed);
                Err(Error::QueryTimeout { timeout })
            } else {
                Err(Error::Sqlite(e))
            }
        }
    }
}

fn run_with_deadline(
    conn: &Connection,
    statement: &Statement,
    timeout: Duration,
    counters: &Counters,
) -> Result<Outcome> {
    let start = Instant::now();
    conn.progress_handler(10_000, Some(move || start.elapsed() > timeout))?;
    let result = exec::run(conn, statement);
    conn.progress_handler(10_000, None::<fn() -> bool>)?;

    match result {
        Ok(executed) => Ok(Outcome::Ok(executed)),
        Err(SqlError::Logic(msg)) => Ok(Outcome::Rejected(msg)),
        Err(SqlError::Fatal(e)) => {
            // An interrupt raised by our own deadline is a deliberate timeout, not a
            // machine fault, and must not be reported as one.
            if start.elapsed() >= timeout {
                counters.timed_out.fetch_add(1, Ordering::Relaxed);
                Err(Error::QueryTimeout { timeout })
            } else {
                Err(Error::Sqlite(e))
            }
        }
    }
}
