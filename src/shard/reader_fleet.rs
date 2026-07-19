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
use crate::storage::exec::{self, Outcome, SqlError};

struct Job {
    shard: ShardId,
    sql: String,
    reply: SyncSender<Result<Outcome>>,
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

    /// Run a read-only statement against one shard on the next free reader.
    pub fn query(&self, shard: ShardId, sql: &str) -> Result<Outcome> {
        let (reply_tx, reply_rx) = sync_channel(1);
        let tx = self.tx.as_ref().ok_or(Error::ReaderPoolGone)?;

        // `try_send`, never `send`: blocking would convert backpressure into an unbounded
        // latency queue held in caller threads instead of in memory.
        tx.try_send(Job {
            shard,
            sql: sql.to_string(),
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
}

fn reader_loop(rx: flume::Receiver<Job>, ctx: ThreadCtx) {
    let mut open: Lru<Connection> = Lru::new(ctx.capacity);

    while let Ok(job) = rx.recv() {
        ctx.counters.queries.fetch_add(1, Ordering::Relaxed);

        let result = match ensure_shard(&mut open, &ctx, job.shard) {
            Ok(conn) => run_with_deadline(conn, &job.sql, ctx.timeout, &ctx.counters),
            Err(e) => Err(e),
        };
        let _ = job.reply.send(result);
    }
}

fn ensure_shard<'a>(
    open: &'a mut Lru<Connection>,
    ctx: &ThreadCtx,
    shard: ShardId,
) -> Result<&'a mut Connection> {
    if !open.contains(shard) {
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

    let (conn, event) = open.get_or_open(shard, move || -> Result<Connection> {
        match open_reader_unchecked(&path, &profile) {
            Ok(c) => Ok(c),
            Err(first) => {
                // The shard may have gone away between `ensure_open` and here — another
                // thread's eviction plus an unclean close can leave a `-wal` a read-only
                // connection cannot attach to. Re-establish and retry once; a second
                // failure is real.
                let Some(w) = writers.upgrade() else {
                    return Err(first);
                };
                w.ensure_open(shard)?;
                open_reader_unchecked(&path, &profile)
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

    Ok(conn)
}

/// Open a reader without a [`crate::storage::open::WriterOpened`] token.
///
/// The token proves the writer opened this database, which is checked at the type level in
/// the single-database path. Here the same guarantee is established dynamically by
/// [`WriterFleet::ensure_open`] — a token cannot be threaded through, because the writer
/// that opened the shard lives on another thread and may evict it at any time.
fn open_reader_unchecked(path: &Path, profile: &PragmaProfile) -> Result<Connection> {
    use crate::storage::pragma;
    use rusqlite::OpenFlags;

    let path_str = path.display().to_string();
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = Connection::open_with_flags(path, flags).map_err(|source| Error::Open {
        path: path_str.clone(),
        source,
    })?;
    pragma::apply(&conn, profile, &path_str)?;
    pragma::verify(&conn, profile)?;
    Ok(conn)
}

fn run_with_deadline(
    conn: &Connection,
    sql: &str,
    timeout: Duration,
    counters: &Counters,
) -> Result<Outcome> {
    let start = Instant::now();
    conn.progress_handler(10_000, Some(move || start.elapsed() > timeout))?;
    let result = exec::run(conn, sql);
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
