//! The reader pool.
//!
//! WAL mode is what makes this worth having: readers do not block the writer and the
//! writer does not block readers, so N reader threads genuinely run in parallel against
//! a database being written to.
//!
//! Each thread owns one `SQLITE_OPEN_READ_ONLY` connection for its lifetime and they all
//! race to receive from one shared queue, which is self-balancing — an idle thread takes
//! the next job. The queue is **bounded**: when it fills, queries are rejected rather
//! than queued, so overload shows up as a fast, honest error instead of unbounded memory
//! growth and climbing latency.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use rusqlite::Connection;

use crate::config::{PragmaProfile, ReaderPoolConfig};
use crate::error::{Error, Result};
use crate::storage::exec::{self, Outcome, SqlError};
use crate::storage::open::{WriterOpened, open_reader};

struct Job {
    sql: String,
    reply: SyncSender<Result<Outcome>>,
}

#[derive(Debug, Default)]
struct Counters {
    queries: AtomicU64,
    rejected_busy: AtomicU64,
    timed_out: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReaderStats {
    pub queries: u64,
    pub rejected_busy: u64,
    pub timed_out: u64,
    pub threads: usize,
}

pub struct ReaderPool {
    tx: Option<flume::Sender<Job>>,
    threads: Vec<JoinHandle<()>>,
    counters: Arc<Counters>,
    n_threads: usize,
}

impl ReaderPool {
    /// Start the pool.
    ///
    /// Takes a [`WriterOpened`] token because a read-only connection cannot create the
    /// `-wal`/`-shm` sidecars — the writer must have opened this database first.
    pub fn spawn(
        path: &Path,
        profile: &PragmaProfile,
        cfg: &ReaderPoolConfig,
        token: &WriterOpened,
    ) -> Result<Self> {
        assert!(cfg.threads > 0, "reader pool needs at least one thread");

        let (tx, rx) = flume::bounded::<Job>(cfg.queue_capacity);
        let counters = Arc::new(Counters::default());
        let mut threads = Vec::with_capacity(cfg.threads);

        for i in 0..cfg.threads {
            // Opened here rather than in the thread so a bad profile fails `spawn`
            // instead of silently killing a worker.
            let conn = open_reader(path, profile, token)?;
            let rx = rx.clone();
            let counters = Arc::clone(&counters);
            let timeout = cfg.query_timeout;

            let handle = std::thread::Builder::new()
                .name(format!("meshdb-reader-{i}"))
                .spawn(move || reader_loop(conn, rx, &counters, timeout))
                .expect("spawning a reader thread");
            threads.push(handle);
        }

        Ok(Self {
            tx: Some(tx),
            threads,
            counters,
            n_threads: cfg.threads,
        })
    }

    /// Run a read-only statement on the next free reader.
    ///
    /// Returns [`Error::ReaderPoolBusy`] immediately if the queue is full.
    pub fn query(&self, sql: &str) -> Result<Outcome> {
        let (reply_tx, reply_rx) = sync_channel(1);
        let tx = self.tx.as_ref().ok_or(Error::ReaderPoolGone)?;

        // `try_send`, never `send`: blocking here would convert backpressure into an
        // unbounded latency queue held in caller threads instead of in memory.
        tx.try_send(Job {
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

    pub fn stats(&self) -> ReaderStats {
        ReaderStats {
            queries: self.counters.queries.load(Ordering::Relaxed),
            rejected_busy: self.counters.rejected_busy.load(Ordering::Relaxed),
            timed_out: self.counters.timed_out.load(Ordering::Relaxed),
            threads: self.n_threads,
        }
    }
}

impl Drop for ReaderPool {
    fn drop(&mut self) {
        // Dropping the only sender disconnects the queue, which ends every reader loop.
        self.tx.take();
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

fn reader_loop(conn: Connection, rx: flume::Receiver<Job>, counters: &Counters, timeout: Duration) {
    while let Ok(job) = rx.recv() {
        counters.queries.fetch_add(1, Ordering::Relaxed);
        let result = run_with_deadline(&conn, &job.sql, timeout, counters);
        let _ = job.reply.send(result);
    }
}

/// Execute one statement, aborting it if it outruns `timeout`.
///
/// The progress handler is SQLite's own interrupt point — it fires every N virtual
/// machine instructions, so it can stop a runaway query that never returns a row, which
/// a timeout on the result stream could not.
fn run_with_deadline(
    conn: &Connection,
    sql: &str,
    timeout: Duration,
    counters: &Counters,
) -> Result<Outcome> {
    let start = Instant::now();
    conn.progress_handler(10_000, Some(move || start.elapsed() > timeout))?;

    let result = exec::run(conn, sql);

    // Clear it so the handler never applies to an unrelated later query. Propagated
    // rather than ignored: a handler left installed with a stale deadline would abort
    // every subsequent query on this connection, which is worse than losing one result.
    conn.progress_handler(10_000, None::<fn() -> bool>)?;

    match result {
        Ok(executed) => Ok(Outcome::Ok(executed)),
        Err(SqlError::Logic(msg)) => Ok(Outcome::Rejected(msg)),
        Err(SqlError::Fatal(e)) => {
            // An interrupt raised by our own progress handler is a deliberate timeout,
            // not a machine fault, and must not be reported as one.
            if start.elapsed() >= timeout {
                counters.timed_out.fetch_add(1, Ordering::Relaxed);
                Err(Error::QueryTimeout { timeout })
            } else {
                Err(Error::Sqlite(e))
            }
        }
    }
}
