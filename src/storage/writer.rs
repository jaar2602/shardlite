//! The single writer thread, and group-commit batching.
//!
//! SQLite serializes writes at the file level, so contending writer connections buy
//! nothing but lock thrash and `SQLITE_BUSY` retries. Instead every write for a database
//! funnels through one thread that owns one connection for its lifetime, and callers
//! never see a busy error.
//!
//! Throughput comes from **group commit**: while the writer is inside `COMMIT` paying an
//! fsync, newly submitted requests pile up in the channel, and the next iteration drains
//! all of them into a single transaction. There is deliberately **no linger timer** — the
//! batching is self-tuning. Under low load batches are size 1 and latency is minimal;
//! under high load they grow exactly as much as the fsync cost justifies. An artificial
//! delay would add latency when idle and buy nothing when busy.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;

use rusqlite::{Connection, TransactionBehavior};

use crate::config::PragmaProfile;
use crate::error::{Error, Result};
use crate::storage::exec::{self, Outcome, SqlError};
use crate::storage::open::{WriterOpened, open_writer};

/// Upper bound on requests merged into one transaction.
///
/// This bounds memory and worst-case latency; it is *not* a throughput tuning knob.
/// Group commit tunes itself against the fsync cost.
pub const MAX_REQUESTS_PER_TXN: usize = 64;

type Reply = Result<Vec<Outcome>>;

struct Pending {
    sqls: Vec<String>,
    reply: SyncSender<Reply>,
}

enum Job {
    Write(Pending),
    Shutdown,
}

#[derive(Debug, Default)]
struct Counters {
    batches: AtomicU64,
    requests: AtomicU64,
    max_batch: AtomicU64,
}

/// Observed batching behaviour. `mean_batch > 1` under load is the signature of group
/// commit actually working; if it stays pinned at 1, batching is not happening.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WriterStats {
    pub batches: u64,
    pub requests: u64,
    pub max_batch: u64,
}

impl WriterStats {
    pub fn mean_batch(&self) -> f64 {
        if self.batches == 0 {
            0.0
        } else {
            self.requests as f64 / self.batches as f64
        }
    }
}

/// Owns the writer thread. Dropping it shuts the thread down and joins it.
pub struct Writer {
    tx: Option<mpsc::Sender<Job>>,
    thread: Option<JoinHandle<()>>,
    counters: Arc<Counters>,
}

/// A cheap, cloneable handle for submitting writes from any thread.
#[derive(Clone)]
pub struct WriterHandle {
    tx: mpsc::Sender<Job>,
}

impl Writer {
    /// Open the database and start its writer thread.
    ///
    /// The connection is opened here rather than inside the thread so the caller receives
    /// the [`WriterOpened`] token needed to open readers.
    pub fn spawn(path: &Path, profile: &PragmaProfile) -> Result<(Self, WriterOpened)> {
        let (conn, token) = open_writer(path, profile)?;
        let (tx, rx) = mpsc::channel();
        let counters = Arc::new(Counters::default());

        let thread = {
            let counters = Arc::clone(&counters);
            std::thread::Builder::new()
                .name("meshdb-writer".to_string())
                .spawn(move || writer_loop(conn, rx, &counters))
                .expect("spawning the writer thread")
        };

        Ok((
            Self {
                tx: Some(tx),
                thread: Some(thread),
                counters,
            },
            token,
        ))
    }

    pub fn handle(&self) -> WriterHandle {
        WriterHandle {
            tx: self.tx.clone().expect("writer is running"),
        }
    }

    pub fn stats(&self) -> WriterStats {
        WriterStats {
            batches: self.counters.batches.load(Ordering::Relaxed),
            requests: self.counters.requests.load(Ordering::Relaxed),
            max_batch: self.counters.max_batch.load(Ordering::Relaxed),
        }
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(Job::Shutdown);
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl WriterHandle {
    /// Submit statements and block until the batch containing them commits.
    ///
    /// The statements in one call are applied in order within a single transaction, but
    /// they may share that transaction with other callers' statements.
    pub fn execute(&self, sqls: Vec<String>) -> Result<Vec<Outcome>> {
        let (reply_tx, reply_rx) = sync_channel(1);
        self.tx
            .send(Job::Write(Pending {
                sqls,
                reply: reply_tx,
            }))
            .map_err(|_| Error::WriterGone)?;
        reply_rx.recv().map_err(|_| Error::WriterGone)?
    }

    pub fn execute_one(&self, sql: &str) -> Result<Outcome> {
        let mut out = self.execute(vec![sql.to_string()])?;
        Ok(out.pop().expect("one request yields one outcome"))
    }
}

fn writer_loop(mut conn: Connection, rx: Receiver<Job>, counters: &Counters) {
    loop {
        // Block for the first job, then take whatever else has already arrived. The
        // `try_recv` drain is what produces group commit: everything that queued while
        // the previous COMMIT was fsyncing gets absorbed here.
        let first = match rx.recv() {
            Ok(Job::Write(p)) => p,
            Ok(Job::Shutdown) | Err(_) => return,
        };

        let mut batch = vec![first];
        let mut requests: usize = batch[0].sqls.len();
        let mut stopping = false;

        while requests < MAX_REQUESTS_PER_TXN {
            match rx.try_recv() {
                Ok(Job::Write(p)) => {
                    requests += p.sqls.len();
                    batch.push(p);
                }
                Ok(Job::Shutdown) => {
                    // Honour the shutdown, but only after the batch in hand commits —
                    // dropping it would lose writes callers are still blocked on.
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

        counters.batches.fetch_add(1, Ordering::Relaxed);
        counters
            .requests
            .fetch_add(requests as u64, Ordering::Relaxed);
        counters
            .max_batch
            .fetch_max(requests as u64, Ordering::Relaxed);

        apply_batch(&mut conn, batch);

        if stopping {
            return;
        }
    }
}

/// Apply one batch as a single transaction.
///
/// Each request gets its own SAVEPOINT, so one caller's constraint violation rolls back
/// only that request — the rest of the batch still commits. A [`SqlError::Fatal`] aborts
/// the entire transaction and every caller in it is told so.
fn apply_batch(conn: &mut Connection, batch: Vec<Pending>) {
    // IMMEDIATE takes the write lock at BEGIN rather than on first write, so a problem
    // surfaces before any work is done instead of midway through the batch.
    let mut tx = match conn.transaction_with_behavior(TransactionBehavior::Immediate) {
        Ok(tx) => tx,
        Err(e) => return reply_all(batch, &e.to_string()),
    };

    let mut results: Vec<Vec<Outcome>> = Vec::with_capacity(batch.len());
    let mut fatal: Option<String> = None;

    'outer: for pending in &batch {
        let mut outcomes = Vec::with_capacity(pending.sqls.len());

        for sql in &pending.sqls {
            let sp = match tx.savepoint() {
                Ok(sp) => sp,
                Err(e) => {
                    fatal = Some(e.to_string());
                    break 'outer;
                }
            };

            match exec::run(&sp, sql) {
                Ok(executed) => {
                    if let Err(e) = sp.commit() {
                        fatal = Some(e.to_string());
                        break 'outer;
                    }
                    outcomes.push(Outcome::Ok(executed));
                }
                Err(SqlError::Logic(msg)) => {
                    // Dropping the savepoint rolls it back, leaving earlier requests in
                    // this transaction untouched.
                    drop(sp);
                    outcomes.push(Outcome::Rejected(msg));
                }
                Err(SqlError::Fatal(e)) => {
                    fatal = Some(e.to_string());
                    break 'outer;
                }
            }
        }

        results.push(outcomes);
    }

    if let Some(msg) = fatal {
        drop(tx); // rolls back; nothing from this batch lands
        return reply_all(batch, &msg);
    }

    if let Err(e) = tx.commit() {
        return reply_all(batch, &e.to_string());
    }

    for (pending, outcomes) in batch.into_iter().zip(results) {
        // A caller that gave up is not an error worth reporting.
        let _ = pending.reply.send(Ok(outcomes));
    }
}

fn reply_all(batch: Vec<Pending>, msg: &str) {
    for pending in batch {
        let _ = pending.reply.send(Err(Error::BatchAborted {
            reason: msg.to_string(),
        }));
    }
}
