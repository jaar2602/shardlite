//! Write throughput: does group commit actually pay, and can the win be attributed to it?
//!
//! Run with `cargo bench --bench write_throughput` (release; a debug build measures rustc,
//! not SQLite).
//!
//! A custom harness rather than criterion: criterion measures per-iteration latency of a
//! closure, which is the wrong shape for sustained throughput under N concurrent callers.
//!
//! Three variants, and the third is the one that makes the result interpretable:
//!
//! - **A — naive contending writers.** N threads, each with its own read-write connection
//!   to the same file, relying on `busy_timeout`. The strawman the design rejects.
//! - **B — batched single writer.** The real design: one writer thread, group commit.
//! - **B-nobatch — single writer, `max_batch = 1`.** Same serialization, no batching.
//!
//! Without B-nobatch, B beating A proves only that serializing beats contending. The
//! B / B-nobatch ratio is what isolates batching's contribution, and it should track the
//! observed mean batch size.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use meshdb::config::{PragmaProfile, Synchronous};
use meshdb::shard::{ShardConfig, ShardId, ShardManager};
use meshdb::storage::exec::{Statement, Value};
use meshdb::storage::open::open_writer;

const S0: ShardId = ShardId(0);
const TOTAL_WRITES: usize = 4_000;
const CONCURRENCY: &[usize] = &[1, 4, 16, 64];

struct Sample {
    writes: usize,
    elapsed: Duration,
    latencies: Vec<Duration>,
    mean_batch: f64,
    errors: usize,
}

impl Sample {
    fn per_sec(&self) -> f64 {
        self.writes as f64 / self.elapsed.as_secs_f64()
    }

    /// Percentile from a sorted latency vector.
    fn pct(&self, p: f64) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }
        let idx = ((self.latencies.len() as f64 - 1.0) * p).round() as usize;
        self.latencies[idx]
    }
}

fn ddl() -> &'static str {
    "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, who INTEGER, pad TEXT) STRICT"
}

fn insert(who: usize) -> Statement {
    Statement::with_params(
        "INSERT INTO t (who, pad) VALUES (?1, ?2)",
        vec![Value::Integer(who as i64), Value::Text("x".repeat(64))],
    )
}

/// Variant A: every thread holds its own read-write connection and they fight for the lock.
fn variant_a(dir: &Path, threads: usize, sync: Synchronous) -> Sample {
    let path = dir.join("a.db");
    let mut profile = PragmaProfile::writer_floor();
    profile.synchronous = sync;

    let setup = open_writer(&path, &profile).unwrap();
    setup.execute_batch(ddl()).unwrap();
    drop(setup);

    let per_thread = TOTAL_WRITES / threads;
    let errors = Arc::new(AtomicUsize::new(0));
    let all: Arc<std::sync::Mutex<Vec<Duration>>> = Arc::new(std::sync::Mutex::new(Vec::new()));

    let start = Instant::now();
    std::thread::scope(|s| {
        for who in 0..threads {
            let path = path.clone();
            let profile = profile.clone();
            let errors = Arc::clone(&errors);
            let all = Arc::clone(&all);
            s.spawn(move || {
                let conn = match open_writer(&path, &profile) {
                    Ok(c) => c,
                    Err(_) => {
                        // Counted, not fatal: a contending writer failing to open at all is
                        // a legitimate result for this variant.
                        errors.fetch_add(per_thread, Ordering::Relaxed);
                        return;
                    }
                };
                let mut mine = Vec::with_capacity(per_thread);
                for _ in 0..per_thread {
                    let t0 = Instant::now();
                    let r = conn.execute(
                        "INSERT INTO t (who, pad) VALUES (?1, ?2)",
                        meshdb::rusqlite::params![who as i64, "x".repeat(64)],
                    );
                    mine.push(t0.elapsed());
                    if r.is_err() {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                all.lock().unwrap().extend(mine);
            });
        }
    });
    let elapsed = start.elapsed();

    let mut latencies = Arc::try_unwrap(all).unwrap().into_inner().unwrap();
    latencies.sort();
    Sample {
        writes: per_thread * threads,
        elapsed,
        latencies,
        mean_batch: 1.0,
        errors: errors.load(Ordering::Relaxed),
    }
}

/// Variants B and B-nobatch: one writer thread; `max_batch` decides whether it batches.
fn variant_fleet(dir: &Path, threads: usize, sync: Synchronous, max_batch: usize) -> Sample {
    let cfg = ShardConfig {
        shard_count: 1,
        writer_threads: 1,
        max_batch,
        // Deep enough that shedding never distorts the measurement.
        write_queue_depth: 65_536,
        ..ShardConfig::floor()
    };
    let mut writer = PragmaProfile::writer_shard();
    writer.synchronous = sync;
    let m = Arc::new(
        ShardManager::open_with_profiles(dir, cfg, writer, PragmaProfile::reader_shard()).unwrap(),
    );
    m.execute_one(S0, ddl()).unwrap();

    let per_thread = TOTAL_WRITES / threads;
    let errors = Arc::new(AtomicUsize::new(0));
    let all: Arc<std::sync::Mutex<Vec<Duration>>> = Arc::new(std::sync::Mutex::new(Vec::new()));

    let start = Instant::now();
    std::thread::scope(|s| {
        for who in 0..threads {
            let m = Arc::clone(&m);
            let errors = Arc::clone(&errors);
            let all = Arc::clone(&all);
            s.spawn(move || {
                let mut mine = Vec::with_capacity(per_thread);
                for _ in 0..per_thread {
                    let t0 = Instant::now();
                    let r = m.execute_one(S0, insert(who));
                    mine.push(t0.elapsed());
                    if r.is_err() {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                all.lock().unwrap().extend(mine);
            });
        }
    });
    let elapsed = start.elapsed();

    let stats = m.writer_stats();
    let mut latencies = Arc::try_unwrap(all).unwrap().into_inner().unwrap();
    latencies.sort();
    Sample {
        writes: per_thread * threads,
        elapsed,
        latencies,
        mean_batch: stats.mean_batch(),
        errors: errors.load(Ordering::Relaxed),
    }
}

fn row(name: &str, threads: usize, s: &Sample) {
    println!(
        "{name:<12} C={threads:<4} {:>10.0}/s  p50={:>8.2}ms p99={:>8.2}ms p999={:>8.2}ms  \
         mean_batch={:>6.2}  errors={}",
        s.per_sec(),
        s.pct(0.50).as_secs_f64() * 1000.0,
        s.pct(0.99).as_secs_f64() * 1000.0,
        s.pct(0.999).as_secs_f64() * 1000.0,
        s.mean_batch,
        s.errors
    );
}

fn main() {
    println!("meshdb write throughput — {TOTAL_WRITES} writes per configuration");
    println!(
        "host: {} cores (NOT the 1-CPU floor profile)\n",
        num_threads()
    );

    for sync in [Synchronous::Full, Synchronous::Normal] {
        println!("=== synchronous = {sync:?} ===");
        let mut b_at: Vec<(usize, f64)> = Vec::new();
        let mut nb_at: Vec<(usize, f64)> = Vec::new();

        for &c in CONCURRENCY {
            let tmp = tempfile::tempdir().unwrap();
            let a = variant_a(tmp.path(), c, sync);
            row("A-contend", c, &a);

            let tmp = tempfile::tempdir().unwrap();
            let b = variant_fleet(tmp.path(), c, sync, 64);
            row("B-batched", c, &b);
            b_at.push((c, b.per_sec()));

            let tmp = tempfile::tempdir().unwrap();
            let nb = variant_fleet(tmp.path(), c, sync, 1);
            row("B-nobatch", c, &nb);
            nb_at.push((c, nb.per_sec()));

            println!(
                "             ratio B/B-nobatch = {:.2}x   (should track mean_batch = {:.2})",
                b.per_sec() / nb.per_sec().max(1.0),
                b.mean_batch
            );
            println!();
        }
    }
}

fn num_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
