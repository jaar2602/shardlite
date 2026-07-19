//! Step 3 acceptance: readers run concurrently with each other and with the writer,
//! the bounded queue sheds load instead of growing, and runaway queries are cancelled.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use meshdb::config::{PragmaProfile, ReaderPoolConfig};
use meshdb::db::Db;
use meshdb::error::Error;
use meshdb::storage::exec::{Executed, Outcome};
use meshdb::storage::reader::ReaderPool;
use meshdb::storage::writer::Writer;
use meshdb::storage::{Value, WriterOpened};
use tempfile::TempDir;

fn rows(o: &Outcome) -> &Vec<Vec<Value>> {
    match o {
        Outcome::Ok(Executed::Rows(r)) => &r.rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn scalar(o: &Outcome) -> i64 {
    match &rows(o)[0][0] {
        Value::Integer(i) => *i,
        v => panic!("expected an integer, got {v:?}"),
    }
}

fn seeded(dir: &TempDir, n: i64) -> (Writer, WriterOpened) {
    let path = dir.path().join("shard_0.db");
    let (w, token) = Writer::spawn(&path, &PragmaProfile::writer_floor()).unwrap();
    let h = w.handle();
    h.execute_one("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT")
        .unwrap();
    let stmts: Vec<String> = (1..=n)
        .map(|i| format!("INSERT INTO t VALUES ({i}, 'v{i}')"))
        .collect();
    for chunk in stmts.chunks(50) {
        h.execute(chunk.to_vec()).unwrap();
    }
    (w, token)
}

#[test]
fn many_threads_read_concurrently() {
    const THREADS: usize = 12;
    const PER_THREAD: usize = 30;

    let dir = TempDir::new().unwrap();
    let (_w, token) = seeded(&dir, 100);
    let pool = ReaderPool::spawn(
        &dir.path().join("shard_0.db"),
        &PragmaProfile::reader_floor(),
        &ReaderPoolConfig::floor(),
        &token,
    )
    .unwrap();

    let pool = Arc::new(pool);
    let failures = Arc::new(AtomicUsize::new(0));

    std::thread::scope(|s| {
        for _ in 0..THREADS {
            let pool = Arc::clone(&pool);
            let failures = Arc::clone(&failures);
            s.spawn(move || {
                for _ in 0..PER_THREAD {
                    // Retry on deliberate backpressure; anything else is a real failure.
                    loop {
                        match pool.query("SELECT count(*) FROM t") {
                            Ok(o) => {
                                if scalar(&o) != 100 {
                                    failures.fetch_add(1, Ordering::Relaxed);
                                }
                                break;
                            }
                            Err(Error::ReaderPoolBusy) => std::thread::yield_now(),
                            Err(_) => {
                                failures.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                        }
                    }
                }
            });
        }
    });

    assert_eq!(failures.load(Ordering::Relaxed), 0);
    let stats = pool.stats();
    assert_eq!(stats.queries, (THREADS * PER_THREAD) as u64);
    assert_eq!(stats.timed_out, 0);
}

#[test]
fn readers_do_not_block_the_writer() {
    // The whole reason for WAL: sustained reads and writes proceed together. If readers
    // took the write lock this would deadlock or produce SQLITE_BUSY.
    let dir = TempDir::new().unwrap();
    let (w, token) = seeded(&dir, 50);
    let pool = ReaderPool::spawn(
        &dir.path().join("shard_0.db"),
        &PragmaProfile::reader_floor(),
        &ReaderPoolConfig::floor(),
        &token,
    )
    .unwrap();

    let pool = Arc::new(pool);
    let read_errors = Arc::new(AtomicUsize::new(0));
    let write_errors = Arc::new(AtomicUsize::new(0));

    std::thread::scope(|s| {
        for _ in 0..4 {
            let pool = Arc::clone(&pool);
            let errs = Arc::clone(&read_errors);
            s.spawn(move || {
                for _ in 0..100 {
                    match pool.query("SELECT count(*) FROM t") {
                        Ok(_) | Err(Error::ReaderPoolBusy) => {}
                        Err(_) => {
                            errs.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });
        }
        for who in 0..4 {
            let h = w.handle();
            let errs = Arc::clone(&write_errors);
            s.spawn(move || {
                for i in 0..100 {
                    let id = 1000 + who * 1000 + i;
                    if !matches!(
                        h.execute_one(&format!("INSERT INTO t VALUES ({id}, 'x')")),
                        Ok(Outcome::Ok(_))
                    ) {
                        errs.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });

    assert_eq!(read_errors.load(Ordering::Relaxed), 0, "reads failed");
    assert_eq!(write_errors.load(Ordering::Relaxed), 0, "writes failed");
}

#[test]
fn a_full_queue_sheds_load_instead_of_growing() {
    // Backpressure must surface as a fast error. A capacity-1 pool with one slow-ish
    // reader thread is the easiest way to force the queue full.
    let dir = TempDir::new().unwrap();
    let (_w, token) = seeded(&dir, 2_000);

    let cfg = ReaderPoolConfig {
        threads: 1,
        queue_capacity: 1,
        query_timeout: Duration::from_secs(30),
    };
    let pool = Arc::new(
        ReaderPool::spawn(
            &dir.path().join("shard_0.db"),
            &PragmaProfile::reader_floor(),
            &cfg,
            &token,
        )
        .unwrap(),
    );

    let busy = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|s| {
        for _ in 0..8 {
            let pool = Arc::clone(&pool);
            let busy = Arc::clone(&busy);
            s.spawn(move || {
                for _ in 0..50 {
                    // A cross join over 2000 rows is enough work to keep the single
                    // reader occupied while the queue fills behind it.
                    if let Err(Error::ReaderPoolBusy) =
                        pool.query("SELECT count(*) FROM t a, t b WHERE a.id < b.id")
                    {
                        busy.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });

    assert!(
        busy.load(Ordering::Relaxed) > 0,
        "expected the bounded queue to reject at least one query"
    );
    assert_eq!(
        pool.stats().rejected_busy,
        busy.load(Ordering::Relaxed) as u64
    );
}

#[test]
fn a_runaway_query_is_cancelled_at_the_deadline() {
    let dir = TempDir::new().unwrap();
    let (_w, token) = seeded(&dir, 500);

    let cfg = ReaderPoolConfig {
        threads: 1,
        queue_capacity: 8,
        query_timeout: Duration::from_millis(150),
    };
    let pool = ReaderPool::spawn(
        &dir.path().join("shard_0.db"),
        &PragmaProfile::reader_floor(),
        &cfg,
        &token,
    )
    .unwrap();

    // A three-way cross join over 500 rows is ~125M candidate rows: far beyond 150ms.
    let start = Instant::now();
    let result = pool.query("SELECT count(*) FROM t a, t b, t c WHERE a.id < b.id AND b.id < c.id");
    let elapsed = start.elapsed();

    match result {
        Err(Error::QueryTimeout { .. }) => {}
        other => panic!("expected QueryTimeout, got {other:?}"),
    }
    assert!(
        elapsed < Duration::from_secs(5),
        "cancellation should be prompt, took {elapsed:?}"
    );
    assert_eq!(pool.stats().timed_out, 1);

    // The connection must remain usable after an interrupt.
    let after = pool.query("SELECT count(*) FROM t").unwrap();
    assert_eq!(scalar(&after), 500);
}

#[test]
fn db_routes_reads_to_the_pool_and_writes_to_the_writer() {
    let dir = TempDir::new().unwrap();
    let db = Db::open(&dir.path().join("shard_0.db")).unwrap();

    db.run("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    db.run("INSERT INTO t VALUES (1)").unwrap();
    db.run("INSERT INTO t VALUES (2)").unwrap();

    let before = db.reader_stats().queries;
    let out = db.run("SELECT count(*) FROM t").unwrap();
    assert_eq!(scalar(&out), 2);

    assert_eq!(
        db.reader_stats().queries,
        before + 1,
        "a SELECT should be served by the reader pool"
    );
    assert_eq!(
        db.writer_stats().requests,
        3,
        "only the three writes should have reached the writer"
    );
}
