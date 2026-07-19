//! Properties of the writer and reader fleets.
//!
//! These were previously proven against the single-database `Writer`/`ReaderPool`, which
//! the sharded fleets replaced. They are the behaviours that matter regardless of shard
//! count, so they moved here rather than being lost with those modules.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use meshdb::config::{CheckpointConfig, PragmaProfile, ReaderPoolConfig};
use meshdb::error::Error;
use meshdb::shard::reader_fleet::ReaderFleet;
use meshdb::shard::writer_fleet::WriterFleet;
use meshdb::shard::{ShardConfig, ShardId, ShardManager};
use meshdb::storage::Value;
use meshdb::storage::exec::{Executed, Outcome};
use tempfile::TempDir;

const S0: ShardId = ShardId(0);

fn scalar(o: &Outcome) -> i64 {
    match o {
        Outcome::Ok(Executed::Rows(r)) => match &r.rows[0][0] {
            Value::Integer(i) => *i,
            v => panic!("expected an integer, got {v:?}"),
        },
        other => panic!("expected rows, got {other:?}"),
    }
}

fn single_shard(dir: &TempDir) -> ShardManager {
    let cfg = ShardConfig {
        shard_count: 1,
        ..ShardConfig::floor()
    };
    ShardManager::open(dir.path(), cfg).unwrap()
}

#[test]
fn writes_apply_in_order() {
    let dir = TempDir::new().unwrap();
    let m = single_shard(&dir);
    m.execute_one(S0, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT")
        .unwrap();
    for i in 1..=5 {
        m.execute_one(S0, &format!("INSERT INTO t VALUES ({i}, 'v{i}')"))
            .unwrap();
    }
    assert_eq!(scalar(&m.query(S0, "SELECT count(*) FROM t").unwrap()), 5);
}

#[test]
fn a_failed_request_does_not_roll_back_its_batch_peers() {
    // Savepoint isolation: request 3 violates a constraint and 1, 2, 4, 5 must still land.
    let dir = TempDir::new().unwrap();
    let m = single_shard(&dir);
    m.execute_one(S0, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    m.execute_one(S0, "INSERT INTO t VALUES (3)").unwrap();

    let outcomes = m
        .execute(
            S0,
            vec![
                "INSERT INTO t VALUES (1)".into(),
                "INSERT INTO t VALUES (2)".into(),
                "INSERT INTO t VALUES (3)".into(), // duplicate primary key
                "INSERT INTO t VALUES (4)".into(),
                "INSERT INTO t VALUES (5)".into(),
            ],
        )
        .unwrap();

    assert_eq!(outcomes.len(), 5);
    assert!(matches!(outcomes[0], Outcome::Ok(_)));
    assert!(matches!(outcomes[1], Outcome::Ok(_)));
    assert!(
        matches!(outcomes[2], Outcome::Rejected(_)),
        "{:?}",
        outcomes[2]
    );
    assert!(matches!(outcomes[3], Outcome::Ok(_)));
    assert!(matches!(outcomes[4], Outcome::Ok(_)));

    assert_eq!(scalar(&m.query(S0, "SELECT count(*) FROM t").unwrap()), 5);
}

#[test]
fn a_rejected_statement_is_not_an_error() {
    let dir = TempDir::new().unwrap();
    let m = single_shard(&dir);

    match m.execute_one(S0, "INSERT INTO does_not_exist VALUES (1)") {
        Ok(Outcome::Rejected(msg)) => assert!(msg.contains("does_not_exist"), "{msg}"),
        other => panic!("expected Rejected, got {other:?}"),
    }
    match m.execute_one(S0, "THIS IS NOT SQL") {
        Ok(Outcome::Rejected(_)) => {}
        other => panic!("expected Rejected for a syntax error, got {other:?}"),
    }
}

#[test]
fn concurrent_writers_all_succeed_without_busy_errors() {
    const THREADS: usize = 16;
    const PER_THREAD: usize = 25;

    let dir = TempDir::new().unwrap();
    let m = Arc::new(single_shard(&dir));
    m.execute_one(
        S0,
        "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, who INTEGER) STRICT",
    )
    .unwrap();

    let errors = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|s| {
        for who in 0..THREADS {
            let m = Arc::clone(&m);
            let errors = Arc::clone(&errors);
            s.spawn(move || {
                for _ in 0..PER_THREAD {
                    if !matches!(
                        m.execute_one(S0, &format!("INSERT INTO t (who) VALUES ({who})")),
                        Ok(Outcome::Ok(_))
                    ) {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });

    assert_eq!(errors.load(Ordering::Relaxed), 0);
    assert_eq!(
        scalar(&m.query(S0, "SELECT count(*) FROM t").unwrap()),
        (THREADS * PER_THREAD) as i64
    );
}

#[test]
fn group_commit_actually_batches_under_load() {
    // If this fails the writer is committing one statement per transaction, and the whole
    // throughput argument for the design is wrong.
    const THREADS: usize = 16;
    const PER_THREAD: usize = 50;

    let dir = TempDir::new().unwrap();
    let m = Arc::new(single_shard(&dir));
    m.execute_one(
        S0,
        "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, who INTEGER) STRICT",
    )
    .unwrap();

    std::thread::scope(|s| {
        for who in 0..THREADS {
            let m = Arc::clone(&m);
            s.spawn(move || {
                for _ in 0..PER_THREAD {
                    let _ = m.execute_one(S0, &format!("INSERT INTO t (who) VALUES ({who})"));
                }
            });
        }
    });

    let stats = m.writer_stats();
    let mean = stats.requests as f64 / stats.batches.max(1) as f64;
    println!(
        "batches={} requests={} max_batch={} mean={mean:.2}",
        stats.batches, stats.requests, stats.max_batch
    );
    assert!(
        stats.max_batch > 1,
        "group commit never merged anything: max_batch={} over {} requests",
        stats.max_batch,
        stats.requests
    );
}

#[test]
fn readers_do_not_block_the_writer() {
    let dir = TempDir::new().unwrap();
    let m = Arc::new(single_shard(&dir));
    m.execute_one(S0, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();

    let read_errors = Arc::new(AtomicUsize::new(0));
    let write_errors = Arc::new(AtomicUsize::new(0));

    std::thread::scope(|s| {
        for _ in 0..4 {
            let m = Arc::clone(&m);
            let errs = Arc::clone(&read_errors);
            s.spawn(move || {
                for _ in 0..100 {
                    match m.query(S0, "SELECT count(*) FROM t") {
                        Ok(_) | Err(Error::ReaderPoolBusy) => {}
                        Err(_) => {
                            errs.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });
        }
        for who in 0..4 {
            let m = Arc::clone(&m);
            let errs = Arc::clone(&write_errors);
            s.spawn(move || {
                for i in 0..100 {
                    let id = 1000 + who * 1000 + i;
                    if !matches!(
                        m.execute_one(S0, &format!("INSERT INTO t VALUES ({id})")),
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
fn a_full_read_queue_sheds_load_instead_of_growing() {
    let dir = TempDir::new().unwrap();
    let cfg = ShardConfig {
        shard_count: 1,
        reader_threads: 1,
        open_readers_per_thread: 1,
        ..ShardConfig::floor()
    };
    let writers = Arc::new(
        WriterFleet::spawn(
            dir.path(),
            cfg.clone(),
            PragmaProfile::writer_shard(),
            CheckpointConfig::floor(),
        )
        .unwrap(),
    );
    writers
        .execute_one(S0, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    let stmts: Vec<String> = (1..=2_000)
        .map(|i| format!("INSERT INTO t VALUES ({i})"))
        .collect();
    for chunk in stmts.chunks(50) {
        writers.execute(S0, chunk.to_vec()).unwrap();
    }

    let pool = ReaderPoolConfig {
        threads: 1,
        queue_capacity: 1,
        query_timeout: Duration::from_secs(30),
    };
    let readers = Arc::new(
        ReaderFleet::spawn(
            dir.path(),
            &cfg,
            &pool,
            PragmaProfile::reader_shard(),
            Arc::downgrade(&writers),
        )
        .unwrap(),
    );

    let busy = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|s| {
        for _ in 0..8 {
            let readers = Arc::clone(&readers);
            let busy = Arc::clone(&busy);
            s.spawn(move || {
                for _ in 0..50 {
                    if let Err(Error::ReaderPoolBusy) =
                        readers.query(S0, "SELECT count(*) FROM t a, t b WHERE a.id < b.id")
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
}

#[test]
fn a_runaway_query_is_cancelled_at_the_deadline() {
    let dir = TempDir::new().unwrap();
    let cfg = ShardConfig {
        shard_count: 1,
        reader_threads: 1,
        ..ShardConfig::floor()
    };
    let writers = Arc::new(
        WriterFleet::spawn(
            dir.path(),
            cfg.clone(),
            PragmaProfile::writer_shard(),
            CheckpointConfig::floor(),
        )
        .unwrap(),
    );
    writers
        .execute_one(S0, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    let stmts: Vec<String> = (1..=500)
        .map(|i| format!("INSERT INTO t VALUES ({i})"))
        .collect();
    for chunk in stmts.chunks(50) {
        writers.execute(S0, chunk.to_vec()).unwrap();
    }

    let pool = ReaderPoolConfig {
        threads: 1,
        queue_capacity: 8,
        query_timeout: Duration::from_millis(150),
    };
    let readers = ReaderFleet::spawn(
        dir.path(),
        &cfg,
        &pool,
        PragmaProfile::reader_shard(),
        Arc::downgrade(&writers),
    )
    .unwrap();

    let start = Instant::now();
    let result = readers.query(
        S0,
        "SELECT count(*) FROM t a, t b, t c WHERE a.id < b.id AND b.id < c.id",
    );
    let elapsed = start.elapsed();

    match result {
        Err(Error::QueryTimeout { .. }) => {}
        other => panic!("expected QueryTimeout, got {other:?}"),
    }
    assert!(elapsed < Duration::from_secs(5), "took {elapsed:?}");

    // The connection must remain usable after an interrupt.
    let after = readers.query(S0, "SELECT count(*) FROM t").unwrap();
    assert_eq!(scalar(&after), 500);
}
