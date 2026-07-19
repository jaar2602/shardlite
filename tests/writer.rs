//! Step 2 acceptance: the writer serializes correctly, isolates failures per request,
//! and actually batches under load.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use meshdb::config::PragmaProfile;
use meshdb::error::Error;
use meshdb::storage::exec::{Executed, Outcome};
use meshdb::storage::writer::Writer;
use tempfile::TempDir;

fn spawn(dir: &TempDir) -> Writer {
    let path = dir.path().join("shard_0.db");
    let (w, _token) = Writer::spawn(&path, &PragmaProfile::writer_floor()).unwrap();
    w
}

#[test]
fn writes_apply_in_order() {
    let dir = TempDir::new().unwrap();
    let w = spawn(&dir);
    let h = w.handle();

    h.execute_one("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT")
        .unwrap();
    for i in 1..=5 {
        h.execute_one(&format!("INSERT INTO t VALUES ({i}, 'v{i}')"))
            .unwrap();
    }

    let out = h.execute_one("SELECT count(*) FROM t").unwrap();
    match out {
        Outcome::Ok(Executed::Rows(r)) => {
            assert_eq!(r.rows.len(), 1);
            assert_eq!(r.rows[0][0], meshdb::storage::Value::Integer(5));
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn a_failed_request_does_not_roll_back_its_batch_peers() {
    // The core savepoint-isolation property: request 3 violates a constraint, and
    // requests 1, 2, 4, 5 must still commit.
    let dir = TempDir::new().unwrap();
    let w = spawn(&dir);
    let h = w.handle();

    h.execute_one("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    h.execute_one("INSERT INTO t VALUES (3)").unwrap();

    let outcomes = h
        .execute(vec![
            "INSERT INTO t VALUES (1)".into(),
            "INSERT INTO t VALUES (2)".into(),
            "INSERT INTO t VALUES (3)".into(), // duplicate primary key
            "INSERT INTO t VALUES (4)".into(),
            "INSERT INTO t VALUES (5)".into(),
        ])
        .unwrap();

    assert_eq!(outcomes.len(), 5);
    assert!(matches!(outcomes[0], Outcome::Ok(_)));
    assert!(matches!(outcomes[1], Outcome::Ok(_)));
    assert!(
        matches!(outcomes[2], Outcome::Rejected(_)),
        "duplicate key should be rejected, got {:?}",
        outcomes[2]
    );
    assert!(matches!(outcomes[3], Outcome::Ok(_)));
    assert!(matches!(outcomes[4], Outcome::Ok(_)));

    let out = h.execute_one("SELECT id FROM t ORDER BY id").unwrap();
    match out {
        Outcome::Ok(Executed::Rows(r)) => {
            let ids: Vec<_> = r
                .rows
                .iter()
                .map(|row| match &row[0] {
                    meshdb::storage::Value::Integer(i) => *i,
                    v => panic!("unexpected {v:?}"),
                })
                .collect();
            assert_eq!(ids, vec![1, 2, 3, 4, 5]);
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn a_rejected_statement_is_not_an_error() {
    // Bad SQL is a deterministic property of the data, so it comes back as a rejected
    // outcome rather than an Err that would abort the batch.
    let dir = TempDir::new().unwrap();
    let w = spawn(&dir);
    let h = w.handle();

    match h.execute_one("INSERT INTO does_not_exist VALUES (1)") {
        Ok(Outcome::Rejected(msg)) => assert!(
            msg.contains("does_not_exist"),
            "message should name the table: {msg}"
        ),
        other => panic!("expected Rejected, got {other:?}"),
    }

    match h.execute_one("THIS IS NOT SQL") {
        Ok(Outcome::Rejected(_)) => {}
        other => panic!("expected Rejected for a syntax error, got {other:?}"),
    }
}

#[test]
fn concurrent_writers_all_succeed_without_busy_errors() {
    // The point of funnelling through one thread: many concurrent callers, zero
    // SQLITE_BUSY, zero lost writes.
    const THREADS: usize = 16;
    const PER_THREAD: usize = 25;

    let dir = TempDir::new().unwrap();
    let w = spawn(&dir);
    let h = w.handle();
    h.execute_one("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, who INTEGER) STRICT")
        .unwrap();

    let errors = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|s| {
        for who in 0..THREADS {
            let h = w.handle();
            let errors = Arc::clone(&errors);
            s.spawn(move || {
                for _ in 0..PER_THREAD {
                    match h.execute_one(&format!("INSERT INTO t (who) VALUES ({who})")) {
                        Ok(Outcome::Ok(_)) => {}
                        _ => {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });
        }
    });

    assert_eq!(errors.load(Ordering::Relaxed), 0, "no write should fail");

    let out = h.execute_one("SELECT count(*) FROM t").unwrap();
    match out {
        Outcome::Ok(Executed::Rows(r)) => {
            assert_eq!(
                r.rows[0][0],
                meshdb::storage::Value::Integer((THREADS * PER_THREAD) as i64)
            );
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn group_commit_actually_batches_under_load() {
    // If this fails, the writer is committing one request per transaction and the entire
    // throughput argument for the design is wrong. `synchronous = FULL` means each commit
    // pays an fsync, which is exactly the window during which requests should pile up.
    const THREADS: usize = 16;
    const PER_THREAD: usize = 50;

    let dir = TempDir::new().unwrap();
    let w = spawn(&dir);
    w.handle()
        .execute_one("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, who INTEGER) STRICT")
        .unwrap();

    std::thread::scope(|s| {
        for who in 0..THREADS {
            let h = w.handle();
            s.spawn(move || {
                for _ in 0..PER_THREAD {
                    let _ = h.execute_one(&format!("INSERT INTO t (who) VALUES ({who})"));
                }
            });
        }
    });

    let stats = w.stats();
    println!(
        "batches={} requests={} max_batch={} mean_batch={:.2}",
        stats.batches,
        stats.requests,
        stats.max_batch,
        stats.mean_batch()
    );

    assert!(
        stats.max_batch > 1,
        "group commit never merged anything: max_batch={} over {} requests",
        stats.max_batch,
        stats.requests
    );
}

#[test]
fn writer_reports_when_it_is_gone() {
    let dir = TempDir::new().unwrap();
    let h = {
        let w = spawn(&dir);
        w.handle()
    }; // Writer dropped here: thread told to stop and joined.

    match h.execute_one("SELECT 1") {
        Err(Error::WriterGone) => {}
        other => panic!("expected WriterGone, got {other:?}"),
    }
}
