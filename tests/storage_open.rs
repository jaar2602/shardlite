//! Step 1 acceptance: a database opens with the intended PRAGMA profile, the settings
//! actually took effect, and the writer-before-reader ordering invariant holds.

use std::path::PathBuf;

use meshdb::config::{PragmaProfile, Role, Synchronous};
use meshdb::error::Error;
use meshdb::storage::{open_reader_existing, open_writer, pragma};
use tempfile::TempDir;

fn db_path(dir: &TempDir) -> PathBuf {
    dir.path().join("shard_0.db")
}

#[test]
fn writer_opens_in_wal_mode() {
    let dir = TempDir::new().unwrap();
    let path = db_path(&dir);
    let conn = open_writer(&path, &PragmaProfile::writer_floor()).unwrap();

    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode.to_lowercase(), "wal");
}

#[test]
fn writer_pragmas_actually_take_effect() {
    let dir = TempDir::new().unwrap();
    let path = db_path(&dir);
    let profile = PragmaProfile::writer_floor();
    let conn = open_writer(&path, &profile).unwrap();

    let read = |name: &str| -> i64 {
        conn.query_row(&format!("PRAGMA {name}"), [], |r| r.get(0))
            .unwrap()
    };

    assert_eq!(read("synchronous"), Synchronous::Full.as_i64());
    assert_eq!(read("cache_size"), profile.cache_size);
    assert_eq!(read("mmap_size"), 0);
    assert_eq!(read("busy_timeout"), profile.busy_timeout_ms);
    assert_eq!(read("foreign_keys"), 1);
    assert_eq!(read("temp_store"), 2);
    assert_eq!(read("trusted_schema"), 0);
    // Checkpoints are explicit, run by the writer thread between transactions.
    assert_eq!(read("wal_autocheckpoint"), 0);
    assert_eq!(read("auto_vacuum"), 0);
    assert_eq!(read("soft_heap_limit"), profile.soft_heap_limit);
}

#[test]
fn verify_passes_for_both_roles() {
    let dir = TempDir::new().unwrap();
    let path = db_path(&dir);

    let writer_profile = PragmaProfile::writer_floor();
    let writer = open_writer(&path, &writer_profile).unwrap();
    pragma::verify(&writer, &writer_profile).unwrap();

    let reader_profile = PragmaProfile::reader_floor();
    let reader = open_reader_existing(&path, &reader_profile).unwrap();
    pragma::verify(&reader, &reader_profile).unwrap();
}

#[test]
fn verify_detects_a_setting_that_drifted() {
    // The whole point of `verify` is catching PRAGMAs that silently fail to apply.
    // If this test passes vacuously, `verify` is not actually checking anything.
    let dir = TempDir::new().unwrap();
    let path = db_path(&dir);
    let profile = PragmaProfile::writer_floor();
    let conn = open_writer(&path, &profile).unwrap();

    conn.execute_batch("PRAGMA cache_size = -1234;").unwrap();

    match pragma::verify(&conn, &profile) {
        Err(Error::PragmaMismatch {
            role,
            pragma,
            expected,
            actual,
        }) => {
            assert_eq!(role, Role::Writer);
            assert_eq!(pragma, "cache_size");
            assert_eq!(expected, profile.cache_size.to_string());
            assert_eq!(actual, "-1234");
        }
        other => panic!("expected PragmaMismatch, got {other:?}"),
    }
}

#[test]
fn reader_opens_after_writer_and_sees_committed_data() {
    let dir = TempDir::new().unwrap();
    let path = db_path(&dir);

    let writer = open_writer(&path, &PragmaProfile::writer_floor()).unwrap();
    writer
        .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT; INSERT INTO t VALUES (1, 'a');")
        .unwrap();

    let reader = open_reader_existing(&path, &PragmaProfile::reader_floor()).unwrap();
    let v: String = reader
        .query_row("SELECT v FROM t WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, "a");
}

#[test]
fn reader_cannot_write() {
    let dir = TempDir::new().unwrap();
    let path = db_path(&dir);

    let writer = open_writer(&path, &PragmaProfile::writer_floor()).unwrap();
    writer
        .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT;")
        .unwrap();

    let reader = open_reader_existing(&path, &PragmaProfile::reader_floor()).unwrap();
    assert!(
        reader.execute_batch("INSERT INTO t VALUES (1);").is_err(),
        "read-only connection must reject writes"
    );
}

#[test]
fn reader_alone_cannot_open_a_fresh_database() {
    // Documents *why* something must create a shard before a reader touches it: a
    // read-only connection cannot create the database file at all. This is what
    // `WriterFleet::ensure_open` exists to guarantee.
    let dir = TempDir::new().unwrap();
    let path = db_path(&dir);

    let flags = meshdb::rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY;
    assert!(
        meshdb::rusqlite::Connection::open_with_flags(&path, flags).is_err(),
        "a read-only open of a nonexistent database must fail"
    );
}

#[test]
fn wal_sidecars_exist_after_writer_open() {
    // Readers depend on these existing; `materialize_wal` is what guarantees it.
    let dir = TempDir::new().unwrap();
    let path = db_path(&dir);
    let _writer = open_writer(&path, &PragmaProfile::writer_floor()).unwrap();

    let wal = path.with_extension("db-wal");
    let shm = path.with_extension("db-shm");
    assert!(wal.exists(), "-wal should exist at {}", wal.display());
    assert!(shm.exists(), "-shm should exist at {}", shm.display());
}

#[test]
fn concurrent_opens_of_the_same_database_do_not_fail_busy() {
    // Smoke test for concurrent opens.
    //
    // Honest limitation: this does NOT reproduce the SQLITE_BUSY-at-open failure seen in
    // the 1-CPU benchmark. It was written as a regression test for the `busy_timeout`
    // ordering fix and then checked by reintroducing the bug — it passed either way, so it
    // does not discriminate. It is kept because "many threads can open the same database"
    // is worth holding, not because it guards that fix.
    let dir = TempDir::new().unwrap();
    let path = db_path(&dir);

    // A failure is only acceptable if it waited. Under heavy machine load a genuine
    // busy-timeout expiry is possible, and asserting zero failures makes this test flaky
    // rather than wrong — it failed only when the whole suite ran in parallel. What must
    // never happen is an *immediate* failure, which is what an unset timeout looks like.
    let quick_failures = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let timeout = PragmaProfile::writer_floor().busy_timeout_ms as u128;

    std::thread::scope(|s| {
        for _ in 0..24 {
            let path = path.clone();
            let quick = std::sync::Arc::clone(&quick_failures);
            s.spawn(move || {
                let t0 = std::time::Instant::now();
                match open_writer(&path, &PragmaProfile::writer_floor()) {
                    Ok(conn) => {
                        // Hold it briefly so the opens genuinely overlap.
                        let _ = conn.execute_batch("SELECT 1");
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    Err(_) => {
                        // Half the configured timeout is generous; an unset timeout fails
                        // in microseconds.
                        if t0.elapsed().as_millis() < timeout / 2 {
                            quick.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            });
        }
    });

    assert_eq!(
        quick_failures.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "an open failed without waiting for the busy timeout, which is what an unset \
         timeout looks like"
    );
}

#[test]
fn a_contended_open_waits_on_the_busy_timeout_instead_of_failing() {
    // Verifies that a contended open waits for the lock rather than failing.
    //
    // Same honest caveat as above: this passes with and without the `busy_timeout`
    // ordering fix, because by the time `materialize_wal` takes the write lock the timeout
    // has been set in either ordering. The ordering fix is justified on its own merits —
    // no statement that can take a lock should run before the timeout is configured — but
    // it is NOT demonstrated by this test.
    let dir = TempDir::new().unwrap();
    let path = db_path(&dir);

    let holder = open_writer(&path, &PragmaProfile::writer_floor()).unwrap();
    holder.execute_batch("BEGIN EXCLUSIVE").unwrap();

    let opener = {
        let path = path.clone();
        std::thread::spawn(move || {
            let t0 = std::time::Instant::now();
            let r = open_writer(&path, &PragmaProfile::writer_floor());
            (r.is_ok(), t0.elapsed())
        })
    };

    std::thread::sleep(std::time::Duration::from_millis(250));
    holder.execute_batch("COMMIT").unwrap();

    let (ok, waited) = opener.join().unwrap();
    assert!(
        ok,
        "a contended open must succeed once the lock is released"
    );
    assert!(
        waited >= std::time::Duration::from_millis(200),
        "the open returned in {waited:?}, so it did not wait for the lock — \
         busy_timeout is not in effect when the lock is taken"
    );
}
