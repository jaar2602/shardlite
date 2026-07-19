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
fn opening_while_another_connection_holds_the_write_lock_succeeds() {
    // Deterministic version of a race that used to appear only under load.
    //
    // The cause, measured on 3.53.2: `PRAGMA journal_mode = WAL` does **not** honour
    // `busy_timeout`. With a 5000 ms timeout set and another connection holding the write
    // lock, it gives up after 23 microseconds, while an ordinary INSERT on the same
    // connection waits the full 5.01 seconds. SQLite does not invoke the busy handler for a
    // journal-mode change, so `open_writer` retries it explicitly.
    let dir = TempDir::new().unwrap();
    let path = db_path(&dir);

    // A holds the write lock on a database that is NOT yet in WAL mode, which is exactly
    // the state a second opener has to convert.
    let holder = meshdb::rusqlite::Connection::open(&path).unwrap();
    holder.execute_batch("CREATE TABLE t(x);").unwrap();
    holder.execute_batch("BEGIN IMMEDIATE;").unwrap();

    let opener = {
        let path = path.clone();
        std::thread::spawn(move || open_writer(&path, &PragmaProfile::writer_floor()).is_ok())
    };

    // Hold it long enough that a non-retrying open would certainly have given up.
    std::thread::sleep(std::time::Duration::from_millis(300));
    holder.execute_batch("COMMIT;").unwrap();

    assert!(
        opener.join().unwrap(),
        "open_writer gave up instead of retrying the WAL conversion"
    );
}

#[test]
fn many_concurrent_opens_of_a_fresh_database_all_succeed() {
    // The shape that surfaced the bug: threads racing to convert the same fresh database.
    // Some are mid-conversion while others already hold the write lock.
    let dir = TempDir::new().unwrap();
    let path = db_path(&dir);

    let failures = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    std::thread::scope(|s| {
        for _ in 0..24 {
            let path = path.clone();
            let failures = std::sync::Arc::clone(&failures);
            s.spawn(
                move || match open_writer(&path, &PragmaProfile::writer_floor()) {
                    Ok(conn) => {
                        let _ = conn.execute_batch("BEGIN IMMEDIATE; COMMIT;");
                    }
                    Err(_) => {
                        failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                },
            );
        }
    });

    assert_eq!(
        failures.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "concurrent opens of a fresh database must all succeed"
    );
}

#[test]
fn a_contended_open_waits_on_the_busy_timeout_instead_of_failing() {
    // Covers the ordinary-write half: once the database is already in WAL mode,
    // `materialize_wal`'s BEGIN IMMEDIATE does honour `busy_timeout` and waits.
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

#[test]
fn wal_conversion_contention_is_counted_and_logged() {
    // Retrying the conversion makes contention invisible unless something records it. A
    // silent retry turns "your shards are being opened by too many writers" or "your
    // storage is slow" into no signal at all.
    use meshdb::storage::wal_conversion_stats;

    let before = wal_conversion_stats();

    let dir = TempDir::new().unwrap();
    let path = db_path(&dir);

    // Force the contention deterministically: hold the write lock on a non-WAL database.
    let holder = meshdb::rusqlite::Connection::open(&path).unwrap();
    holder.execute_batch("CREATE TABLE t(x);").unwrap();
    holder.execute_batch("BEGIN IMMEDIATE;").unwrap();

    let opener = {
        let path = path.clone();
        std::thread::spawn(move || open_writer(&path, &PragmaProfile::writer_floor()).is_ok())
    };
    std::thread::sleep(std::time::Duration::from_millis(200));
    holder.execute_batch("COMMIT;").unwrap();
    assert!(opener.join().unwrap(), "the open should have succeeded");

    let after = wal_conversion_stats();
    assert!(
        after.retries > before.retries,
        "retries were not counted: {before:?} -> {after:?}"
    );
    assert!(
        after.contended_opens > before.contended_opens,
        "the contended open was not counted: {before:?} -> {after:?}"
    );
    assert!(
        after.max_wait_ms >= 100,
        "the wait should have been recorded, got {}ms",
        after.max_wait_ms
    );
    // Contention that resolves is not a failure.
    assert_eq!(
        after.failed_opens, before.failed_opens,
        "a successful open must not be counted as failed"
    );
}
