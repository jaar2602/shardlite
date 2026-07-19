//! Step 1 acceptance: a database opens with the intended PRAGMA profile, the settings
//! actually took effect, and the writer-before-reader ordering invariant holds.

use std::path::PathBuf;

use meshdb::config::{PragmaProfile, Role, Synchronous};
use meshdb::error::Error;
use meshdb::storage::{open_reader, open_writer, pragma};
use tempfile::TempDir;

fn db_path(dir: &TempDir) -> PathBuf {
    dir.path().join("shard_0.db")
}

#[test]
fn writer_opens_in_wal_mode() {
    let dir = TempDir::new().unwrap();
    let path = db_path(&dir);
    let (conn, _token) = open_writer(&path, &PragmaProfile::writer_floor()).unwrap();

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
    let (conn, _token) = open_writer(&path, &profile).unwrap();

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
    let (writer, token) = open_writer(&path, &writer_profile).unwrap();
    pragma::verify(&writer, &writer_profile).unwrap();

    let reader_profile = PragmaProfile::reader_floor();
    let reader = open_reader(&path, &reader_profile, &token).unwrap();
    pragma::verify(&reader, &reader_profile).unwrap();
}

#[test]
fn verify_detects_a_setting_that_drifted() {
    // The whole point of `verify` is catching PRAGMAs that silently fail to apply.
    // If this test passes vacuously, `verify` is not actually checking anything.
    let dir = TempDir::new().unwrap();
    let path = db_path(&dir);
    let profile = PragmaProfile::writer_floor();
    let (conn, _token) = open_writer(&path, &profile).unwrap();

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

    let (writer, token) = open_writer(&path, &PragmaProfile::writer_floor()).unwrap();
    writer
        .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT; INSERT INTO t VALUES (1, 'a');")
        .unwrap();

    let reader = open_reader(&path, &PragmaProfile::reader_floor(), &token).unwrap();
    let v: String = reader
        .query_row("SELECT v FROM t WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, "a");
}

#[test]
fn reader_cannot_write() {
    let dir = TempDir::new().unwrap();
    let path = db_path(&dir);

    let (writer, token) = open_writer(&path, &PragmaProfile::writer_floor()).unwrap();
    writer
        .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT;")
        .unwrap();

    let reader = open_reader(&path, &PragmaProfile::reader_floor(), &token).unwrap();
    assert!(
        reader.execute_batch("INSERT INTO t VALUES (1);").is_err(),
        "read-only connection must reject writes"
    );
}

#[test]
fn reader_alone_cannot_open_a_fresh_database() {
    // Documents *why* `open_reader` demands a `WriterOpened` token: a read-only
    // connection cannot create the database or its WAL sidecars.
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
    let (_writer, _token) = open_writer(&path, &PragmaProfile::writer_floor()).unwrap();

    let wal = path.with_extension("db-wal");
    let shm = path.with_extension("db-shm");
    assert!(wal.exists(), "-wal should exist at {}", wal.display());
    assert!(shm.exists(), "-shm should exist at {}", shm.display());
}
