//! Step 0 — the VFS capture spike. **Pass/fail gate for the replication architecture.**
//!
//! The claim under test: a pass-through VFS can back a real on-disk SQLite database *and*
//! tee committed WAL frames accurately enough to reconstruct that database elsewhere.
//!
//! dqlite proves frames can be captured through a VFS with stock SQLite, but its VFS keeps
//! the whole database in process memory and never writes to disk. Doing both is what was
//! unverified — and at a few hundred GB, the in-memory fallback does not exist.
//!
//! Criteria:
//!   1. ordinary SQLite works unchanged through the VFS
//!   2. WAL header and frame headers parse
//!   3. commit frames are detected
//!   4. a byte-identical database is reconstructed on a follower
//!   5. it survives a checkpoint (WAL reset, salt rotation)
//!   6. it survives concurrent readers and a TRUNCATE checkpoint

use std::path::Path;

use meshdb::rusqlite::{Connection, OpenFlags};
use meshdb::vfs::{self, WalCapture};
use tempfile::TempDir;

fn open_captured(path: &Path) -> (Connection, std::sync::Arc<WalCapture>) {
    // Registration must precede the open: xOpen consults the registry when the -wal file
    // is first opened, and a capture registered later would miss frames.
    let capture = vfs::capture_for(path).unwrap();
    let conn = Connection::open_with_flags_and_vfs(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        vfs::VFS_NAME,
    )
    .unwrap();
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    conn.pragma_update(None, "synchronous", "FULL").unwrap();
    // Checkpoints are driven explicitly by these tests.
    conn.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    (conn, capture)
}

fn full_checkpoint(conn: &Connection) {
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, i64>(2)?,
        ))
    })
    .unwrap();
}

/// Criterion 1: the VFS is transparent.
#[test]
fn ordinary_sqlite_works_through_the_vfs() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("leader.db");
    let (conn, _cap) = open_captured(&path);

    conn.execute_batch(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT;
         INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');
         CREATE INDEX idx_v ON t(v);",
    )
    .unwrap();

    let n: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 3);

    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode.to_lowercase(), "wal");

    // The database is a real on-disk file, not an in-memory image.
    full_checkpoint(&conn);
    drop(conn);
    assert!(path.exists());
    assert!(std::fs::metadata(&path).unwrap().len() > 0);

    // And it is readable by a plain SQLite with no custom VFS at all.
    let plain = Connection::open(&path).unwrap();
    let n: i64 = plain
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 3);
}

/// Criteria 2 and 3: the WAL parses, and commits are identified.
#[test]
fn commit_frames_are_detected() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("leader.db");
    let (conn, cap) = open_captured(&path);

    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'a')", []).unwrap();
    conn.execute("INSERT INTO t VALUES (2, 'b')", []).unwrap();

    let stats = cap.stats();
    println!("{stats:?}");
    assert!(stats.page_size >= 512, "WAL header should have parsed");
    assert_eq!(
        stats.pending_frames, 0,
        "every frame should be inside a commit"
    );

    let txns = cap.drain_committed();
    assert!(
        txns.len() >= 3,
        "expected at least one transaction per statement, got {}",
        txns.len()
    );
    for t in &txns {
        assert!(t.db_size_pages > 0, "a commit must carry a database size");
        assert!(!t.frames.is_empty());
        assert_eq!(t.page_size, stats.page_size);
    }
}

/// Criterion 4: reconstruct a byte-identical database on a follower.
#[test]
fn a_follower_is_reconstructed_byte_identically() {
    let dir = TempDir::new().unwrap();
    let leader = dir.path().join("leader.db");
    let follower = dir.path().join("follower.db");

    let (conn, cap) = open_captured(&leader);
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT")
        .unwrap();

    // Establish a common baseline: checkpoint everything into the main file and copy it.
    full_checkpoint(&conn);
    std::fs::copy(&leader, &follower).unwrap();
    cap.drain_committed(); // frames up to the baseline are already in the copy

    // Diverge the leader.
    for i in 1..=200 {
        conn.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            meshdb::rusqlite::params![i, format!("value-{i}")],
        )
        .unwrap();
    }
    conn.execute("DELETE FROM t WHERE id % 3 = 0", []).unwrap();
    conn.execute_batch("CREATE INDEX idx_v ON t(v)").unwrap();

    // Apply the captured frames to the follower — no SQL is executed there.
    let txns = cap.drain_committed();
    assert!(!txns.is_empty());
    vfs::apply_to_db_file(&follower, &txns).unwrap();

    // Fold the leader's WAL into its main file so the two are comparable.
    full_checkpoint(&conn);
    drop(conn);

    let a = std::fs::read(&leader).unwrap();
    let b = std::fs::read(&follower).unwrap();
    assert_eq!(a.len(), b.len(), "database sizes differ");
    assert!(a == b, "reconstructed database is not byte-identical");

    // And it is a valid database, readable with no custom VFS.
    let plain = Connection::open(&follower).unwrap();
    let n: i64 = plain
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 200 - 66, "200 rows less those divisible by 3");
    let check: String = plain
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(check, "ok");
}

/// Criterion 5: survive checkpoints, which reset the WAL and rotate the salt.
#[test]
fn capture_survives_checkpoints_and_wal_resets() {
    let dir = TempDir::new().unwrap();
    let leader = dir.path().join("leader.db");
    let follower = dir.path().join("follower.db");

    let (conn, cap) = open_captured(&leader);
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT")
        .unwrap();
    full_checkpoint(&conn);
    std::fs::copy(&leader, &follower).unwrap();
    cap.drain_committed();

    // Several rounds, each ending in a checkpoint that resets the WAL.
    for round in 0..5 {
        for i in 0..40 {
            let id = round * 1000 + i;
            conn.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                meshdb::rusqlite::params![id, format!("r{round}-{i}")],
            )
            .unwrap();
        }
        // Apply before checkpointing, so the follower tracks the leader continuously.
        let txns = cap.drain_committed();
        assert!(!txns.is_empty(), "round {round} captured nothing");
        vfs::apply_to_db_file(&follower, &txns).unwrap();

        full_checkpoint(&conn);
    }

    let stats = cap.stats();
    println!("{stats:?}");
    assert!(
        stats.generation > 1,
        "checkpoints should have started new WAL generations: {stats:?}"
    );

    let remaining = cap.drain_committed();
    vfs::apply_to_db_file(&follower, &remaining).unwrap();
    full_checkpoint(&conn);
    drop(conn);

    let a = std::fs::read(&leader).unwrap();
    let b = std::fs::read(&follower).unwrap();
    assert!(a == b, "follower diverged across WAL resets");

    let plain = Connection::open(&follower).unwrap();
    let n: i64 = plain
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 200);
}

/// Negative control: the byte-comparison above must be capable of failing.
///
/// Without this, a bug that captured nothing at all would leave leader and follower
/// trivially equal and every criterion above would pass vacuously.
#[test]
fn dropping_one_transaction_is_detected_as_divergence() {
    let dir = TempDir::new().unwrap();
    let leader = dir.path().join("leader.db");
    let follower = dir.path().join("follower.db");

    let (conn, cap) = open_captured(&leader);
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT")
        .unwrap();
    full_checkpoint(&conn);
    std::fs::copy(&leader, &follower).unwrap();
    cap.drain_committed();

    for i in 1..=100 {
        conn.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            meshdb::rusqlite::params![i, format!("v{i}")],
        )
        .unwrap();
    }

    let mut txns = cap.drain_committed();
    assert!(txns.len() > 1, "need several transactions to drop one");
    let dropped = txns.pop().unwrap();
    println!(
        "dropping 1 of {} transactions ({} frames)",
        txns.len() + 1,
        dropped.frames.len()
    );

    vfs::apply_to_db_file(&follower, &txns).unwrap();
    full_checkpoint(&conn);
    drop(conn);

    let a = std::fs::read(&leader).unwrap();
    let b = std::fs::read(&follower).unwrap();
    assert!(
        a != b,
        "a missing transaction must produce a detectable difference — \
         if this passes, the comparison proves nothing"
    );
}

/// A heavier workload: page reuse, a freelist, index churn, and a multi-MB file.
///
/// The light tests could pass on append-only behaviour alone. This one forces pages to be
/// freed and reallocated, which is where a naive capture goes wrong.
#[test]
fn reconstruction_holds_under_page_reuse_and_churn() {
    let dir = TempDir::new().unwrap();
    let leader = dir.path().join("leader.db");
    let follower = dir.path().join("follower.db");

    let (conn, cap) = open_captured(&leader);
    conn.execute_batch(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, pad TEXT NOT NULL) STRICT;
         CREATE INDEX idx_pad ON t(pad);",
    )
    .unwrap();
    full_checkpoint(&conn);
    std::fs::copy(&leader, &follower).unwrap();
    cap.drain_committed();

    let pad = "x".repeat(300);
    conn.execute_batch("BEGIN").unwrap();
    for i in 1..=5_000 {
        conn.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            meshdb::rusqlite::params![i, format!("{pad}-{i}")],
        )
        .unwrap();
    }
    conn.execute_batch("COMMIT").unwrap();

    // Free a large number of pages, then reallocate them.
    conn.execute("DELETE FROM t WHERE id % 2 = 0", []).unwrap();
    conn.execute_batch("BEGIN").unwrap();
    for i in 10_000..12_000 {
        conn.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            meshdb::rusqlite::params![i, format!("{pad}-{i}")],
        )
        .unwrap();
    }
    conn.execute_batch("COMMIT").unwrap();
    conn.execute_batch("DROP INDEX idx_pad; CREATE INDEX idx_pad2 ON t(pad);")
        .unwrap();

    let txns = cap.drain_committed();
    let frames: usize = txns.iter().map(|t| t.frames.len()).sum();
    println!("{} transactions, {frames} frames captured", txns.len());
    let sizes: Vec<u32> = txns.iter().map(|t| t.db_size_pages).collect();
    println!("db_size_pages per txn: {sizes:?}");
    let captured_pages: std::collections::HashSet<u32> = txns
        .iter()
        .flat_map(|t| t.frames.iter().map(|f| f.page_no))
        .collect();
    println!("distinct pages captured: {}", captured_pages.len());
    vfs::apply_to_db_file(&follower, &txns).unwrap();

    full_checkpoint(&conn);
    drop(conn);

    let a = std::fs::read(&leader).unwrap();
    let b = std::fs::read(&follower).unwrap();
    println!("leader {} bytes, follower {} bytes", a.len(), b.len());
    assert!(
        a.len() > 2 * 1024 * 1024,
        "expected a multi-MB database, got {} bytes — workload too small to be meaningful",
        a.len()
    );

    if a != b {
        let ps = 4096usize;
        let mut differing = Vec::new();
        for p in 0..(a.len() / ps) {
            if a[p * ps..(p + 1) * ps] != b[p * ps..(p + 1) * ps] {
                differing.push(p + 1); // page numbers are 1-based
            }
        }
        println!(
            "{} of {} pages differ; first 20: {:?}",
            differing.len(),
            a.len() / ps,
            &differing[..differing.len().min(20)]
        );
        let never_captured: Vec<u32> = differing
            .iter()
            .map(|p| *p as u32)
            .filter(|p| !captured_pages.contains(p))
            .collect();
        println!(
            "of the differing pages, {} were NEVER captured: {:?}",
            never_captured.len(),
            &never_captured[..never_captured.len().min(20)]
        );
        // Is the follower's copy zeroed there (truncation destroyed it) or just stale?
        for p in differing.iter().take(3) {
            let off = (p - 1) * ps;
            let fa = &a[off..off + 32];
            let fb = &b[off..off + 32];
            let b_all_zero = b[off..off + ps].iter().all(|x| *x == 0);
            println!("page {p}: leader {fa:02x?}");
            println!("page {p}: follr  {fb:02x?} (all_zero={b_all_zero})");
        }
        panic!("follower diverged under page reuse");
    }

    let plain = Connection::open(&follower).unwrap();
    let check: String = plain
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(check, "ok");
    let n: i64 = plain
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 2_500 + 2_000);
}

/// Criterion 6: survive concurrent readers and a TRUNCATE checkpoint.
#[test]
fn capture_survives_concurrent_readers_and_truncate() {
    let dir = TempDir::new().unwrap();
    let leader = dir.path().join("leader.db");
    let follower = dir.path().join("follower.db");

    let (conn, cap) = open_captured(&leader);
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT")
        .unwrap();
    full_checkpoint(&conn);
    std::fs::copy(&leader, &follower).unwrap();
    cap.drain_committed();

    let reader_path = leader.clone();
    let reader = std::thread::spawn(move || {
        let r = Connection::open_with_flags(
            &reader_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .unwrap();
        let mut seen = 0i64;
        for _ in 0..300 {
            let n: i64 = r
                .query_row("SELECT count(*) FROM t", [], |x| x.get(0))
                .unwrap();
            seen = seen.max(n);
            std::thread::sleep(std::time::Duration::from_micros(100));
        }
        seen
    });

    for i in 1..=300 {
        conn.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            meshdb::rusqlite::params![i, format!("v{i}")],
        )
        .unwrap();
        if i % 100 == 0 {
            // TRUNCATE while readers are active: may be refused, which is fine.
            let _ = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                r.get::<_, i64>(0)
            });
        }
    }

    let seen = reader.join().unwrap();
    println!("concurrent reader observed up to {seen} rows");

    let txns = cap.drain_committed();
    vfs::apply_to_db_file(&follower, &txns).unwrap();

    full_checkpoint(&conn);
    drop(conn);

    let a = std::fs::read(&leader).unwrap();
    let b = std::fs::read(&follower).unwrap();
    assert!(
        a == b,
        "follower diverged with concurrent readers and TRUNCATE checkpoints"
    );

    let plain = Connection::open(&follower).unwrap();
    let n: i64 = plain
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 300);
    let check: String = plain
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(check, "ok");
}
