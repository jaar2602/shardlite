//! Step 7: capture wired into the real write path, drained to a sink.
//!
//! Step 0 proved a pass-through VFS *can* capture frames. This proves the writer fleet
//! actually does it — per shard, across LRU eviction, with retention bounded and a follower
//! reconstructible from what the sink received.

use std::sync::Arc;

use meshdb::config::{CheckpointConfig, PragmaProfile};
use meshdb::error::Error;
use meshdb::replication::{FrameSink, MemorySink, NullSink};
use meshdb::shard::writer_fleet::WriterFleet;
use meshdb::shard::{ShardConfig, ShardId};
use meshdb::storage::Value;
use meshdb::storage::exec::{Executed, Outcome, Statement};
use meshdb::vfs::{self, CommittedTxn};
use tempfile::TempDir;

const S0: ShardId = ShardId(0);

fn cfg(shards: u32, capture: bool) -> ShardConfig {
    ShardConfig {
        shard_count: shards,
        writer_threads: 1,
        open_writers_per_thread: 2,
        reader_threads: 1,
        open_readers_per_thread: 2,
        capture,
        ..ShardConfig::floor()
    }
}

fn fleet(dir: &TempDir, cfg: ShardConfig, sink: Option<Arc<dyn FrameSink>>) -> WriterFleet {
    WriterFleet::spawn_with_sink(
        dir.path(),
        cfg,
        PragmaProfile::writer_shard(),
        CheckpointConfig::floor(),
        sink,
    )
    .unwrap()
}

#[test]
fn capture_is_off_by_default_and_costs_nothing() {
    // A single-node deployment has no replication target, so it should not be paying for
    // capture at all — neither the throughput nor, more importantly, the retained memory.
    assert!(
        !ShardConfig::floor().capture,
        "capture must default to off while there is nothing to replicate to"
    );

    let dir = TempDir::new().unwrap();
    let sink = Arc::new(NullSink::new());
    let w = fleet(&dir, cfg(1, false), Some(sink.clone()));

    w.execute_one(S0, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    for i in 1..=200 {
        w.execute_one(S0, format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }

    let (txns, frames, bytes) = sink.counts();
    assert_eq!(
        (txns, frames, bytes),
        (0, 0, 0),
        "with capture off, nothing should reach the sink"
    );
}

#[test]
fn committed_frames_reach_the_sink() {
    let dir = TempDir::new().unwrap();
    let sink = Arc::new(NullSink::new());
    let w = fleet(&dir, cfg(1, true), Some(sink.clone()));

    w.execute_one(S0, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT")
        .unwrap();
    for i in 1..=200 {
        w.execute_one(S0, format!("INSERT INTO t VALUES ({i}, 'v{i}')"))
            .unwrap();
    }

    let (txns, frames, bytes) = sink.counts();
    println!("sink received {txns} txns, {frames} frames, {bytes} bytes");
    assert!(txns >= 200, "expected a transaction per write, got {txns}");
    assert!(
        frames >= txns,
        "every transaction carries at least one frame"
    );
    assert!(bytes > 0);
}

#[test]
fn a_follower_is_reconstructed_from_what_the_sink_received() {
    // The end-to-end claim: a replica built only from sink output matches the primary
    // byte for byte, having executed no SQL of its own.
    let dir = TempDir::new().unwrap();
    let sink = Arc::new(MemorySink::new());
    let w = fleet(&dir, cfg(1, true), Some(sink.clone()));

    w.execute_one(
        S0,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT",
    )
    .unwrap();

    // Baseline: fold everything into the main file and copy it, then discard those frames.
    w.execute_one(S0, "PRAGMA wal_checkpoint(TRUNCATE)").ok();
    let primary = S0.path(dir.path());
    let follower = dir.path().join("follower.db");
    std::fs::copy(&primary, &follower).unwrap();
    let _ = sink.take(S0);

    for i in 1..=300 {
        w.execute_one(
            S0,
            Statement::with_params(
                "INSERT INTO t VALUES (?1, ?2)",
                vec![Value::Integer(i), Value::Text(format!("value-{i}"))],
            ),
        )
        .unwrap();
    }
    w.execute_one(S0, "DELETE FROM t WHERE id % 3 = 0").unwrap();
    w.execute_one(S0, "CREATE INDEX idx_v ON t(v)").unwrap();

    let txns: Vec<CommittedTxn> = sink.take(S0);
    assert!(!txns.is_empty(), "sink should have received the changes");
    vfs::apply_to_db_file(&follower, &txns).unwrap();

    // Fold the primary's WAL in so the two files are comparable.
    w.execute_one(S0, "PRAGMA wal_checkpoint(TRUNCATE)").ok();
    drop(w);

    let a = std::fs::read(&primary).unwrap();
    let b = std::fs::read(&follower).unwrap();
    assert_eq!(a.len(), b.len(), "sizes differ");
    assert!(
        a == b,
        "follower rebuilt from the sink is not byte-identical"
    );

    let plain = meshdb::rusqlite::Connection::open(&follower).unwrap();
    let n: i64 = plain
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 300 - 100);
    let check: String = plain
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(check, "ok");
}

#[test]
fn each_shard_is_captured_separately() {
    let dir = TempDir::new().unwrap();
    let sink = Arc::new(MemorySink::new());
    let w = fleet(&dir, cfg(8, true), Some(sink.clone()));

    for s in 0..8u32 {
        w.execute_one(ShardId(s), "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
            .unwrap();
        for i in 0..10 {
            w.execute_one(ShardId(s), format!("INSERT INTO t VALUES ({i})"))
                .unwrap();
        }
    }

    let shards = sink.shards();
    assert_eq!(shards.len(), 8, "every shard should have its own stream");
    for s in 0..8u32 {
        assert!(
            sink.txn_count(ShardId(s)) >= 10,
            "shard {s} lost transactions"
        );
    }
}

#[test]
fn capture_survives_lru_eviction_and_reopen() {
    // Only 2 writer connections for 8 shards, so every shard is evicted and reopened
    // repeatedly. Reopening resets the WAL and bumps the capture generation; committed
    // frames must still reach the sink across that boundary.
    let dir = TempDir::new().unwrap();
    let sink = Arc::new(MemorySink::new());
    let w = fleet(&dir, cfg(8, true), Some(sink.clone()));

    for s in 0..8u32 {
        w.execute_one(ShardId(s), "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
            .unwrap();
    }
    // Round-robin, guaranteeing each shard is cold every time it is touched.
    for round in 0..5 {
        for s in 0..8u32 {
            w.execute_one(ShardId(s), format!("INSERT INTO t VALUES ({round})"))
                .unwrap();
        }
    }

    let stats = w.stats();
    assert!(
        stats.shard_evictions > 10,
        "expected heavy eviction, got {}",
        stats.shard_evictions
    );
    for s in 0..8u32 {
        assert!(
            sink.txn_count(ShardId(s)) >= 6,
            "shard {s} lost frames across eviction: {} txns",
            sink.txn_count(ShardId(s))
        );
    }
}

#[test]
fn retention_stays_bounded_because_the_writer_drains_every_batch() {
    // The reason capture can be on at all inside a 150 MB budget: retained bytes are
    // bounded by one batch, not by the write rate.
    let dir = TempDir::new().unwrap();
    let sink = Arc::new(NullSink::new());
    let w = fleet(&dir, cfg(1, true), Some(sink.clone()));

    w.execute_one(
        S0,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, pad BLOB) STRICT",
    )
    .unwrap();

    let pad = vec![0u8; 4096];
    for chunk in 0..40 {
        let stmts: Vec<Statement> = (0..50)
            .map(|i| {
                Statement::with_params(
                    "INSERT INTO t VALUES (?1, ?2)",
                    vec![Value::Integer(chunk * 50 + i), Value::Blob(pad.clone())],
                )
            })
            .collect();
        w.execute(S0, stmts).unwrap();
    }

    let capture = vfs::capture_for(&S0.path(dir.path())).unwrap();
    let retained = capture.retained_bytes();
    let (_, _, bytes_shipped) = sink.counts();
    println!("shipped {bytes_shipped} bytes, retained {retained} bytes");

    assert!(
        bytes_shipped > 4 * 1024 * 1024,
        "workload was too small to matter"
    );
    assert!(
        retained < 1024 * 1024,
        "retention should be bounded by one batch, but {retained} bytes are held"
    );
    assert!(!capture.overflowed());
}

#[test]
fn overflow_fails_writes_rather_than_dropping_frames() {
    // A sink that cannot keep up must not be papered over. Frames are never dropped, so the
    // only honest response is to stop accepting writes — committing locally what no replica
    // will receive is the divergence physical replication exists to prevent.
    //
    // Simulated with a tiny cap and no sink draining, which is the same condition.
    let dir = TempDir::new().unwrap();
    let mut c = cfg(1, true);
    c.max_retained_bytes = 64 * 1024; // far below one batch of 4 KiB blobs
    let w = fleet(&dir, c, None);

    w.execute_one(
        S0,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, pad BLOB) STRICT",
    )
    .unwrap();

    // Force a single transaction large enough to blow the cap before it commits.
    let pad = vec![0u8; 4096];
    let stmts: Vec<Statement> = (0..200)
        .map(|i| {
            Statement::with_params(
                "INSERT INTO t VALUES (?1, ?2)",
                vec![Value::Integer(i), Value::Blob(pad.clone())],
            )
        })
        .collect();

    // The batch itself may commit; what matters is that the failure surfaces and does not
    // silently continue with an incomplete stream.
    let first = w.execute(S0, stmts);
    let second = w.execute_one(S0, "INSERT INTO t VALUES (99999, x'00')");

    let saw_overflow = matches!(first, Err(Error::CaptureOverflow { .. }))
        || matches!(second, Err(Error::CaptureOverflow { .. }));
    assert!(
        saw_overflow,
        "expected CaptureOverflow; got first={first:?} second={second:?}"
    );
}

#[test]
fn captured_and_uncaptured_databases_are_identical_on_disk() {
    // Capture is a pass-through. The file it produces must be indistinguishable from one
    // written without it, or "ordinary SQLite database" is no longer true.
    let mk = |capture: bool| -> Vec<u8> {
        let dir = TempDir::new().unwrap();
        let w = fleet(&dir, cfg(1, capture), None);
        w.execute_one(S0, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT")
            .unwrap();
        let stmts: Vec<Statement> = (1..=100)
            .map(|i| {
                Statement::with_params(
                    "INSERT INTO t VALUES (?1, ?2)",
                    vec![Value::Integer(i), Value::Text(format!("v{i}"))],
                )
            })
            .collect();
        w.execute(S0, stmts).unwrap();
        w.execute_one(S0, "PRAGMA wal_checkpoint(TRUNCATE)").ok();
        let path = S0.path(dir.path());
        drop(w);
        std::fs::read(&path).unwrap()
    };

    let plain = mk(false);
    let captured = mk(true);
    assert_eq!(plain.len(), captured.len(), "file sizes differ");
    assert!(
        plain == captured,
        "capture changed the on-disk database; it must be a pass-through"
    );
}

#[test]
fn reads_still_work_against_a_captured_database() {
    let dir = TempDir::new().unwrap();
    let sink = Arc::new(NullSink::new());
    let w = fleet(&dir, cfg(1, true), Some(sink));

    w.execute_one(S0, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    for i in 1..=50 {
        w.execute_one(S0, format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }

    match w.execute_one(S0, "SELECT count(*) FROM t").unwrap() {
        Outcome::Ok(Executed::Rows(r)) => assert_eq!(r.rows[0][0], Value::Integer(50)),
        other => panic!("expected rows, got {other:?}"),
    }
}
