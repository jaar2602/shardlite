//! Step 7: capture wired into the real write path, drained to a sink.
//!
//! Step 0 proved a pass-through VFS *can* capture frames. This proves the writer fleet
//! actually does it — per shard, across LRU eviction, with retention bounded and a follower
//! reconstructible from what the sink received.

use std::sync::Arc;

use meshdb::config::{CheckpointConfig, PragmaProfile};
use meshdb::error::Error;
use meshdb::replication::StreamTxn;
use meshdb::replication::{FrameSink, MemorySink, NullSink};
use meshdb::shard::writer_fleet::WriterFleet;
use meshdb::shard::{ShardConfig, ShardId};
use meshdb::storage::Value;
use meshdb::storage::exec::{Executed, Outcome, Statement};
use meshdb::vfs;
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

    let stream: Vec<StreamTxn> = sink.take(S0);
    assert!(!stream.is_empty(), "sink should have received the changes");
    let txns: Vec<_> = stream.into_iter().map(|s| s.txn).collect();
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
fn overflow_fails_writes_while_the_backlog_cannot_be_shifted() {
    // Overflow is only reachable when the backlog genuinely cannot move — a sink that is
    // not consuming. With no sink at all the drain always succeeds, so retention never
    // persists and this state is unreachable; the earlier version of this test asserted
    // behaviour that the drain-first ordering removed.
    let dir = TempDir::new().unwrap();
    let sink = Arc::new(FlakySink::default());
    let mut c = cfg(1, true);
    c.max_retained_bytes = 32 * 1024;
    let w = fleet(&dir, c, Some(sink.clone()));

    // Schema first, while the sink is healthy. Taking it down before the DDL fails the DDL
    // too — correct behaviour, but it means the test never reaches what it is testing.
    w.execute_one(
        S0,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, pad BLOB) STRICT",
    )
    .unwrap();
    sink.down.store(true, std::sync::atomic::Ordering::SeqCst);

    let pad = vec![0u8; 4096];
    let mut saw_overflow = false;
    for i in 0..60 {
        let stmt = Statement::with_params(
            "INSERT INTO t VALUES (?1, ?2)",
            vec![Value::Integer(i), Value::Blob(pad.clone())],
        );
        if matches!(
            w.execute(S0, vec![stmt]),
            Err(Error::CaptureOverflow { .. })
        ) {
            saw_overflow = true;
            break;
        }
    }
    assert!(
        saw_overflow,
        "a sink that never consumes must eventually produce CaptureOverflow rather than \
         letting retention grow without limit"
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

/// A sink that can be taken down and brought back, to exercise overflow recovery.
#[derive(Default)]
struct FlakySink {
    down: std::sync::atomic::AtomicBool,
    accepted: std::sync::atomic::AtomicU64,
}

impl FrameSink for FlakySink {
    fn accept(
        &self,
        _shard: ShardId,
        _epoch: u64,
        txns: Vec<meshdb::replication::StreamTxn>,
    ) -> meshdb::Result<()> {
        if self.down.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::Unsupported("sink is down".into()));
        }
        self.accepted
            .fetch_add(txns.len() as u64, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn overflow_clears_once_the_backlog_is_consumed() {
    // Overflow is a backlog, not a loss: frames are never dropped to stay under the cap.
    // So once a sink comes back and the backlog drains, writes must resume on their own.
    // Requiring a restart to clear it would turn a transient sink outage into an outage of
    // the database.
    let dir = TempDir::new().unwrap();
    let sink = Arc::new(FlakySink::default());
    let mut c = cfg(1, true);
    c.max_retained_bytes = 32 * 1024;
    let w = fleet(&dir, c, Some(sink.clone()));

    w.execute_one(
        S0,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, pad BLOB) STRICT",
    )
    .unwrap();

    // Take the sink down and write until the backlog trips the cap.
    sink.down.store(true, std::sync::atomic::Ordering::SeqCst);
    let pad = vec![0u8; 4096];
    let mut failed = 0;
    for i in 0..40 {
        let stmt = Statement::with_params(
            "INSERT INTO t VALUES (?1, ?2)",
            vec![Value::Integer(i), Value::Blob(pad.clone())],
        );
        if w.execute(S0, vec![stmt]).is_err() {
            failed += 1;
        }
    }
    assert!(
        failed > 0,
        "a sink that is down must eventually refuse writes"
    );

    // Bring it back. The next write drains the backlog, clearing the overflow.
    sink.down.store(false, std::sync::atomic::Ordering::SeqCst);
    let recovered = w.execute_one(S0, "INSERT INTO t VALUES (9999, x'00')");
    assert!(
        recovered.is_ok(),
        "writes must resume once the backlog drains, got {recovered:?}"
    );
    assert!(
        sink.accepted.load(std::sync::atomic::Ordering::SeqCst) > 0,
        "the backlog should have reached the sink, not been dropped"
    );

    // And it keeps working.
    for i in 100..110 {
        w.execute_one(S0, format!("INSERT INTO t VALUES ({i}, x'00')"))
            .unwrap();
    }
}

#[test]
fn vacuum_reclaims_space_and_keeps_the_data() {
    // VACUUM cannot run inside a transaction, which is why the ordinary statement path
    // rejects it; this is the maintenance path that runs it on the writer thread outside
    // one.
    let dir = TempDir::new().unwrap();
    let w = fleet(&dir, cfg(1, false), None);

    w.execute_one(
        S0,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, pad BLOB) STRICT",
    )
    .unwrap();
    let pad = vec![0u8; 4096];
    for chunk in 0..20 {
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
    w.execute_one(S0, "DELETE FROM t WHERE id % 10 != 0")
        .unwrap();

    // Force a checkpoint before measuring. Until the WAL is folded in the main file holds
    // almost nothing — the first version of this test compared a 4 KiB main file against a
    // 471 KiB one and concluded vacuum had made things worse.
    let (_e, _l, _p) = w.begin_snapshot(S0).unwrap();
    w.end_snapshot(S0).unwrap();
    let before = std::fs::metadata(S0.path(dir.path())).unwrap().len();

    w.vacuum(S0).unwrap();
    let after = std::fs::metadata(S0.path(dir.path())).unwrap().len();

    println!("vacuum: {before} -> {after} bytes");
    assert!(
        after < before / 2,
        "vacuum should have reclaimed most of the file: {before} -> {after}"
    );

    match w.execute_one(S0, "SELECT count(*) FROM t").unwrap() {
        Outcome::Ok(Executed::Rows(r)) => assert_eq!(r.rows[0][0], Value::Integer(100)),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn vacuum_is_refused_while_a_snapshot_is_held() {
    // VACUUM rewrites every page, which would change the file a snapshot copy is reading.
    let dir = TempDir::new().unwrap();
    let w = fleet(&dir, cfg(1, false), None);
    w.execute_one(S0, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();

    let (_e, _l, _src) = w.begin_snapshot(S0).unwrap();
    assert!(
        w.vacuum(S0).is_err(),
        "vacuum must not run under a snapshot hold"
    );
    assert!(w.end_snapshot(S0).unwrap());
    w.vacuum(S0).unwrap();
}

/// Records every LSN it is handed, so loss and gaps are both visible.
#[derive(Default)]
struct RecordingSink {
    down: std::sync::atomic::AtomicBool,
    seen: std::sync::Mutex<Vec<u64>>,
}

impl FrameSink for RecordingSink {
    fn accept(
        &self,
        _shard: ShardId,
        _epoch: u64,
        txns: Vec<meshdb::replication::StreamTxn>,
    ) -> meshdb::Result<()> {
        if self.down.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::Unsupported("sink is down".into()));
        }
        self.seen.lock().unwrap().extend(txns.iter().map(|t| t.lsn));
        Ok(())
    }
}

#[test]
fn frames_refused_by_a_sink_are_not_lost() {
    // The regression that matters. `drain_committed` hands ownership to the caller, so a
    // sink that then refuses a batch would leave those frames delivered nowhere and
    // retained nowhere, with their stream positions already spent — silent truncation.
    //
    // Checked by content, not by "the sink got something": every committed transaction must
    // eventually arrive, and the LSNs must be dense. A gap is just as fatal as a loss,
    // because the follower refuses everything after it.
    let dir = TempDir::new().unwrap();
    let sink = Arc::new(RecordingSink::default());
    let w = fleet(&dir, cfg(1, true), Some(sink.clone()));

    w.execute_one(S0, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();

    // Everything written while the sink is down commits locally but cannot ship.
    sink.down.store(true, std::sync::atomic::Ordering::SeqCst);
    let mut refused = 0;
    for i in 0..25 {
        if w.execute_one(S0, format!("INSERT INTO t VALUES ({i})"))
            .is_err()
        {
            refused += 1;
        }
    }
    assert_eq!(refused, 25, "every write should have been refused");

    // Bring it back; the backlog must flush.
    sink.down.store(false, std::sync::atomic::Ordering::SeqCst);
    w.execute_one(S0, "INSERT INTO t VALUES (9999)").unwrap();

    let seen = sink.seen.lock().unwrap().clone();
    println!("sink saw {} LSNs: {:?}", seen.len(), seen);

    // Every row that is in the database must be in the stream.
    let rows = match w.execute_one(S0, "SELECT count(*) FROM t").unwrap() {
        Outcome::Ok(Executed::Rows(r)) => match r.rows[0][0] {
            Value::Integer(n) => n,
            ref v => panic!("expected an integer, got {v:?}"),
        },
        other => panic!("expected rows, got {other:?}"),
    };
    assert_eq!(rows, 26, "all writes committed locally");

    // 1 DDL + 25 inserts + 1 final insert = 27 transactions, positions 1..=27, no gaps.
    assert_eq!(
        seen.len(),
        27,
        "the sink received {} transactions but 27 were committed — frames were dropped",
        seen.len()
    );
    let expected: Vec<u64> = (1..=27).collect();
    assert_eq!(
        seen, expected,
        "stream positions must be dense; a gap makes the follower refuse everything after it"
    );
}
