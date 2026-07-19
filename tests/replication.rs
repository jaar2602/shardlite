//! Step 8: a follower converges with its primary, and refuses anything it cannot place.
//!
//! In-process: primary and follower are two directories in one test. That deliberately
//! leaves the network out — the properties under test are ordering, gap detection, and
//! bootstrap, none of which a transport changes.

use std::sync::Arc;

use meshdb::error::Error;
use meshdb::replication::{Follower, FollowerSink, MemorySink, Need, StreamTxn};
use meshdb::shard::{ShardConfig, ShardId, ShardManager};
use meshdb::storage::Value;
use meshdb::storage::exec::Statement;
use tempfile::TempDir;

const S0: ShardId = ShardId(0);

fn cfg(shards: u32) -> ShardConfig {
    ShardConfig {
        shard_count: shards,
        writer_threads: 1,
        open_writers_per_thread: 2,
        reader_threads: 1,
        open_readers_per_thread: 2,
        capture: true,
        ..ShardConfig::floor()
    }
}

fn insert(i: i64) -> Statement {
    Statement::with_params(
        "INSERT INTO t VALUES (?1, ?2)",
        vec![Value::Integer(i), Value::Text(format!("value-{i}"))],
    )
}

/// Byte-compare a primary shard against its follower copy.
///
/// Uses `snapshot` rather than sending `PRAGMA wal_checkpoint` through `execute_one`: that
/// path wraps every statement in a transaction, and a checkpoint inside one does nothing,
/// leaving the primary's data stranded in the WAL and the comparison meaningless.
fn assert_converged(primary: &ShardManager, follower: &Follower, shard: ShardId) {
    let tmp = TempDir::new().unwrap();
    let snap = tmp.path().join("cmp.db");
    primary.snapshot(shard, &snap).unwrap();
    let a = std::fs::read(&snap).unwrap();
    let b = std::fs::read(shard.path(follower.dir())).unwrap();
    assert_eq!(a.len(), b.len(), "{shard}: sizes differ");
    assert!(a == b, "{shard}: follower diverged from primary");

    let conn = meshdb::rusqlite::Connection::open(shard.path(follower.dir())).unwrap();
    let check: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(check, "ok", "{shard}: follower failed integrity_check");
}

#[test]
fn a_follower_converges_with_its_primary() {
    let pdir = TempDir::new().unwrap();
    let fdir = TempDir::new().unwrap();

    let follower = Arc::new(Follower::open(fdir.path()).unwrap());
    let sink = Arc::new(FollowerSink::new(Arc::clone(&follower)));
    let primary = ShardManager::open_with_sink(pdir.path(), cfg(1), Some(sink)).unwrap();

    primary
        .execute_one(
            S0,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT",
        )
        .unwrap();
    for i in 1..=300 {
        primary.execute_one(S0, insert(i)).unwrap();
    }
    primary
        .execute_one(S0, "DELETE FROM t WHERE id % 4 = 0")
        .unwrap();
    primary
        .execute_one(S0, "CREATE INDEX idx_v ON t(v)")
        .unwrap();

    assert_converged(&primary, &follower, S0);

    let conn = meshdb::rusqlite::Connection::open(S0.path(follower.dir())).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 300 - 75, "follower has the wrong rows");

    let pos = follower.position(S0);
    assert!(pos.applied_lsn >= 300, "position did not advance: {pos:?}");
    assert_eq!(Some(pos.epoch), primary.epoch());
}

#[test]
fn every_shard_converges_independently() {
    let pdir = TempDir::new().unwrap();
    let fdir = TempDir::new().unwrap();
    let follower = Arc::new(Follower::open(fdir.path()).unwrap());
    let sink = Arc::new(FollowerSink::new(Arc::clone(&follower)));
    let primary = ShardManager::open_with_sink(pdir.path(), cfg(8), Some(sink)).unwrap();

    primary
        .execute_all_shards("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    for s in 0..8u32 {
        for i in 0..25 {
            primary
                .execute_one(ShardId(s), format!("INSERT INTO t VALUES ({i})"))
                .unwrap();
        }
    }

    for s in 0..8u32 {
        assert_converged(&primary, &follower, ShardId(s));
        // Each shard is its own stream, so each has its own dense position.
        assert!(follower.position(ShardId(s)).applied_lsn >= 25);
    }
}

#[test]
fn a_gap_is_refused_rather_than_applied_across() {
    // The property that makes loss detectable. Applying page N+2 without N leaves pages
    // that are individually valid — integrity_check may well pass — while the database as a
    // whole is wrong. Refusing is the only safe response.
    let pdir = TempDir::new().unwrap();
    let fdir = TempDir::new().unwrap();
    let sink = Arc::new(MemorySink::new());
    let primary = ShardManager::open_with_sink(pdir.path(), cfg(1), Some(sink.clone())).unwrap();

    primary
        .execute_one(S0, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    for i in 1..=20 {
        primary
            .execute_one(S0, format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }

    let epoch = primary.epoch().unwrap();
    let all: Vec<StreamTxn> = sink.take(S0);
    assert!(all.len() > 5);

    let follower = Follower::open(fdir.path()).unwrap();

    // Applying with the first transaction removed must fail, not silently succeed.
    let with_hole: Vec<StreamTxn> = all.iter().skip(1).cloned().collect();
    match follower.apply(S0, epoch, &with_hole) {
        Err(Error::ReplicationGap { expected, got, .. }) => {
            assert_eq!(expected, 1);
            assert_eq!(got, all[1].lsn);
        }
        other => panic!("expected ReplicationGap, got {other:?}"),
    }

    // A hole in the middle is caught too.
    let mut middle_hole = all.clone();
    middle_hole.remove(3);
    match follower.apply(S0, epoch, &middle_hole) {
        Err(Error::ReplicationGap { .. }) => {}
        other => panic!("expected ReplicationGap for a mid-stream hole, got {other:?}"),
    }

    // The contiguous stream applies cleanly.
    follower.apply(S0, epoch, &all).unwrap();
    assert_eq!(follower.position(S0).applied_lsn, all.last().unwrap().lsn);
}

#[test]
fn a_new_epoch_demands_a_bootstrap() {
    let fdir = TempDir::new().unwrap();
    let follower = Follower::open(fdir.path()).unwrap();

    // A follower that has never seen this shard asks for a bootstrap, even though `apply`
    // would accept a stream starting at LSN 1 — see the note on `need`.
    assert!(matches!(follower.need(S0, 7), Need::Bootstrap { .. }));

    // Once bootstrapped into epoch 7 it asks for the next LSN.
    let dummy = fdir.path().join("seed.db");
    std::fs::write(&dummy, b"").unwrap();
    follower.install_snapshot(S0, 7, 42, &dummy).unwrap();
    assert_eq!(follower.need(S0, 7), Need::Next(43));

    // A primary that restarted uncleanly moves to a new epoch, whose LSNs cannot be
    // compared to the old ones — so the follower must be re-seeded.
    assert!(matches!(follower.need(S0, 8), Need::Bootstrap { .. }));
}

#[test]
fn a_follower_bootstraps_from_a_snapshot_and_then_streams() {
    // The realistic path: a follower joins a primary that already has data, takes a
    // snapshot, and catches up on frames from that point.
    let pdir = TempDir::new().unwrap();
    let fdir = TempDir::new().unwrap();
    let snapdir = TempDir::new().unwrap();

    let sink = Arc::new(MemorySink::new());
    let primary = ShardManager::open_with_sink(pdir.path(), cfg(1), Some(sink.clone())).unwrap();

    primary
        .execute_one(
            S0,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL) STRICT",
        )
        .unwrap();
    for i in 1..=200 {
        primary.execute_one(S0, insert(i)).unwrap();
    }

    // The follower is new and has none of that history.
    let follower = Follower::open(fdir.path()).unwrap();
    let epoch = primary.epoch().unwrap();
    assert!(
        matches!(follower.need(S0, epoch), Need::Bootstrap { .. }),
        "a follower with no copy must ask to be seeded"
    );

    let snap = snapdir.path().join("s0.snapshot");
    let (snap_epoch, snap_lsn) = primary.snapshot(S0, &snap).unwrap();
    follower
        .install_snapshot(S0, snap_epoch, snap_lsn, &snap)
        .unwrap();
    assert_eq!(follower.need(S0, epoch), Need::Next(snap_lsn + 1));

    // Frames from before the snapshot are already inside it, so discard them.
    let _ = sink.take(S0);

    // Keep writing; the follower now catches up by streaming.
    for i in 201..=350 {
        primary.execute_one(S0, insert(i)).unwrap();
    }
    let tail: Vec<StreamTxn> = sink.take(S0);
    assert!(!tail.is_empty(), "expected frames after the snapshot");
    assert_eq!(
        tail[0].lsn,
        snap_lsn + 1,
        "the stream must resume exactly where the snapshot ended"
    );
    follower.apply(S0, epoch, &tail).unwrap();

    assert_converged(&primary, &follower, S0);
    let conn = meshdb::rusqlite::Connection::open(S0.path(follower.dir())).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 350);
}

#[test]
fn a_clean_restart_continues_the_stream_without_rebootstrap() {
    // Re-bootstrapping on every restart would mean copying the whole database each time.
    // A clean shutdown persists the position so the stream simply continues.
    let pdir = TempDir::new().unwrap();

    let (epoch1, last1) = {
        let sink = Arc::new(MemorySink::new());
        let p = ShardManager::open_with_sink(pdir.path(), cfg(1), Some(sink.clone())).unwrap();
        p.execute_one(S0, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
            .unwrap();
        for i in 1..=30 {
            p.execute_one(S0, format!("INSERT INTO t VALUES ({i})"))
                .unwrap();
        }
        let e = p.epoch().unwrap();
        let last = sink.take(S0).last().unwrap().lsn;
        (e, last)
        // dropped cleanly here, which persists the position
    };

    let sink = Arc::new(MemorySink::new());
    let p2 = ShardManager::open_with_sink(pdir.path(), cfg(1), Some(sink.clone())).unwrap();
    p2.execute_one(S0, "INSERT INTO t VALUES (999)").unwrap();

    assert_eq!(
        p2.epoch().unwrap(),
        epoch1,
        "a clean restart must not bump the epoch, or every follower re-copies the database"
    );
    let resumed = sink.take(S0);
    assert_eq!(
        resumed[0].lsn,
        last1 + 1,
        "the stream must resume at the next LSN, not restart"
    );
}

#[test]
fn capture_off_means_no_stream_at_all() {
    let pdir = TempDir::new().unwrap();
    let fdir = TempDir::new().unwrap();
    let follower = Arc::new(Follower::open(fdir.path()).unwrap());
    let sink = Arc::new(FollowerSink::new(Arc::clone(&follower)));

    let mut c = cfg(1);
    c.capture = false;
    let primary = ShardManager::open_with_sink(pdir.path(), c, Some(sink)).unwrap();

    primary
        .execute_one(S0, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    for i in 1..=50 {
        primary
            .execute_one(S0, format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }

    assert_eq!(follower.position(S0).applied_lsn, 0);
    assert!(primary.epoch().is_none());
}

#[test]
fn an_empty_follower_may_join_a_stream_at_its_beginning() {
    // The counterpart to `need` being conservative: when the frames in hand genuinely start
    // at LSN 1 and the follower holds nothing, no gap is possible, so `apply` accepts.
    let pdir = TempDir::new().unwrap();
    let fdir = TempDir::new().unwrap();
    let sink = Arc::new(MemorySink::new());
    let primary = ShardManager::open_with_sink(pdir.path(), cfg(1), Some(sink.clone())).unwrap();

    primary
        .execute_one(S0, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    for i in 1..=20 {
        primary
            .execute_one(S0, format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }

    let epoch = primary.epoch().unwrap();
    let all: Vec<StreamTxn> = sink.take(S0);
    assert_eq!(all[0].lsn, 1, "the primary's stream should start at 1 here");

    let follower = Follower::open(fdir.path()).unwrap();
    follower.apply(S0, epoch, &all).unwrap();
    assert_converged(&primary, &follower, S0);
}

#[test]
fn an_empty_follower_cannot_join_mid_stream() {
    // The other half: nothing held locally and frames that do not start at 1 means the
    // prefix is missing and only a snapshot can supply it.
    let pdir = TempDir::new().unwrap();
    let fdir = TempDir::new().unwrap();
    let sink = Arc::new(MemorySink::new());
    let primary = ShardManager::open_with_sink(pdir.path(), cfg(1), Some(sink.clone())).unwrap();

    primary
        .execute_one(S0, "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT")
        .unwrap();
    for i in 1..=20 {
        primary
            .execute_one(S0, format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }

    let epoch = primary.epoch().unwrap();
    let all: Vec<StreamTxn> = sink.take(S0);
    let tail: Vec<StreamTxn> = all.into_iter().skip(5).collect();

    let follower = Follower::open(fdir.path()).unwrap();
    match follower.apply(S0, epoch, &tail) {
        Err(Error::ReplicationGap { expected, .. }) => assert_eq!(expected, 1),
        other => panic!("expected ReplicationGap joining mid-stream, got {other:?}"),
    }
}
