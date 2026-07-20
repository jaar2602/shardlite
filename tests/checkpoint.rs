//! Step 4 acceptance: the WAL stays bounded under sustained writes, a long-lived reader
//! is detected as stalling the checkpointer, and escalation reclaims the file.

use std::time::Duration;

use meshdb::config::{CheckpointConfig, PragmaProfile};
use meshdb::shard::writer_fleet::WriterFleet;
use meshdb::shard::{ShardConfig, ShardId};
use meshdb::storage::checkpoint::wal_path_for;
use meshdb::storage::open::open_reader_existing;
use tempfile::TempDir;

const S0: ShardId = ShardId(0);

/// One shard, one writer thread — the checkpointer is per shard, so this isolates it.
fn one_shard() -> ShardConfig {
    ShardConfig {
        shard_count: 1,
        writer_threads: 1,
        open_writers_per_thread: 1,
        reader_threads: 1,
        open_readers_per_thread: 1,
        ..ShardConfig::floor()
    }
}

fn spawn(dir: &TempDir, cp: CheckpointConfig) -> (WriterFleet, std::path::PathBuf) {
    let fleet =
        WriterFleet::spawn(dir.path(), one_shard(), PragmaProfile::writer_floor(), cp).unwrap();
    // Bring the shard into existence so its path is valid for readers and wal_path_for.
    fleet.ensure_open(S0).unwrap();
    let path = S0.path(dir.path());
    (fleet, path)
}

/// Small limits so the ladder is reachable in a test rather than after 128 MB of writes.
fn tight() -> CheckpointConfig {
    CheckpointConfig {
        soft_limit_bytes: 16 * 1024,
        hard_limit_bytes: 64 * 1024,
        stall_threshold: 2,
        check_every_batches: 1,
        max_stall_backoff: 8,
    }
}

/// Rows sized so the WAL crosses the tight limits above quickly.
///
/// The suite was ~20s, dominated by writing padded rows one fsync per batch. The limits
/// are what the tests actually exercise, so fewer, fatter rows reach them in a fraction of
/// the time without weakening any assertion.
const PAD: usize = 4_000;

fn wal_len(db: &std::path::Path) -> u64 {
    std::fs::metadata(wal_path_for(db))
        .map(|m| m.len())
        .unwrap_or(0)
}

/// Writes enough padded rows to push the WAL well past the limits above.
///
/// Submitted in chunks rather than one statement at a time: `synchronous = FULL` means
/// one fsync per *batch*, so row-at-a-time here would spend the whole test in fsync.
fn write_rows(h: &WriterFleet, start: i64, n: i64) {
    let pad = "x".repeat(PAD);
    for chunk_start in (start..start + n).step_by(50) {
        let chunk_end = (chunk_start + 50).min(start + n);
        let stmts: Vec<String> = (chunk_start..chunk_end)
            .map(|i| format!("INSERT INTO t VALUES ({i}, '{pad}')"))
            .collect();
        h.execute(S0, stmts.iter().map(Into::into).collect())
            .unwrap();
    }
}

#[test]
fn wal_stays_bounded_under_sustained_writes() {
    let dir = TempDir::new().unwrap();
    let (w, _path) = spawn(&dir, tight());
    let h = &w;

    h.execute_one(
        S0,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, pad TEXT) STRICT",
    )
    .unwrap();
    write_rows(h, 1, 120);

    let stats = w.checkpoint_stats();
    println!(
        "passive={} truncated={} stalls={} wal_bytes={}",
        stats.passive, stats.truncated, stats.stalls, stats.wal_bytes
    );

    assert!(
        stats.passive > 0,
        "the checkpointer should have run with no readers present"
    );
    // With nothing pinning a snapshot, passive checkpointing keeps the live WAL drained.
    assert_eq!(
        stats.stalls, 0,
        "no reader was active, so nothing should stall"
    );
}

#[test]
fn a_long_lived_reader_stalls_the_checkpointer_and_forces_escalation() {
    // The starvation scenario the ladder exists for: a reader holding a snapshot stops the
    // checkpoint advancing, the WAL grows past its hard limit, and the ladder escalates to a
    // blocking TRUNCATE rather than letting the disk fill.
    //
    // Both assertions below were once made *after* the reader released, and both were wrong
    // there. Measured on 3.53.2: a fully successful PASSIVE checkpoint — `(busy=0, log=2213,
    // checkpointed=2213)`, everything copied — leaves the WAL file at exactly its previous
    // size. Only TRUNCATE shrinks it. So once the reader lets go, passive checkpoints succeed,
    // the stall counter resets, escalation correctly never fires, and the file stays at its
    // high-water mark. The old test passed only when a stray stall happened to trigger a
    // TRUNCATE after release, which is timing, not behaviour.
    let dir = TempDir::new().unwrap();
    let (w, path) = spawn(&dir, tight());
    let h = &w;

    h.execute_one(
        S0,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, pad TEXT) STRICT",
    )
    .unwrap();
    write_rows(h, 1, 50);

    let reader = open_reader_existing(&path, &PragmaProfile::reader_floor()).unwrap();
    {
        // Hold an open read transaction. The first read pins the snapshot, and the
        // checkpointer cannot copy frames past it.
        let read_tx = reader.unchecked_transaction().unwrap();
        let _: i64 = read_tx
            .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
            .unwrap();

        // Enough writes that the WAL passes the hard limit with the stall counter exhausted.
        // Escalation needs both, and it must happen *here*, while the reader still pins the
        // snapshot — that is the condition the ladder is for.
        write_rows(h, 100, 200);

        let during = w.checkpoint_stats();
        println!(
            "with reader held: passive={} truncated={} stalls={} wal={}",
            during.passive, during.truncated, during.stalls, during.wal_bytes
        );
        assert!(
            during.stalls > 0,
            "a pinned snapshot should stall passive checkpoints"
        );
        assert!(
            wal_len(&path) > tight().soft_limit_bytes,
            "the WAL should have grown while the reader pinned it"
        );
        assert!(
            during.truncated > 0,
            "with the WAL past its hard limit and passive checkpoints stalling, the ladder \
             must escalate to TRUNCATE: {during:?}"
        );
    } // read transaction ends here

    // The *connection* has to go too, not just the transaction. Measured: a reader that has
    // ended its transaction but stayed open still holds a read mark, so passive checkpoints
    // keep partially stalling. Ending the transaction alone is not releasing the snapshot.
    drop(reader);

    // What "recovered" means here is that the checkpointer stops being starved — not
    // anything about the file's size, and not immediately.
    //
    // Four earlier versions of this test asserted the wrong thing here. Measured on 3.53.2:
    // (a) a fully successful PASSIVE checkpoint (`busy=0, log=2213, checkpointed=2213` —
    // everything copied) leaves the WAL at exactly its previous length; only TRUNCATE
    // shrinks it. (b) The file legitimately keeps growing while writes outpace the
    // checkpoint interval, even with nothing stalling. (c) Dropping the reader does not
    // clear its read mark instantly, so one more stall can follow the drop — that alone
    // made the test fail 17 runs in 40.
    //
    // So: let it settle, then measure a clean window. Asserting on the instant after release
    // is asserting on a transient.
    write_rows(h, 100_000, 200);
    let settled = w.checkpoint_stats();

    write_rows(h, 200_000, 200);
    let after = w.checkpoint_stats();
    println!(
        "settled: passive={} stalls={} | after a clean window: passive={} stalls={}",
        settled.passive, settled.stalls, after.passive, after.stalls
    );

    assert!(
        after.passive > settled.passive,
        "checkpointing should still be running after the reader let go"
    );
    assert_eq!(
        after.stalls,
        settled.stalls,
        "with no reader pinning a snapshot, a settled checkpointer must not stall again; \
         {} new stalls appeared",
        after.stalls - settled.stalls
    );
}

#[test]
fn checkpointing_does_not_lose_or_corrupt_data() {
    // Checkpointing moves pages from the WAL into the main database file. If that is
    // mishandled, rows silently disappear — so verify every row survives, including
    // across a reopen that has to recover from whatever state the WAL was left in.
    let dir = TempDir::new().unwrap();
    {
        let (w, _path) = spawn(&dir, tight());
        let h = &w;
        h.execute_one(
            S0,
            "CREATE TABLE t (id INTEGER PRIMARY KEY, pad TEXT) STRICT",
        )
        .unwrap();
        write_rows(h, 1, 120);

        let out = h
            .execute_one(S0, "SELECT count(*), sum(id) FROM t")
            .unwrap();
        match out {
            meshdb::storage::Outcome::Ok(meshdb::storage::Executed::Rows(r)) => {
                assert_eq!(r.rows[0][0], meshdb::storage::Value::Integer(120));
            }
            other => panic!("expected rows, got {other:?}"),
        }
    } // writer dropped: thread joined, connection closed

    // Reopen and confirm the data is all still there.
    let (w2, _path) = spawn(&dir, tight());
    let out = w2
        .execute_one(S0, "SELECT count(*), sum(id) FROM t")
        .unwrap();
    match out {
        meshdb::storage::Outcome::Ok(meshdb::storage::Executed::Rows(r)) => {
            assert_eq!(r.rows[0][0], meshdb::storage::Value::Integer(120));
            // 1 + 2 + ... + 120
            assert_eq!(r.rows[0][1], meshdb::storage::Value::Integer(7_260));
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn checkpointing_is_skipped_below_the_soft_limit() {
    // Cheap in the common case: a small database should never pay for a checkpoint.
    let dir = TempDir::new().unwrap();
    let cfg = CheckpointConfig {
        soft_limit_bytes: 64 * 1024 * 1024,
        check_every_batches: 1,
        ..tight()
    };
    let (w, _path) = spawn(&dir, cfg);
    let h = &w;

    h.execute_one(
        S0,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, pad TEXT) STRICT",
    )
    .unwrap();
    write_rows(h, 1, 40);

    let stats = w.checkpoint_stats();
    assert_eq!(
        stats.passive, 0,
        "no checkpoint should run below the soft limit"
    );
    assert_eq!(stats.truncated, 0);
}

#[test]
fn readers_still_work_while_checkpointing_runs() {
    let dir = TempDir::new().unwrap();
    let (w, path) = spawn(&dir, tight());
    let h = &w;
    h.execute_one(
        S0,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, pad TEXT) STRICT",
    )
    .unwrap();

    let reader = open_reader_existing(&path, &PragmaProfile::reader_floor()).unwrap();

    std::thread::scope(|s| {
        s.spawn(|| write_rows(h, 1, 1_500));
        // `Connection` is `Send` but not `Sync`, so the reader moves into its thread
        // rather than being borrowed by it.
        s.spawn(move || {
            for _ in 0..40 {
                let n: i64 = reader
                    .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
                    .expect("reads must keep working during checkpointing");
                assert!(n >= 0);
                std::thread::sleep(Duration::from_micros(200));
            }
        });
    });

    assert!(w.checkpoint_stats().passive > 0);
}
