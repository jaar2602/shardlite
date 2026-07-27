//! Two-phase commit for writes that touch more than one shard.
//!
//! The property under test is the one D6 violated: a write that fails partway must leave *no*
//! trace, at any shard count. Before this, a five-row `INSERT` with one bad row reported failure
//! and left four rows behind — so a client that retried on failure double-inserted.
//!
//! Recovery is tested by constructing the durable record directly rather than by killing a
//! process. The interesting states are only two — decided and undecided — and building them by
//! hand exercises the same code the crash path runs, without a process harness.

use shardlite::shard::txn::{self, Decision, TxnRecord};
use shardlite::shard::{ShardConfig, ShardId, ShardManager};
use shardlite::storage::exec::{Executed, Outcome};
use tempfile::TempDir;

const COUNTS: [u32; 4] = [1, 3, 4, 8];

fn open_at(dir: &TempDir, shard_count: u32) -> ShardManager {
    ShardManager::open(
        dir.path(),
        ShardConfig {
            shard_count,
            ..ShardConfig::floor()
        },
    )
    .expect("open shard manager")
}

fn ids(m: &ShardManager, sql: &str) -> Vec<i64> {
    m.query_all_shards(sql)
        .expect("read")
        .rows
        .iter()
        .filter_map(|r| match r.first() {
            Some(shardlite::storage::Value::Integer(n)) => Some(*n),
            _ => None,
        })
        .collect()
}

/// D6, the primary case: one bad row in a multi-row `INSERT` must void the whole statement.
#[test]
fn a_failed_multi_row_insert_applies_nothing() {
    for count in COUNTS {
        let dir = TempDir::new().unwrap();
        let m = open_at(&dir, count);
        m.run_routed("CREATE TABLE p (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        m.run_routed("INSERT INTO p (id, v) VALUES (3, 'seed')")
            .unwrap();

        let outcome = m
            .run_routed("INSERT INTO p (id, v) VALUES (1,'a'),(2,'b'),(3,'dup'),(4,'d'),(5,'e')")
            .expect("the statement itself is well formed");
        assert!(
            matches!(outcome, Outcome::Rejected(ref msg) if msg.contains("UNIQUE")),
            "at N={count}, expected a rejection, got {outcome:?}"
        );
        assert_eq!(
            ids(&m, "SELECT id FROM p ORDER BY id"),
            vec![3],
            "at N={count}, a rejected multi-row INSERT left rows behind"
        );
    }
}

/// The same statement with no conflict must still apply in full — the guard against "fixing"
/// atomicity by rejecting everything.
#[test]
fn a_clean_multi_row_insert_applies_everything() {
    for count in COUNTS {
        let dir = TempDir::new().unwrap();
        let m = open_at(&dir, count);
        m.run_routed("CREATE TABLE p (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        m.run_routed("INSERT INTO p (id, v) VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")
            .expect("no conflict");
        assert_eq!(
            ids(&m, "SELECT id FROM p ORDER BY id"),
            vec![1, 2, 3, 4, 5],
            "at N={count}"
        );
    }
}

/// A keyless `UPDATE` broadcasts to every shard and had the identical defect.
#[test]
fn a_failed_broadcast_update_applies_nothing() {
    for count in COUNTS {
        let dir = TempDir::new().unwrap();
        let m = open_at(&dir, count);
        m.run_routed("CREATE TABLE w (id INTEGER PRIMARY KEY, v INTEGER CHECK (v < 100))")
            .unwrap();
        for i in 1..=8 {
            m.run_routed(format!("INSERT INTO w (id, v) VALUES ({i}, {i})"))
                .unwrap();
        }

        // Violates the CHECK only for rows where v >= 2, so at N>1 some shards succeed and some
        // fail. Setting every row to the same bad value would fail everywhere and pass vacuously.
        let outcome = m
            .run_routed("UPDATE w SET v = v * 50")
            .expect("well formed");
        assert!(
            matches!(outcome, Outcome::Rejected(_)),
            "at N={count}, expected a rejection, got {outcome:?}"
        );
        assert_eq!(
            ids(&m, "SELECT v FROM w ORDER BY v"),
            vec![1, 2, 3, 4, 5, 6, 7, 8],
            "at N={count}, a rejected broadcast UPDATE changed rows"
        );
    }
}

/// A DDL broadcast that fails partway must leave the schema untouched.
///
/// The failure has to be arranged on a *late* shard: making it fail on shard 0 aborts before
/// applying anything anywhere, which proves nothing.
#[test]
fn a_failed_ddl_broadcast_applies_nothing() {
    for count in COUNTS.iter().copied().filter(|c| *c > 1) {
        let dir = TempDir::new().unwrap();
        let m = open_at(&dir, count);
        let last = ShardId(count - 1);
        m.execute_one(last, "CREATE TABLE d (id INTEGER PRIMARY KEY)")
            .expect("pre-create on the last shard");

        let _ = m.run_routed("CREATE TABLE d (id INTEGER PRIMARY KEY)");

        let carriers = (0..count)
            .filter(|s| {
                matches!(
                    m.query(ShardId(*s), "SELECT COUNT(*) FROM sqlite_master WHERE name = 'd'"),
                    Ok(Outcome::Ok(Executed::Rows(ref r)))
                        if format!("{:?}", r.rows).contains("Integer(1)")
                )
            })
            .count();
        assert_eq!(
            carriers, 1,
            "at N={count}, a failed CREATE TABLE left the schema on {carriers} shards"
        );
    }
}

/// A successful cross-shard write leaves no record behind — the log is for in-flight work only.
#[test]
fn a_committed_transaction_leaves_no_record() {
    let dir = TempDir::new().unwrap();
    let m = open_at(&dir, 4);
    m.run_routed("CREATE TABLE p (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    m.run_routed("INSERT INTO p (id, v) VALUES (1,'a'),(2,'b'),(3,'c')")
        .unwrap();
    assert!(
        txn::pending(dir.path()).is_empty(),
        "a resolved transaction must not leave a record"
    );
}

/// An undecided record means no shard committed, because the decision is fsynced first. Recovery
/// drops it.
#[test]
fn recovery_drops_an_undecided_transaction() {
    let dir = TempDir::new().unwrap();
    {
        let m = open_at(&dir, 4);
        m.run_routed("CREATE TABLE p (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
    }
    txn::put(
        dir.path(),
        &TxnRecord {
            id: 900,
            participants: vec![
                (
                    0,
                    vec!["INSERT INTO p (id, v) VALUES (10, 'x')".to_string()],
                ),
                (
                    1,
                    vec!["INSERT INTO p (id, v) VALUES (11, 'y')".to_string()],
                ),
            ],
            ddl: false,
            decision: None,
        },
    )
    .unwrap();

    let m = open_at(&dir, 4);
    assert!(
        txn::pending(dir.path()).is_empty(),
        "record must be dropped"
    );
    assert!(
        ids(&m, "SELECT id FROM p").is_empty(),
        "an undecided transaction must not be applied"
    );
}

/// A decided record may have committed anywhere. SQLite rolled back whatever was merely prepared,
/// so recovery re-applies it wherever the marker says it did not land.
#[test]
fn recovery_reapplies_a_decided_transaction() {
    let dir = TempDir::new().unwrap();
    {
        let m = open_at(&dir, 4);
        m.run_routed("CREATE TABLE p (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
    }
    txn::put(
        dir.path(),
        &TxnRecord {
            id: 901,
            participants: vec![
                (
                    0,
                    vec!["INSERT INTO p (id, v) VALUES (10, 'x')".to_string()],
                ),
                (
                    1,
                    vec!["INSERT INTO p (id, v) VALUES (11, 'y')".to_string()],
                ),
            ],
            ddl: false,
            decision: Some(Decision::Commit),
        },
    )
    .unwrap();

    let m = open_at(&dir, 4);
    assert!(
        txn::pending(dir.path()).is_empty(),
        "record must be resolved"
    );
    assert_eq!(
        ids(&m, "SELECT id FROM p ORDER BY id"),
        vec![10, 11],
        "a decided transaction must be completed by recovery"
    );
}

/// Recovery must be idempotent: running it twice must not apply the rows twice. This is what the
/// per-shard marker table exists for.
#[test]
fn recovery_is_idempotent() {
    let dir = TempDir::new().unwrap();
    {
        let m = open_at(&dir, 4);
        m.run_routed("CREATE TABLE p (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
    }
    let record = TxnRecord {
        id: 902,
        participants: vec![(
            0,
            vec!["INSERT INTO p (id, v) VALUES (20, 'x')".to_string()],
        )],
        ddl: false,
        decision: Some(Decision::Commit),
    };
    txn::put(dir.path(), &record).unwrap();
    {
        let m = open_at(&dir, 4);
        assert_eq!(ids(&m, "SELECT id FROM p"), vec![20]);
        // Replay the same record: the marker must stop it applying a second time.
        txn::put(dir.path(), &record).unwrap();
        assert_eq!(m.recover_transactions().unwrap(), 1);
        assert_eq!(
            ids(&m, "SELECT id FROM p"),
            vec![20],
            "re-applying an already-applied transaction duplicated its rows"
        );
    }
}

/// Transaction ids must never be reused across a restart: the marker table is keyed by id, so a
/// repeat would make recovery skip a fresh transaction believing it had already landed.
#[test]
fn transaction_ids_do_not_restart() {
    let dir = TempDir::new().unwrap();
    txn::put(
        dir.path(),
        &TxnRecord {
            id: 5_000,
            participants: vec![(0, vec!["SELECT 1".to_string()])],
            ddl: false,
            decision: None,
        },
    )
    .unwrap();
    assert!(txn::next_id_after_recovery(dir.path()) > 5_000);
}

/// A parked shard must not block the shards beside it. This is the acceptance criterion for the
/// concurrency risk: shards map to writer threads by `id % writer_threads`, so a naive
/// implementation that blocked the thread would stall every sibling shard on it.
#[test]
fn a_cross_shard_write_does_not_stall_other_shards() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let dir = TempDir::new().unwrap();
    let m = Arc::new(open_at(&dir, 8));
    m.run_routed("CREATE TABLE p (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    m.run_routed("CREATE TABLE solo (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicUsize::new(0));

    // Single-shard writes, hammering while cross-shard transactions run.
    let writer = {
        let (m, stop, done) = (Arc::clone(&m), Arc::clone(&stop), Arc::clone(&done));
        std::thread::spawn(move || {
            let mut i = 0i64;
            while !stop.load(Ordering::Relaxed) {
                i += 1;
                if m.run_routed(format!("INSERT INTO solo (id, v) VALUES ({i}, 'x')"))
                    .is_ok()
                {
                    done.fetch_add(1, Ordering::Relaxed);
                }
            }
        })
    };

    for batch in 0..40i64 {
        let base = batch * 10 + 1;
        let rows: Vec<String> = (0..8).map(|k| format!("({}, 'y')", base + k)).collect();
        m.run_routed(format!("INSERT INTO p (id, v) VALUES {}", rows.join(",")))
            .expect("cross-shard insert");
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    assert!(
        done.load(Ordering::Relaxed) > 40,
        "single-shard writes made almost no progress ({}) while cross-shard \
         transactions ran — a parked shard is stalling its siblings",
        done.load(Ordering::Relaxed)
    );
    assert_eq!(ids(&m, "SELECT COUNT(*) FROM p"), vec![320]);
}

/// Concurrent cross-shard transactions must not deadlock, and must each apply completely.
#[test]
fn concurrent_cross_shard_writes_do_not_deadlock() {
    use std::sync::Arc;

    let dir = TempDir::new().unwrap();
    let m = Arc::new(open_at(&dir, 8));
    m.run_routed("CREATE TABLE p (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    let threads: Vec<_> = (0..4i64)
        .map(|t| {
            let m = Arc::clone(&m);
            std::thread::spawn(move || {
                for batch in 0..15i64 {
                    let base = t * 10_000 + batch * 10 + 1;
                    let rows: Vec<String> =
                        (0..8).map(|k| format!("({}, 'z')", base + k)).collect();
                    m.run_routed(format!("INSERT INTO p (id, v) VALUES {}", rows.join(",")))
                        .expect("cross-shard insert");
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }

    assert_eq!(ids(&m, "SELECT COUNT(*) FROM p"), vec![4 * 15 * 8]);
    assert!(txn::pending(dir.path()).is_empty());
}
