//! Step 5 acceptance: many shards served by a bounded number of threads and connections,
//! with the writer-opens-first invariant surviving every eviction and reopen.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use meshdb::shard::{ShardConfig, ShardId, ShardManager, shard_of};
use meshdb::storage::Value;
use meshdb::storage::exec::{Executed, Outcome};
use tempfile::TempDir;

fn scalar(o: &Outcome) -> i64 {
    match o {
        Outcome::Ok(Executed::Rows(r)) => match &r.rows[0][0] {
            Value::Integer(i) => *i,
            v => panic!("expected an integer, got {v:?}"),
        },
        other => panic!("expected rows, got {other:?}"),
    }
}

fn manager(dir: &TempDir, cfg: ShardConfig) -> ShardManager {
    let m = ShardManager::open(dir.path(), cfg).unwrap();
    for (_, outcome) in m
        .execute_all_shards("CREATE TABLE t (k TEXT PRIMARY KEY, v INTEGER NOT NULL) STRICT")
        .unwrap()
    {
        assert!(matches!(outcome, Outcome::Ok(_)), "DDL failed: {outcome:?}");
    }
    m
}

#[test]
fn writes_and_reads_route_to_the_same_shard() {
    let dir = TempDir::new().unwrap();
    let m = manager(&dir, ShardConfig::floor());

    for i in 0..500 {
        let key = format!("key-{i}");
        let shard = m.route(key.as_bytes());
        m.execute_one(shard, &format!("INSERT INTO t VALUES ('{key}', {i})"))
            .unwrap();
    }

    // Every key must be found on the shard routing sends it to.
    for i in 0..500 {
        let key = format!("key-{i}");
        let shard = m.route(key.as_bytes());
        let out = m
            .query(shard, &format!("SELECT v FROM t WHERE k = '{key}'"))
            .unwrap();
        assert_eq!(scalar(&out), i, "key {key} missing from {shard}");
    }

    // And the rows really are spread across shards, not all in one.
    let mut populated = 0;
    for s in 0..m.config().shard_count {
        let n = scalar(&m.query(ShardId(s), "SELECT count(*) FROM t").unwrap());
        if n > 0 {
            populated += 1;
        }
    }
    assert!(
        populated > 40,
        "expected keys spread over most shards, only {populated} populated"
    );
}

#[test]
fn sixty_four_shards_are_served_by_a_bounded_number_of_connections() {
    // The core claim of step 5: shard count and resident connection count are decoupled.
    let cfg = ShardConfig::floor();
    let dir = TempDir::new().unwrap();
    let m = manager(&dir, cfg.clone());

    // Touch every shard, repeatedly, in an order guaranteed to thrash an 8-entry LRU.
    for round in 0..3 {
        for s in 0..cfg.shard_count {
            m.execute_one(
                ShardId(s),
                &format!("INSERT INTO t VALUES ('r{round}-s{s}', {s})"),
            )
            .unwrap();
        }
    }

    let stats = m.writer_stats();
    println!("{stats:?}");

    let ceiling = (cfg.open_writers_per_thread * cfg.writer_threads) as u64;
    assert!(
        stats.open_now <= ceiling,
        "resident writer connections {} exceed the {ceiling} the LRU allows",
        stats.open_now
    );
    assert!(
        stats.shard_evictions > 0,
        "64 shards through a {}-entry LRU must evict",
        cfg.open_writers_per_thread
    );
    assert_eq!(stats.threads, cfg.writer_threads);

    // All data survived the eviction churn.
    let mut total = 0;
    for s in 0..cfg.shard_count {
        total += scalar(&m.query(ShardId(s), "SELECT count(*) FROM t").unwrap());
    }
    assert_eq!(total, 3 * cfg.shard_count as i64);
}

#[test]
fn a_never_written_shard_can_still_be_queried() {
    // This is what makes `ensure_open` load-bearing: a read-only connection cannot create
    // the database file, so a shard nobody has written to has no file for a reader to
    // open. Without the writer being asked to bring it into existence first, this errors
    // instead of answering.
    let dir = TempDir::new().unwrap();
    let cfg = ShardConfig {
        shard_count: 16,
        ..ShardConfig::floor()
    };
    let m = ShardManager::open(dir.path(), cfg).unwrap();

    // No write has ever touched shard 9 — not even DDL.
    let out = m
        .query(ShardId(9), "SELECT count(*) FROM sqlite_schema")
        .unwrap();
    assert_eq!(scalar(&out), 0, "an empty shard should answer, not fail");

    assert!(
        ShardId(9).path(dir.path()).exists(),
        "the query should have brought the shard file into existence"
    );
}

#[test]
fn a_reopened_shard_still_serves_readers() {
    // Eviction churn must not lose data or break access. Note that an ordinary *clean*
    // close checkpoints the WAL into the main file and removes the sidecars, and a
    // read-only connection reads that file happily — so eviction alone is benign. What
    // this covers is that the whole open/evict/reopen cycle stays correct under load.
    let cfg = ShardConfig {
        shard_count: 32,
        writer_threads: 1,
        open_writers_per_thread: 2, // tiny, to force constant eviction
        reader_threads: 1,
        open_readers_per_thread: 2,
    };
    let dir = TempDir::new().unwrap();
    let m = manager(&dir, cfg.clone());

    for s in 0..cfg.shard_count {
        m.execute_one(ShardId(s), &format!("INSERT INTO t VALUES ('k{s}', {s})"))
            .unwrap();
    }

    // Read them back in an order that guarantees each shard was long since evicted.
    for s in 0..cfg.shard_count {
        let out = m
            .query(ShardId(s), &format!("SELECT v FROM t WHERE k = 'k{s}'"))
            .unwrap();
        assert_eq!(
            scalar(&out),
            s as i64,
            "shard {s} unreadable after eviction"
        );
    }

    // And writes still work after all that reopening.
    for s in 0..cfg.shard_count {
        m.execute_one(
            ShardId(s),
            &format!("INSERT INTO t VALUES ('late{s}', {s})"),
        )
        .unwrap();
    }
    for s in 0..cfg.shard_count {
        assert_eq!(
            scalar(&m.query(ShardId(s), "SELECT count(*) FROM t").unwrap()),
            2
        );
    }

    let ws = m.writer_stats();
    assert!(
        ws.shard_evictions > 20,
        "expected heavy eviction with a 2-entry LRU, got {}",
        ws.shard_evictions
    );
}

#[test]
fn concurrent_writers_across_shards() {
    let cfg = ShardConfig::floor();
    let dir = TempDir::new().unwrap();
    let m = Arc::new(manager(&dir, cfg.clone()));

    const THREADS: usize = 8;
    const PER_THREAD: usize = 40;
    let failures = Arc::new(AtomicUsize::new(0));

    std::thread::scope(|s| {
        for t in 0..THREADS {
            let m = Arc::clone(&m);
            let failures = Arc::clone(&failures);
            s.spawn(move || {
                for i in 0..PER_THREAD {
                    let key = format!("t{t}-i{i}");
                    let shard = m.route(key.as_bytes());
                    if !matches!(
                        m.execute_one(shard, &format!("INSERT INTO t VALUES ('{key}', {i})")),
                        Ok(Outcome::Ok(_))
                    ) {
                        failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });

    assert_eq!(failures.load(Ordering::Relaxed), 0);

    let mut total = 0;
    for s in 0..cfg.shard_count {
        total += scalar(&m.query(ShardId(s), "SELECT count(*) FROM t").unwrap());
    }
    assert_eq!(total, (THREADS * PER_THREAD) as i64);
}

#[test]
fn a_rejected_statement_on_one_shard_does_not_affect_others() {
    let dir = TempDir::new().unwrap();
    let m = manager(&dir, ShardConfig::floor());

    m.execute_one(ShardId(0), "INSERT INTO t VALUES ('dup', 1)")
        .unwrap();
    let dup = m
        .execute_one(ShardId(0), "INSERT INTO t VALUES ('dup', 2)")
        .unwrap();
    assert!(matches!(dup, Outcome::Rejected(_)), "got {dup:?}");

    // A neighbouring shard is untouched and still writable.
    m.execute_one(ShardId(1), "INSERT INTO t VALUES ('ok', 1)")
        .unwrap();
    assert_eq!(
        scalar(&m.query(ShardId(1), "SELECT count(*) FROM t").unwrap()),
        1
    );
    assert_eq!(
        scalar(&m.query(ShardId(0), "SELECT count(*) FROM t").unwrap()),
        1
    );
}

#[test]
fn a_shard_outside_the_configured_range_is_refused() {
    let dir = TempDir::new().unwrap();
    let cfg = ShardConfig {
        shard_count: 4,
        ..ShardConfig::floor()
    };
    let m = manager(&dir, cfg);
    assert!(m.execute_one(ShardId(99), "SELECT 1").is_err());
    assert!(m.route(b"anything").0 < 4);
}

#[test]
fn only_touched_shards_create_files() {
    // A 64-shard cluster should not pay for 64 files until the shards are used.
    let dir = TempDir::new().unwrap();
    let cfg = ShardConfig {
        shard_count: 64,
        ..ShardConfig::floor()
    };
    let m = ShardManager::open(dir.path(), cfg).unwrap();

    m.execute_one(ShardId(3), "CREATE TABLE t (a INTEGER) STRICT")
        .unwrap();
    m.execute_one(ShardId(7), "CREATE TABLE t (a INTEGER) STRICT")
        .unwrap();

    let files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".db"))
        .collect();
    assert_eq!(
        files.len(),
        2,
        "expected only touched shards, got {files:?}"
    );
}

#[test]
fn routing_respects_the_configured_shard_count() {
    for count in [1u32, 4, 16, 64, 256] {
        for i in 0..200 {
            let s = shard_of(format!("k{i}").as_bytes(), count);
            assert!(s.0 < count, "{s} out of range for {count} shards");
        }
    }
}
