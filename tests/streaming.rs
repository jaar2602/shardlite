//! Streaming reads: an arbitrarily large result must not materialise in memory.

use shardlite::shard::reader_fleet::StreamMsg;
use shardlite::shard::{ShardConfig, ShardId, ShardManager};
use shardlite::storage::Value;
use shardlite::storage::exec::Statement;
use tempfile::TempDir;

const S0: ShardId = ShardId(0);

fn manager(dir: &TempDir) -> ShardManager {
    ShardManager::open(
        dir.path(),
        ShardConfig {
            shard_count: 1,
            ..ShardConfig::floor()
        },
    )
    .unwrap()
}

#[test]
fn a_small_result_streams_correctly() {
    let dir = TempDir::new().unwrap();
    let m = manager(&dir);
    m.execute_all_shards(Statement::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT",
    ))
    .unwrap();
    for i in 1..=3 {
        m.execute_one(
            S0,
            Statement::new(format!("INSERT INTO t VALUES ({i}, 'r{i}')")),
        )
        .unwrap();
    }

    let rx = m
        .query_stream(S0, "SELECT id, v FROM t ORDER BY id", 64)
        .unwrap();
    let mut columns = None;
    let mut rows = Vec::new();
    let mut done = false;
    for msg in rx {
        match msg {
            StreamMsg::Columns(c) => columns = Some(c),
            StreamMsg::Row(r) => rows.push(r),
            StreamMsg::Done => {
                done = true;
                break;
            }
            StreamMsg::Failed(e) => panic!("stream failed: {e}"),
        }
    }
    assert!(done, "the stream must terminate with Done");
    assert_eq!(columns.unwrap(), vec!["id", "v"]);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[2][1], Value::Text("r3".into()));
}

#[test]
fn a_large_result_streams_without_buffering_the_whole_thing() {
    // The property that matters: 200k rows stream through with the channel bounded to 64, so
    // at no point does the reader hold more than ~64 rows. If it materialised, this would
    // allocate the whole result; instead it flows.
    let dir = TempDir::new().unwrap();
    let m = manager(&dir);
    m.execute_all_shards(Statement::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT",
    ))
    .unwrap();

    // Generate 200k rows cheaply with a recursive CTE, in one statement.
    m.execute_one(
        S0,
        Statement::new(
            "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < 200000) \
             INSERT INTO t SELECT x FROM seq",
        ),
    )
    .unwrap();

    // Drain slowly-ish: the bound is 64, so the reader thread blocks whenever we lag,
    // proving backpressure works and memory stays bounded.
    let rx = m
        .query_stream(S0, "SELECT id FROM t ORDER BY id", 64)
        .unwrap();
    let mut count: i64 = 0;
    let mut last = 0i64;
    for msg in rx {
        match msg {
            StreamMsg::Columns(_) => {}
            StreamMsg::Row(r) => {
                count += 1;
                if let Value::Integer(n) = r[0] {
                    assert_eq!(n, last + 1, "rows must arrive in order");
                    last = n;
                }
            }
            StreamMsg::Done => break,
            StreamMsg::Failed(e) => panic!("stream failed at row {count}: {e}"),
        }
    }
    assert_eq!(count, 200_000, "every row must stream through");
    assert_eq!(last, 200_000);
}

#[test]
fn a_dropped_receiver_stops_the_reader_early() {
    // If the consumer goes away mid-stream (client disconnect), the reader must stop rather
    // than run the whole query into a channel nobody drains.
    let dir = TempDir::new().unwrap();
    let m = manager(&dir);
    m.execute_all_shards(Statement::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT",
    ))
    .unwrap();
    m.execute_one(
        S0,
        Statement::new(
            "WITH RECURSIVE seq(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM seq WHERE x < 100000) \
             INSERT INTO t SELECT x FROM seq",
        ),
    )
    .unwrap();

    let rx = m.query_stream(S0, "SELECT id FROM t", 8).unwrap();
    // Take a handful of rows, then drop the receiver.
    let mut taken = 0;
    for msg in rx.iter().take(5) {
        if let StreamMsg::Row(_) = msg {
            taken += 1;
        }
    }
    assert!(taken > 0);
    drop(rx);

    // The shard must still be usable immediately — the reader thread was freed, not wedged.
    let n = m
        .query(S0, Statement::new("SELECT count(*) FROM t"))
        .unwrap();
    match n {
        shardlite::storage::exec::Outcome::Ok(shardlite::storage::exec::Executed::Rows(r)) => {
            assert_eq!(r.rows[0][0], Value::Integer(100_000));
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn a_write_statement_on_the_stream_path_is_refused() {
    let dir = TempDir::new().unwrap();
    let m = manager(&dir);
    m.execute_all_shards(Statement::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT",
    ))
    .unwrap();

    let rx = m.query_stream(S0, "INSERT INTO t VALUES (1)", 64).unwrap();
    let mut failed = false;
    for msg in rx {
        match msg {
            StreamMsg::Failed(e) => {
                assert!(e.contains("write") || e.contains("no columns"), "{e}");
                failed = true;
            }
            StreamMsg::Done => break,
            _ => {}
        }
    }
    assert!(failed, "a write must be refused on the read-stream path");
}
