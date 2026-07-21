//! Cross-node routing at the mechanism level. Two `ShardManager`s, each owning half the shards;
//! each has a `PeerRouter` that forwards the other half to its peer (in-process, no networking).
//! This proves the interception in `query`/`execute_one` makes fan-outs and `run_routed` span
//! nodes — the same seam the real TCP `Router` plugs into over the wire.

use std::collections::HashSet;
use std::sync::Arc;

use meshdb::shard::{PeerRouter, ShardConfig, ShardId, ShardManager};
use meshdb::storage::Value;
use meshdb::storage::exec::{Executed, Outcome, Statement};
use tempfile::TempDir;

const N: u32 = 8;

fn cfg() -> ShardConfig {
    ShardConfig {
        shard_count: N,
        writer_threads: 2,
        reader_threads: 2,
        ..ShardConfig::floor()
    }
}

/// Owns the shards in `mine`; forwards every other shard to `other`.
struct Split {
    mine: HashSet<u32>,
    other: Arc<ShardManager>,
}

impl PeerRouter for Split {
    fn is_local(&self, shard: ShardId) -> bool {
        self.mine.contains(&shard.0)
    }
    fn query_remote(&self, shard: ShardId, stmt: Statement) -> meshdb::Result<Outcome> {
        self.other.query(shard, stmt)
    }
    fn execute_remote(&self, shard: ShardId, stmt: Statement) -> meshdb::Result<Outcome> {
        self.other.execute_one(shard, stmt)
    }
}

/// Node A owns even shards, node B owns odd. Each forwards the other half to its peer.
fn two_nodes() -> (Arc<ShardManager>, Arc<ShardManager>, TempDir, TempDir) {
    let da = TempDir::new().unwrap();
    let db = TempDir::new().unwrap();
    let a = Arc::new(ShardManager::open(da.path(), cfg()).unwrap());
    let b = Arc::new(ShardManager::open(db.path(), cfg()).unwrap());
    let even: HashSet<u32> = (0..N).filter(|s| s % 2 == 0).collect();
    let odd: HashSet<u32> = (0..N).filter(|s| s % 2 == 1).collect();
    a.set_peer_router(Arc::new(Split {
        mine: even,
        other: Arc::clone(&b),
    }));
    b.set_peer_router(Arc::new(Split {
        mine: odd,
        other: Arc::clone(&a),
    }));
    (a, b, da, db)
}

fn rows(o: Outcome) -> Vec<Vec<Value>> {
    match o {
        Outcome::Ok(Executed::Rows(r)) => r.rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn affected(o: Outcome) -> u64 {
    match o {
        Outcome::Ok(Executed::Changed(w)) => w.rows_affected,
        other => panic!("expected a write, got {other:?}"),
    }
}

/// Rows held on `m`'s own shards only (bypasses the fan-out): sum a per-owned-shard count.
fn local_count(m: &ShardManager, owned: &[u32]) -> i64 {
    let mut total = 0;
    for &s in owned {
        let o = m.query(ShardId(s), "SELECT count(*) FROM users").unwrap();
        if let Value::Integer(n) = rows(o)[0][0] {
            total += n;
        }
    }
    total
}

#[test]
fn writes_and_reads_span_two_nodes() {
    let (a, b, _da, _db) = two_nodes();

    // DDL issued on A reaches every shard — the odd ones are created on B by forwarding.
    a.execute_all_shards("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT) STRICT")
        .unwrap();

    // Insert through A's router. Each row lands on the shard's owner — some on A, some on B.
    for i in 0..200 {
        assert_eq!(
            affected(
                a.run_routed(Statement::new(format!(
                    "INSERT INTO users (id, name) VALUES ('user-{i}', 'n{i}')"
                )))
                .unwrap()
            ),
            1
        );
    }

    // The data is genuinely split across the two nodes, not piled on one.
    let on_a = local_count(&a, &[0, 2, 4, 6]);
    let on_b = local_count(&b, &[1, 3, 5, 7]);
    assert_eq!(on_a + on_b, 200, "rows lost across the split");
    assert!(on_a > 0 && on_b > 0, "not spread: A={on_a} B={on_b}");

    // A fan-out COUNT from A gathers partials from both nodes.
    assert_eq!(
        a.query_all_shards("SELECT count(*) FROM users")
            .unwrap()
            .rows,
        vec![vec![Value::Integer(200)]]
    );
    // The same query coordinated from B is equally correct — any node can be the entry point.
    assert_eq!(
        b.query_all_shards("SELECT count(*) FROM users")
            .unwrap()
            .rows,
        vec![vec![Value::Integer(200)]]
    );

    // A grouped fan-out read (the two-phase merge) also spans nodes.
    let grouped = a
        .query_all_shards("SELECT count(*) AS c FROM users GROUP BY substr(id, 1, 4)")
        .unwrap();
    let sum: i64 = grouped
        .rows
        .iter()
        .map(|r| match r[0] {
            Value::Integer(n) => n,
            _ => 0,
        })
        .sum();
    assert_eq!(sum, 200, "grouped fan-out across nodes lost rows");

    // A point read routes to whichever node owns the key's shard and finds the row.
    assert_eq!(
        rows(
            a.run_routed(Statement::new("SELECT name FROM users WHERE id = 'user-7'"))
                .unwrap()
        ),
        vec![vec![Value::Text("n7".into())]]
    );

    // A point UPDATE reaches the owning node.
    assert_eq!(
        affected(
            a.run_routed(Statement::new(
                "UPDATE users SET name = 'x' WHERE id = 'user-7'"
            ))
            .unwrap()
        ),
        1
    );
    assert_eq!(
        rows(
            b.run_routed(Statement::new("SELECT name FROM users WHERE id = 'user-7'"))
                .unwrap()
        ),
        vec![vec![Value::Text("x".into())]],
        "the update coordinated from A must be visible from B"
    );

    // An unpinned DELETE touches every shard on both nodes.
    assert_eq!(
        affected(a.run_routed(Statement::new("DELETE FROM users")).unwrap()),
        200
    );
    assert_eq!(
        a.query_all_shards("SELECT count(*) FROM users")
            .unwrap()
            .rows,
        vec![vec![Value::Integer(0)]]
    );
}
