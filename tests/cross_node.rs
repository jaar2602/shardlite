//! Cross-node routing at the mechanism level. Two `ShardManager`s, each owning half the shards;
//! each has a `PeerRouter` that forwards the other half to its peer (in-process, no networking).
//! This proves the interception in `query`/`execute_one` makes fan-outs and `run_routed` span
//! nodes — the same seam the real TCP `Router` plugs into over the wire.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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

/// Owns the shards in `mine`; forwards every other shard to `other`. Counters record how the
/// fan-out reached the peer: `fan_out_calls` is the number of batched round trips, `remote_seen`
/// the total shards those calls carried — so a batch shows as few calls covering many shards.
struct Split {
    mine: HashSet<u32>,
    other: Arc<ShardManager>,
    fan_out_calls: Arc<AtomicUsize>,
    remote_seen: Arc<AtomicUsize>,
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
    fn fan_out(
        &self,
        shards: &[ShardId],
        statement: &Statement,
        write: bool,
    ) -> Vec<meshdb::Result<Outcome>> {
        // One call, all remote shards — the whole point of batching. (There is only one peer here,
        // so every remote shard is in this single call; a real Router splits by owner node.)
        self.fan_out_calls.fetch_add(1, Ordering::SeqCst);
        self.remote_seen.fetch_add(shards.len(), Ordering::SeqCst);
        shards
            .iter()
            .map(|&id| {
                if write {
                    self.other.execute_one(id, statement.clone())
                } else {
                    self.other.query(id, statement.clone())
                }
            })
            .collect()
    }
}

/// Node A owns even shards, node B owns odd. Each forwards the other half to its peer. Returns
/// node A's fan-out counters (`calls`, `remote_shards_seen`) so a test can assert batching.
struct TwoNodes {
    a: Arc<ShardManager>,
    b: Arc<ShardManager>,
    a_calls: Arc<AtomicUsize>,
    a_remote: Arc<AtomicUsize>,
    _dirs: (TempDir, TempDir),
}

fn two_nodes() -> TwoNodes {
    let da = TempDir::new().unwrap();
    let db = TempDir::new().unwrap();
    let a = Arc::new(ShardManager::open(da.path(), cfg()).unwrap());
    let b = Arc::new(ShardManager::open(db.path(), cfg()).unwrap());
    let even: HashSet<u32> = (0..N).filter(|s| s % 2 == 0).collect();
    let odd: HashSet<u32> = (0..N).filter(|s| s % 2 == 1).collect();
    let a_calls = Arc::new(AtomicUsize::new(0));
    let a_remote = Arc::new(AtomicUsize::new(0));
    a.set_peer_router(Arc::new(Split {
        mine: even,
        other: Arc::clone(&b),
        fan_out_calls: Arc::clone(&a_calls),
        remote_seen: Arc::clone(&a_remote),
    }));
    b.set_peer_router(Arc::new(Split {
        mine: odd,
        other: Arc::clone(&a),
        fan_out_calls: Arc::new(AtomicUsize::new(0)),
        remote_seen: Arc::new(AtomicUsize::new(0)),
    }));
    TwoNodes {
        a,
        b,
        a_calls,
        a_remote,
        _dirs: (da, db),
    }
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
    // Keep `_dirs` bound: it owns the TempDirs, and dropping it would delete the databases.
    let TwoNodes { a, b, _dirs, .. } = two_nodes();

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

#[test]
fn a_fan_out_batches_remote_shards_into_one_call_per_owner() {
    // With 8 shards split evenly, node A owns 4 and node B owns 4. A cross-shard read from A must
    // reach B's 4 shards in ONE batched call, not four separate forwards.
    let TwoNodes {
        a,
        a_calls,
        a_remote,
        _dirs,
        ..
    } = two_nodes();
    a.execute_all_shards("CREATE TABLE t (id TEXT PRIMARY KEY, v INTEGER) STRICT")
        .unwrap();
    for i in 0..80 {
        a.run_routed(Statement::new(format!(
            "INSERT INTO t (id, v) VALUES ('k{i}', {i})"
        )))
        .unwrap();
    }

    // Reset the counters, then run exactly one fan-out read.
    a_calls.store(0, Ordering::SeqCst);
    a_remote.store(0, Ordering::SeqCst);
    let total = a.query_all_shards("SELECT count(*) FROM t").unwrap();
    assert_eq!(total.rows, vec![vec![Value::Integer(80)]]);

    // One batched call to the one peer, carrying all 4 of its shards — not 4 calls.
    assert_eq!(
        a_calls.load(Ordering::SeqCst),
        1,
        "a fan-out should make one batched call per owner node"
    );
    assert_eq!(
        a_remote.load(Ordering::SeqCst),
        (N / 2) as usize,
        "the single batch should carry all of the peer's shards"
    );
}
