//! Rolling schema changes, and the guard that keeps the half-applied window honest.

use std::sync::Arc;

use shardlite::cluster::Fence;
use shardlite::shard::{ShardConfig, ShardId, ShardManager, WriteGate};
use shardlite::storage::Value;
use shardlite::storage::exec::{Executed, Outcome, Statement};
use shardlite::storage::schema::Agreement;
use tempfile::TempDir;

fn manager(shards: u32) -> (ShardManager, TempDir) {
    let dir = TempDir::new().unwrap();
    let m = ShardManager::open(
        dir.path(),
        ShardConfig {
            shard_count: shards,
            ..ShardConfig::floor()
        },
    )
    .unwrap();
    (m, dir)
}

fn scalar(o: Outcome) -> Value {
    match o {
        Outcome::Ok(Executed::Rows(r)) => r.rows[0][0].clone(),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn a_rolled_change_lands_on_every_shard_and_advances_each_version() {
    let (m, _d) = manager(4);
    assert_eq!(m.schema_version(ShardId(0)).unwrap(), 0);

    let applied = m
        .apply_ddl(Statement::new(
            "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT",
        ))
        .unwrap();
    assert_eq!(applied.len(), 4);
    for (_, v) in &applied {
        assert_eq!(*v, 1);
    }

    let second = m
        .apply_ddl(Statement::new("ALTER TABLE t ADD COLUMN note TEXT"))
        .unwrap();
    for (_, v) in &second {
        assert_eq!(*v, 2, "each change advances the version");
    }
    assert_eq!(m.schema_agreement().unwrap(), Agreement::Agreed(2));
}

#[test]
fn the_schema_and_its_version_move_together() {
    // If they could diverge, the version would be lying about what the shard holds — and the
    // version is what every cross-shard read trusts to decide whether it is safe.
    let (m, _d) = manager(2);
    m.apply_ddl(Statement::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT",
    ))
    .unwrap();

    for s in 0..2 {
        let shard = ShardId(s);
        assert_eq!(m.schema_version(shard).unwrap(), 1);
        // The table is really there, not just the version bump.
        m.execute_one(shard, Statement::new("INSERT INTO t VALUES (1)"))
            .unwrap();
    }
}

#[test]
fn a_failed_change_does_not_advance_the_version() {
    let (m, _d) = manager(2);
    let err = m
        .apply_ddl(Statement::new("CREATE TABLE (this is not sql"))
        .unwrap_err();
    assert!(!err.to_string().is_empty());
    assert_eq!(
        m.schema_version(ShardId(0)).unwrap(),
        0,
        "a change that did not apply must not claim it did"
    );
}

#[test]
fn cross_shard_reads_are_refused_while_a_change_is_part_applied() {
    // The guard. Rows combined across two schemas are not a slower answer, they are a wrong
    // one, and returning them silently is the failure this project refuses.
    let (m, _d) = manager(3);
    m.apply_ddl(Statement::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT",
    ))
    .unwrap();
    for s in 0..3 {
        m.execute_one(
            ShardId(s),
            Statement::new(format!("INSERT INTO t VALUES ({})", s + 1)),
        )
        .unwrap();
    }
    // Agreed, so a fan-out read works.
    let n = m.query_all_shards("SELECT count(*) FROM t").unwrap();
    assert_eq!(n.rows[0][0], Value::Integer(3));

    // Now half-apply a change, exactly as an interrupted roll would leave things.
    m.writer_schema_for_test(ShardId(0), "ALTER TABLE t ADD COLUMN note TEXT");

    match m.schema_agreement().unwrap() {
        Agreement::Disagreed {
            lowest, highest, ..
        } => assert_eq!((lowest, highest), (1, 2)),
        other => panic!("expected disagreement, got {other:?}"),
    }

    let err = m
        .query_all_shards("SELECT count(*) FROM t")
        .expect_err("a cross-shard read must be refused while schemas disagree");
    let msg = err.to_string();
    assert!(
        msg.contains("part-applied") || msg.contains("disagree"),
        "{msg}"
    );
    assert!(
        msg.contains("shard_"),
        "the message must name a shard: {msg}"
    );
    println!("{msg}");
}

#[test]
fn single_shard_work_continues_while_schemas_disagree() {
    // The reason for rolling rather than pausing the cluster: a part-applied change must not
    // stop shards that are not involved in the reader's question.
    let (m, _d) = manager(3);
    m.apply_ddl(Statement::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT",
    ))
    .unwrap();
    m.writer_schema_for_test(ShardId(0), "ALTER TABLE t ADD COLUMN note TEXT");

    // Cross-shard is refused...
    assert!(m.query_all_shards("SELECT count(*) FROM t").is_err());

    // ...but every shard still serves its own reads and writes.
    for s in 0..3 {
        let shard = ShardId(s);
        m.execute_one(
            shard,
            Statement::new(format!("INSERT INTO t (id) VALUES ({})", 100 + s)),
        )
        .unwrap();
        let n = m
            .query(shard, Statement::new("SELECT count(*) FROM t"))
            .unwrap();
        assert_eq!(scalar(n), Value::Integer(1));
    }
}

#[test]
fn finishing_the_roll_lets_cross_shard_reads_resume() {
    // The window has to close by itself. A guard that latches would turn a transient
    // disagreement into a permanent outage.
    let (m, _d) = manager(3);
    m.apply_ddl(Statement::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT",
    ))
    .unwrap();
    m.writer_schema_for_test(ShardId(0), "ALTER TABLE t ADD COLUMN note TEXT");
    assert!(m.query_all_shards("SELECT count(*) FROM t").is_err());

    // Bring the stragglers up to the same version.
    for s in 1..3 {
        m.writer_schema_for_test(ShardId(s), "ALTER TABLE t ADD COLUMN note TEXT");
    }
    assert_eq!(m.schema_agreement().unwrap(), Agreement::Agreed(2));
    let n = m.query_all_shards("SELECT count(*) FROM t").unwrap();
    assert_eq!(n.rows[0][0], Value::Integer(0));
}

/// A fenced shard must refuse DDL, exactly as it refuses any other write.
///
/// `Promotion::fence_for_transfer` closes a shard's write gate and drains its queue so that
/// `last_lsn` is *final* — that position is what the destination catches up to before it takes
/// over. Anything that commits on the source after the barrier is a change the destination
/// never sees, and the two copies diverge silently.
///
/// DDL is a write. `schema_op` commits pages and advances `user_version` in the header page,
/// and both replicate physically like any other change. So the barrier has to cover it, or a
/// schema change landing in that window is lost by the very transfer that was supposed to
/// carry it.
#[test]
fn a_fenced_shard_refuses_ddl_like_any_other_write() {
    let dir = TempDir::new().unwrap();
    let fence = Arc::new(Fence::new(1, 0));
    let m = ShardManager::open_clustered(
        dir.path(),
        ShardConfig {
            shard_count: 2,
            ..ShardConfig::floor()
        },
        None,
        None,
        Some(Arc::clone(&fence) as Arc<dyn WriteGate>),
    )
    .unwrap();

    // Leading both shards: the ordinary state before any transfer begins.
    fence.open_for(&[ShardId(0), ShardId(1)], 1);
    m.apply_ddl_to(
        ShardId(0),
        Statement::new("CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT"),
    )
    .unwrap();
    assert_eq!(m.schema_version(ShardId(0)).unwrap(), 1);

    // The transfer's fencing barrier: shard 0's gate closes, shard 1 is untouched.
    fence.close_shard(ShardId(0), "catalog transfer reached its fencing barrier");
    let fenced_at = m.schema_version(ShardId(0)).unwrap();

    // Control. This proves the gate is genuinely wired to this manager, so a failure below is
    // about DDL specifically and not about a gate nobody consults.
    let write = m.execute_one(ShardId(0), Statement::new("INSERT INTO t VALUES (1)"));
    assert!(
        write.is_err(),
        "an ordinary write past the fence must be refused, got {write:?}"
    );

    // The property under test.
    let ddl = m.apply_ddl_to(
        ShardId(0),
        Statement::new("ALTER TABLE t ADD COLUMN note TEXT"),
    );
    assert!(
        ddl.is_err(),
        "DDL past the fence must be refused like any other write, got {ddl:?}"
    );
    assert_eq!(
        m.schema_version(ShardId(0)).unwrap(),
        fenced_at,
        "a refused DDL must not advance the schema version"
    );

    // Leadership is per shard, so a shard this node still leads keeps accepting DDL. Without
    // this the test would also pass if the fix closed the gate for everything.
    m.apply_ddl_to(
        ShardId(1),
        Statement::new("CREATE TABLE u (id INTEGER PRIMARY KEY) STRICT"),
    )
    .unwrap();
}

/// A followed shard refuses DDL outright, so schema replay can never reach a new node.
///
/// This is the guarantee the migration plan's reconcile loop depends on. A node that joins and
/// receives a shard by snapshot plus frame catch-up already holds the schema: `user_version`
/// lives in the SQLite header page, so under physical replication it arrives with the data.
/// Replaying the DDL log against such a shard would **double-apply** it.
///
/// The protection is structural rather than a rule the reconcile loop has to remember.
/// `schema_op` begins with `ensure_shard`, which calls `check_may_open` before anything is
/// opened — so a replay against a followed shard dies at connection-open time, before any SQL
/// runs. A future repair loop cannot violate this even if it forgets to check.
#[test]
fn a_followed_shard_refuses_ddl_so_replay_cannot_reach_a_new_node() {
    let (m, _d) = manager(2);
    m.apply_ddl(Statement::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT",
    ))
    .unwrap();
    assert_eq!(m.schema_version(ShardId(0)).unwrap(), 1);

    // Shard 0 now belongs to the replication path — the state every shard on a newly joined
    // node is in until it is promoted.
    m.follow(ShardId(0)).unwrap();

    let replayed = m.apply_ddl_to(
        ShardId(0),
        Statement::new("ALTER TABLE t ADD COLUMN note TEXT"),
    );
    let err = replayed.expect_err("DDL against a followed shard must be refused");
    let msg = err.to_string();
    assert!(
        msg.contains("shard_0"),
        "the refusal must name the shard: {msg}"
    );
    assert!(
        msg.contains("replicat"),
        "the refusal must say why, not merely refuse: {msg}"
    );

    // Nothing was applied. Take the shard back and read the version that survived — had the
    // replay landed, this would be 2 and the shard would carry the column twice over.
    m.lead(ShardId(0));
    assert_eq!(
        m.schema_version(ShardId(0)).unwrap(),
        1,
        "a refused replay must leave the schema exactly as replication left it"
    );

    // Per shard, like every other ownership rule here: a shard still led applies DDL normally.
    m.apply_ddl_to(
        ShardId(1),
        Statement::new("ALTER TABLE t ADD COLUMN note TEXT"),
    )
    .unwrap();
    assert_eq!(m.schema_version(ShardId(1)).unwrap(), 2);
}

#[test]
fn ddl_through_the_routed_path_advances_the_schema_version() {
    // `Request::Run` — the "just connect and run SQL" path the README leads with, and what the
    // CLI and every driver use — reaches DDL through `run_routed`, which fans the statement out
    // as an ordinary write rather than through the versioned schema path.
    //
    // If that leaves `user_version` at 0, every shard agrees at 0 forever and the skew guard is
    // inert for the primary client path: a roll that fails half way through returns an error,
    // leaves half the shards changed, and `schema_agreement` still reports agreement — so the
    // cross-shard read that Defect 2 exists to refuse is served anyway.
    let (m, _d) = manager(4);
    m.run_routed(Statement::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT",
    ))
    .unwrap();

    for s in 0..4 {
        assert_eq!(
            m.schema_version(ShardId(s)).unwrap(),
            1,
            "DDL run through the routed path must advance shard {s}'s schema version, or the \
             agreement guard has nothing to compare"
        );
    }
    assert_eq!(m.schema_agreement().unwrap(), Agreement::Agreed(1));
}
