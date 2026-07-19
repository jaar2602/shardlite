//! Rolling schema changes, and the guard that keeps the half-applied window honest.

use meshdb::shard::{ShardConfig, ShardId, ShardManager};
use meshdb::storage::Value;
use meshdb::storage::exec::{Executed, Outcome, Statement};
use meshdb::storage::schema::Agreement;
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
