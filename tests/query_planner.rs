//! Cross-shard query planning.
//!
//! The correctness standard throughout: a fan-out answer must equal the answer the same
//! query gives on a single shard holding all the data. Comparing against a ground truth is
//! the only way to catch a merge that is plausible but wrong — which is the whole failure
//! mode this planner exists to avoid.

use meshdb::query::{Combine, OutputCol, Plan, plan};
use meshdb::shard::{ShardConfig, ShardId, ShardManager};
use meshdb::storage::Value;
use meshdb::storage::exec::{Executed, Outcome, QueryResult, Statement};
use tempfile::TempDir;

const ROWS: i64 = 400;

fn cfg(shards: u32) -> ShardConfig {
    ShardConfig {
        shard_count: shards,
        writer_threads: 2,
        reader_threads: 2,
        ..ShardConfig::floor()
    }
}

/// Build two databases holding identical data: one sharded, one single-shard.
///
/// The single-shard copy is the ground truth — whatever SQLite says there is the answer the
/// fan-out must reproduce.
fn twin(dirs: &(TempDir, TempDir), shards: u32) -> (ShardManager, ShardManager) {
    let sharded = ShardManager::open(dirs.0.path(), cfg(shards)).unwrap();
    let single = ShardManager::open(dirs.1.path(), cfg(1)).unwrap();

    let ddl = "CREATE TABLE t (k TEXT PRIMARY KEY, n INTEGER, s TEXT) STRICT";
    sharded.execute_all_shards(ddl).unwrap();
    single.execute_all_shards(ddl).unwrap();

    for i in 0..ROWS {
        let key = format!("key-{i}");
        // A spread of values, with some NULLs so ordering and aggregation are exercised.
        let n = if i % 17 == 0 {
            Value::Null
        } else {
            Value::Integer((i * 7919) % 1000)
        };
        let stmt = Statement::with_params(
            "INSERT INTO t VALUES (?1, ?2, ?3)",
            vec![
                Value::Text(key.clone()),
                n,
                Value::Text(format!("s{:04}", (i * 31) % 500)),
            ],
        );
        sharded
            .execute_one(sharded.route(key.as_bytes()), stmt.clone())
            .unwrap();
        single.execute_one(ShardId(0), stmt).unwrap();
    }
    (sharded, single)
}

fn rows_of(r: &QueryResult) -> Vec<Vec<Value>> {
    r.rows.clone()
}

/// The fan-out answer must equal the single-shard answer.
fn assert_matches_ground_truth(sharded: &ShardManager, single: &ShardManager, sql: &str) {
    let got = sharded.query_all_shards(sql).unwrap();
    let want = single.query_all_shards(sql).unwrap();
    assert_eq!(
        rows_of(&got),
        rows_of(&want),
        "fan-out disagreed with the single-shard answer for: {sql}"
    );
}

/// SQLite's *native* answer, run directly on one shard holding all the data — an oracle
/// independent of the fan-out merge. This matters for grouped queries: `single.query_all_shards`
/// would itself route through the two-phase merge, so it could not catch a bug in that merge.
/// Running the query directly on SQLite can.
fn native(single: &ShardManager, sql: &str) -> Vec<Vec<Value>> {
    match single.query(ShardId(0), sql).unwrap() {
        Outcome::Ok(Executed::Rows(r)) => r.rows,
        other => panic!("native query did not return rows for `{sql}`: {other:?}"),
    }
}

/// Sort rows by a stable, total key so two result sets can be compared as multisets — for a
/// `GROUP BY` with no `ORDER BY`, the row order is unspecified, but the *set* of grouped rows
/// must match SQLite exactly.
fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.sort_by_key(|r| format!("{r:?}"));
    rows
}

/// A grouped fan-out must produce the same *set* of grouped rows as native SQLite.
fn assert_grouped(sharded: &ShardManager, single: &ShardManager, sql: &str) {
    let got = sharded.query_all_shards(sql).unwrap().rows;
    let want = native(single, sql);
    assert_eq!(
        sorted(got),
        sorted(want),
        "grouped fan-out disagreed with native SQLite for: {sql}"
    );
}

/// A grouped fan-out with a deterministic total ordering must match native SQLite row for row,
/// including after `LIMIT`.
fn assert_grouped_ordered(sharded: &ShardManager, single: &ShardManager, sql: &str) {
    let got = sharded.query_all_shards(sql).unwrap().rows;
    let want = native(single, sql);
    assert_eq!(
        got, want,
        "ordered grouped fan-out disagreed with native SQLite for: {sql}"
    );
}

#[test]
fn grouped_queries_match_a_single_shard() {
    let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (sharded, single) = twin(&dirs, 16);

    // Compared as multisets against native SQLite — the grouped values must be exact, regardless
    // of row order. `n % 10` yields low-cardinality buckets including a NULL bucket (n is NULL
    // every 17th row), so NULL grouping and every decomposable aggregate are exercised.
    for sql in [
        "SELECT n % 10 AS b, count(*) FROM t GROUP BY n % 10",
        "SELECT n % 10, count(*), count(n), sum(n), min(n), max(n) FROM t GROUP BY n % 10",
        "SELECT n % 10, avg(n) FROM t GROUP BY n % 10",
        "SELECT n % 10, n % 3, count(*) FROM t GROUP BY n % 10, n % 3",
        // The group key need not be selected.
        "SELECT count(*) FROM t GROUP BY n % 10",
        // With a WHERE that is pushed down.
        "SELECT n % 10, count(*), avg(n) FROM t WHERE n > 300 GROUP BY n % 10",
        // A text group key, and text MIN/MAX per group.
        "SELECT substr(s, 1, 3) AS p, count(*), min(s), max(s) FROM t GROUP BY substr(s, 1, 3)",
        // GROUP BY with no aggregate — a DISTINCT of the group key.
        "SELECT n FROM t GROUP BY n",
        // High-cardinality text key.
        "SELECT s, count(*) FROM t GROUP BY s",
    ] {
        assert_grouped(&sharded, &single, sql);
    }
}

#[test]
fn grouped_ordering_and_limit_match_a_single_shard() {
    let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (sharded, single) = twin(&dirs, 16);

    // Each ORDER BY resolves to a total order (the group key is unique per row), so LIMIT is
    // deterministic and a row-for-row comparison against native SQLite is sound.
    for sql in [
        "SELECT n % 10 AS b, count(*) AS c FROM t GROUP BY n % 10 ORDER BY b",
        "SELECT n % 10 AS b, count(*) AS c FROM t GROUP BY n % 10 ORDER BY b DESC LIMIT 5",
        // Order by an aggregate, with the unique group key as a tiebreak → still total.
        "SELECT n % 10 AS b, count(*) AS c FROM t GROUP BY n % 10 ORDER BY c DESC, b LIMIT 4",
        // Ordinal reference to the aggregate column.
        "SELECT n % 10 AS b, sum(n) AS tot FROM t GROUP BY n % 10 ORDER BY 1",
    ] {
        assert_grouped_ordered(&sharded, &single, sql);
    }
}

#[test]
fn grouped_answers_are_invariant_to_shard_count() {
    // The merge iterates groups in key order, so a grouped answer is deterministic regardless of
    // how the data is spread. Removing the cross-shard re-aggregation would make the same query
    // return duplicate per-shard group rows — so this is non-vacuous.
    let queries = [
        "SELECT n % 10, count(*), sum(n), min(n), max(n) FROM t GROUP BY n % 10",
        "SELECT n % 10, avg(n) FROM t GROUP BY n % 10",
        "SELECT n % 10 AS b, count(*) AS c FROM t GROUP BY n % 10 ORDER BY b",
    ];

    let mut baseline: Option<Vec<Vec<Vec<Value>>>> = None;
    for shards in [1u32, 4, 16, 64] {
        let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
        let (sharded, _single) = twin(&dirs, shards);
        let answers: Vec<Vec<Vec<Value>>> = queries
            .iter()
            .map(|q| sorted(sharded.query_all_shards(q).unwrap().rows))
            .collect();
        match &baseline {
            None => baseline = Some(answers),
            Some(b) => assert_eq!(&answers, b, "grouped answer changed at {shards} shards"),
        }
    }
}

#[test]
fn aggregates_match_a_single_shard() {
    let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (sharded, single) = twin(&dirs, 16);

    for sql in [
        "SELECT count(*) FROM t",
        "SELECT count(n) FROM t",
        "SELECT sum(n) FROM t",
        "SELECT min(n) FROM t",
        "SELECT max(n) FROM t",
        "SELECT min(s) FROM t",
        "SELECT max(s) FROM t",
        "SELECT count(*) FROM t WHERE n > 500",
        "SELECT sum(n) FROM t WHERE n IS NOT NULL",
    ] {
        assert_matches_ground_truth(&sharded, &single, sql);
    }

    // And the count really is the whole table, not one shard's worth.
    let n = match sharded
        .query_all_shards("SELECT count(*) FROM t")
        .unwrap()
        .rows[0][0]
    {
        Value::Integer(i) => i,
        ref v => panic!("expected an integer, got {v:?}"),
    };
    assert_eq!(n, ROWS);
}

#[test]
fn ordered_queries_match_a_single_shard() {
    let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (sharded, single) = twin(&dirs, 16);

    for sql in [
        "SELECT k FROM t ORDER BY k",
        "SELECT k FROM t ORDER BY k DESC",
        "SELECT k FROM t ORDER BY k LIMIT 10",
        "SELECT k FROM t ORDER BY k DESC LIMIT 25",
        // NULLs are present in `n`, so this checks NULL ordering in both directions.
        "SELECT n, k FROM t ORDER BY n, k",
        "SELECT n, k FROM t ORDER BY n DESC, k LIMIT 40",
        "SELECT s, k FROM t ORDER BY s, k LIMIT 50",
        // Ordinal reference.
        "SELECT k, n FROM t ORDER BY 1 LIMIT 20",
    ] {
        assert_matches_ground_truth(&sharded, &single, sql);
    }
}

#[test]
fn plain_selects_return_every_row() {
    let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (sharded, _single) = twin(&dirs, 16);

    let all = sharded.query_all_shards("SELECT k FROM t").unwrap();
    assert_eq!(all.rows.len() as i64, ROWS);

    let filtered = sharded
        .query_all_shards("SELECT k FROM t WHERE n IS NULL")
        .unwrap();
    // Every 17th row of 400.
    assert_eq!(
        filtered.rows.len(),
        (0..ROWS).filter(|i| i % 17 == 0).count()
    );

    // LIMIT without ORDER BY returns some n rows, which is what SQLite promises too.
    let limited = sharded.query_all_shards("SELECT k FROM t LIMIT 7").unwrap();
    assert_eq!(limited.rows.len(), 7);
}

#[test]
fn the_shard_count_does_not_change_the_answer() {
    // The strongest statement of correctness: sharding is an implementation detail of
    // storage, so the same data must give the same answer at any shard count.
    let queries = [
        "SELECT count(*) FROM t",
        "SELECT sum(n) FROM t",
        "SELECT min(n) FROM t",
        "SELECT max(n) FROM t",
        "SELECT k FROM t ORDER BY k LIMIT 30",
        "SELECT n, k FROM t ORDER BY n DESC, k LIMIT 30",
    ];

    let mut baseline: Option<Vec<Vec<Vec<Value>>>> = None;
    for shards in [1u32, 4, 16, 64] {
        let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
        let (sharded, _single) = twin(&dirs, shards);
        let answers: Vec<Vec<Vec<Value>>> = queries
            .iter()
            .map(|q| rows_of(&sharded.query_all_shards(q).unwrap()))
            .collect();

        match &baseline {
            None => baseline = Some(answers),
            Some(b) => assert_eq!(
                &answers, b,
                "answers changed at {shards} shards; sharding must not be observable"
            ),
        }
    }
}

#[test]
fn queries_that_cannot_be_combined_are_refused() {
    // Each of these has a plausible-looking wrong answer available, which is exactly why it
    // must error instead.
    for (sql, expect) in [
        ("SELECT avg(n) FROM t", "AVG"),
        ("SELECT DISTINCT n FROM t", "DISTINCT"),
        ("SELECT a.k FROM t a JOIN t b ON a.k = b.k", "join"),
        ("SELECT k FROM t LIMIT 10 OFFSET 5", "OFFSET"),
        ("SELECT k FROM t UNION SELECT k FROM t", "UNION"),
        (
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "common table expression",
        ),
        ("SELECT count(*), sum(n) FROM t", "alongside other columns"),
        ("SELECT group_concat(k) FROM t", "GROUP_CONCAT"),
        // --- grouped queries that still cannot be answered correctly across shards ---
        // A bare column that is neither grouped nor aggregated is an arbitrary row per shard.
        (
            "SELECT n, k FROM t GROUP BY n",
            "neither grouped nor aggregated",
        ),
        // DISTINCT inside an aggregate double-counts across shards.
        (
            "SELECT n % 10, count(DISTINCT n) FROM t GROUP BY n % 10",
            "DISTINCT",
        ),
        // HAVING filters groups that are only complete after the merge (a later increment).
        (
            "SELECT n % 10, count(*) FROM t GROUP BY n % 10 HAVING count(*) > 5",
            "HAVING",
        ),
        // GROUP_CONCAT within a group has undefined cross-shard order.
        (
            "SELECT n % 10, group_concat(s) FROM t GROUP BY n % 10",
            "GROUP_CONCAT",
        ),
        // A subquery would run per shard, not over the whole table — a silent wrong answer.
        (
            "SELECT n % 10, count(*) FROM t WHERE n > (SELECT avg(n) FROM t) GROUP BY n % 10",
            "subquery",
        ),
    ] {
        let err = plan(sql).expect_err(&format!("{sql} should have been refused"));
        let msg = err.to_string();
        assert!(
            msg.contains(expect),
            "refusal for `{sql}` should mention `{expect}`, got: {msg}"
        );
        // Every refusal must explain itself, not merely say no.
        assert!(
            msg.len() > 60,
            "refusal for `{sql}` is too terse to act on: {msg}"
        );
    }
}

#[test]
fn refusals_reach_the_caller_rather_than_a_wrong_answer() {
    let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (sharded, _single) = twin(&dirs, 8);

    let err = sharded
        .query_all_shards("SELECT avg(n) FROM t")
        .expect_err("AVG must not be answered across shards");
    assert!(err.to_string().contains("AVG"), "{err}");

    // The suggested replacement does work.
    sharded.query_all_shards("SELECT sum(n) FROM t").unwrap();
    sharded.query_all_shards("SELECT count(n) FROM t").unwrap();
}

#[test]
fn plans_are_what_they_claim_to_be() {
    assert_eq!(
        plan("SELECT count(*) FROM t").unwrap(),
        Plan::Aggregate {
            combine: Combine::Sum
        }
    );
    assert_eq!(
        plan("SELECT min(n) FROM t").unwrap(),
        Plan::Aggregate {
            combine: Combine::Min
        }
    );
    assert_eq!(
        plan("SELECT k FROM t").unwrap(),
        Plan::Concat { limit: None }
    );
    assert_eq!(
        plan("SELECT k FROM t LIMIT 5").unwrap(),
        Plan::Concat { limit: Some(5) }
    );

    match plan("SELECT k, n FROM t ORDER BY n DESC LIMIT 3").unwrap() {
        Plan::Merge { keys, limit } => {
            assert_eq!(limit, Some(3));
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0].column, 1);
            assert!(keys[0].descending);
            // SQLite puts NULLs last when descending.
            assert!(!keys[0].nulls_first);
        }
        other => panic!("expected a merge plan, got {other:?}"),
    }

    // A grouped query decomposes into a per-shard partial query plus a fold recipe.
    match plan("SELECT n % 10 AS b, count(*), avg(n) FROM t GROUP BY n % 10 LIMIT 4").unwrap() {
        Plan::Grouped(g) => {
            assert_eq!(g.group_cols, vec![0]);
            assert_eq!(g.limit, Some(4));
            // b -> the group key; count(*) -> a summed partial; avg -> a sum/count pair.
            assert!(matches!(g.outputs[0], OutputCol::Group(0)));
            assert!(matches!(
                g.outputs[1],
                OutputCol::Combine {
                    kind: Combine::Sum,
                    col: 1
                }
            ));
            assert!(matches!(
                g.outputs[2],
                OutputCol::Avg {
                    sum_col: 2,
                    count_col: 3
                }
            ));
            // The per-shard query groups locally and carries AVG as SUM + COUNT.
            let lower = g.shard_sql.to_lowercase();
            assert!(lower.contains("group by"), "shard sql: {}", g.shard_sql);
            assert!(lower.contains("sum(n)") && lower.contains("count(n)"));
            // LIMIT is coordinator-only — it must not be pushed to the shards.
            assert!(
                !lower.contains("limit"),
                "shard sql leaked LIMIT: {}",
                g.shard_sql
            );
        }
        other => panic!("expected a grouped plan, got {other:?}"),
    }
}

#[test]
fn ordering_by_a_column_that_is_not_selected_is_refused() {
    // A merge can only sort by values present in the rows it receives.
    let err = plan("SELECT k FROM t ORDER BY n").expect_err("should be refused");
    assert!(err.to_string().contains("not in the select list"), "{err}");
}

#[test]
fn an_empty_shard_contributes_nothing() {
    // Shards are created lazily, so most of a 64-shard cluster has no table at all until
    // written to. That must not fail the query or inject empty rows.
    let dir = TempDir::new().unwrap();
    let m = ShardManager::open(dir.path(), cfg(64)).unwrap();
    m.execute_all_shards("CREATE TABLE t (k TEXT PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();

    for i in 0..5 {
        let key = format!("k{i}");
        m.execute_one(
            m.route(key.as_bytes()),
            Statement::with_params(
                "INSERT INTO t VALUES (?1, ?2)",
                vec![Value::Text(key.clone()), Value::Integer(i)],
            ),
        )
        .unwrap();
    }

    let count = m.query_all_shards("SELECT count(*) FROM t").unwrap();
    assert_eq!(count.rows[0][0], Value::Integer(5));

    let sum = m.query_all_shards("SELECT sum(n) FROM t").unwrap();
    assert_eq!(sum.rows[0][0], Value::Integer(10));

    let rows = m.query_all_shards("SELECT k FROM t").unwrap();
    assert_eq!(rows.rows.len(), 5, "empty shards must not add rows");
}
