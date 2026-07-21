//! Cross-shard query planning.
//!
//! The correctness standard throughout: a fan-out answer must equal the answer the same
//! query gives on a single shard holding all the data. Comparing against a ground truth is
//! the only way to catch a merge that is plausible but wrong — which is the whole failure
//! mode this planner exists to avoid.

use meshdb::query::{Combine, OutputCol, Plan, SetKind, SetTree, plan};
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
fn grouped_having_matches_a_single_shard() {
    let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (sharded, single) = twin(&dirs, 16);

    // HAVING filters complete groups on the coordinator. Compared as multisets against native
    // SQLite. The NULL bucket (n NULL every 17th row → ~24 rows) is smaller than the others
    // (~38), so a `count(*) > 30` threshold is genuinely selective.
    for sql in [
        "SELECT n % 10 AS b, count(*) AS c FROM t GROUP BY n % 10 HAVING count(*) > 30",
        // The HAVING aggregate need not be selected.
        "SELECT n % 10 FROM t GROUP BY n % 10 HAVING count(*) > 30",
        "SELECT n % 10, sum(n) FROM t GROUP BY n % 10 HAVING sum(n) > 15000 AND count(*) > 30",
        "SELECT n % 10, avg(n) FROM t GROUP BY n % 10 HAVING avg(n) >= 450",
        "SELECT n % 7, min(n), max(n) FROM t GROUP BY n % 7 HAVING max(n) > 950 OR min(n) < 20",
        "SELECT n % 10, count(*) FROM t GROUP BY n % 10 HAVING NOT count(*) < 30",
        // HAVING on the grouping column, including its expression form and NULL handling.
        "SELECT n % 10, count(*) FROM t GROUP BY n % 10 HAVING n % 10 IS NOT NULL",
        "SELECT n % 5, count(*) FROM t GROUP BY n % 5 HAVING (n % 5) <> 0",
    ] {
        assert_grouped(&sharded, &single, sql);
    }

    // HAVING composes with ORDER BY / LIMIT (deterministic — the group key is unique per row).
    for sql in [
        "SELECT n % 10 AS b, count(*) AS c FROM t GROUP BY n % 10 HAVING count(*) > 30 ORDER BY b",
        "SELECT n % 10 AS b, sum(n) AS s FROM t GROUP BY n % 10 HAVING sum(n) > 10000 \
         ORDER BY b DESC LIMIT 3",
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
fn bare_aggregates_match_a_single_shard() {
    let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (sharded, single) = twin(&dirs, 16);

    // AVG and multiple aggregates with no GROUP BY — a group over the whole table. Compared as a
    // single row against native SQLite (AVG is exact on integer data).
    for sql in [
        "SELECT avg(n) FROM t",
        "SELECT sum(n), count(n), avg(n) FROM t",
        "SELECT min(n), max(n), count(*) FROM t",
        "SELECT avg(n) FROM t WHERE n > 300",
    ] {
        assert_grouped(&sharded, &single, sql);
    }
}

#[test]
fn scalar_subqueries_match_a_single_shard() {
    let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (sharded, single) = twin(&dirs, 16);

    // The subquery is evaluated globally and its value substituted before the outer fan-out.
    for sql in [
        "SELECT count(*) FROM t WHERE n > (SELECT avg(n) FROM t)",
        "SELECT count(*) FROM t WHERE n >= (SELECT max(n) FROM t)",
        "SELECT sum(n) FROM t WHERE n < (SELECT avg(n) FROM t)",
        // A subquery in the projection.
        "SELECT (SELECT max(n) FROM t) AS mx, (SELECT min(n) FROM t) AS mn FROM t LIMIT 1",
        // Nested — substituted bottom-up.
        "SELECT count(*) FROM t WHERE n > \
         (SELECT avg(n) FROM t WHERE n < (SELECT max(n) FROM t))",
    ] {
        assert_grouped(&sharded, &single, sql);
    }

    // Row results with a deterministic order (n is unique, so `= max` and `> avg` are stable).
    for sql in [
        "SELECT k FROM t WHERE n > (SELECT avg(n) FROM t) ORDER BY k",
        "SELECT k FROM t WHERE n = (SELECT max(n) FROM t) ORDER BY k",
    ] {
        assert_grouped_ordered(&sharded, &single, sql);
    }
}

#[test]
fn in_and_exists_subqueries_match_a_single_shard() {
    let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (sharded, single) = twin(&dirs, 16);

    // IN / EXISTS are evaluated globally and substituted (a value list / a boolean) before the
    // outer fan-out. `n % 7` gives a small candidate set, so the IN list exercises real membership.
    for sql in [
        "SELECT count(*) FROM t WHERE n % 7 IN (SELECT n % 7 FROM t WHERE n > 500)",
        "SELECT count(*) FROM t WHERE n % 7 NOT IN (SELECT n % 7 FROM t WHERE n < 100)",
        // An empty IN result → no rows; NOT IN of empty → all rows.
        "SELECT count(*) FROM t WHERE n IN (SELECT n FROM t WHERE n > 100000)",
        "SELECT count(*) FROM t WHERE n NOT IN (SELECT n FROM t WHERE n > 100000)",
        // Uncorrelated EXISTS / NOT EXISTS — a constant boolean.
        "SELECT count(*) FROM t WHERE EXISTS (SELECT 1 FROM t WHERE n > 900)",
        "SELECT count(*) FROM t WHERE NOT EXISTS (SELECT 1 FROM t WHERE n > 100000)",
    ] {
        assert_grouped(&sharded, &single, sql);
    }

    // IN combined with an ordered row result.
    assert_grouped_ordered(
        &sharded,
        &single,
        "SELECT k FROM t WHERE n IN (SELECT n FROM t WHERE n % 100 = 0 AND n > 0) ORDER BY k",
    );
}

#[test]
fn a_correlated_subquery_is_refused() {
    // A correlated subquery references the outer row; evaluated on its own it names a column that
    // is not in scope, which surfaces as an error rather than a wrong answer.
    let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (sharded, _single) = twin(&dirs, 8);
    let err = sharded
        .query_all_shards(
            "SELECT a.k FROM t a WHERE a.n > (SELECT avg(b.n) FROM t b WHERE b.k = a.k)",
        )
        .expect_err("a correlated subquery must not be answered");
    assert!(!err.to_string().is_empty(), "{err}");
}

/// Two co-partitioned tables: `customers` routed by `cid`, `orders` routed by `cust`. A customer
/// and all its orders therefore land on the same shard, so a join on `orders.cust = customers.cid`
/// is co-located. Returns a sharded manager (with the co-partitioning declared) and a single-shard
/// ground truth holding identical data.
fn join_twin(dirs: &(TempDir, TempDir), shards: u32) -> (ShardManager, ShardManager) {
    let sharded = ShardManager::open(dirs.0.path(), cfg(shards)).unwrap();
    let single = ShardManager::open(dirs.1.path(), cfg(1)).unwrap();

    for m in [&sharded, &single] {
        m.execute_all_shards("CREATE TABLE customers (cid TEXT PRIMARY KEY, region TEXT) STRICT")
            .unwrap();
        m.execute_all_shards(
            "CREATE TABLE orders (oid TEXT PRIMARY KEY, cust TEXT, amt INTEGER) STRICT",
        )
        .unwrap();
        m.declare_shard_key("customers", "cid").unwrap();
        m.declare_shard_key("orders", "cust").unwrap();
    }

    // 25 customers; orders exist only for c0..c19, so c20..c24 are unmatched (for LEFT JOIN).
    for i in 0..25 {
        let cid = format!("c{i}");
        let stmt = Statement::with_params(
            "INSERT INTO customers VALUES (?1, ?2)",
            vec![Value::Text(cid.clone()), Value::Text(format!("r{}", i % 4))],
        );
        sharded
            .execute_one(sharded.route(cid.as_bytes()), stmt.clone())
            .unwrap();
        single.execute_one(ShardId(0), stmt).unwrap();
    }
    for i in 0..100 {
        let cust = format!("c{}", i % 20);
        let stmt = Statement::with_params(
            "INSERT INTO orders VALUES (?1, ?2, ?3)",
            vec![
                Value::Text(format!("o{i}")),
                Value::Text(cust.clone()),
                Value::Integer((i * 13 % 100) as i64),
            ],
        );
        // Orders route by `cust` — the customer's shard key — so they co-locate with the customer.
        sharded
            .execute_one(sharded.route(cust.as_bytes()), stmt.clone())
            .unwrap();
        single.execute_one(ShardId(0), stmt).unwrap();
    }
    (sharded, single)
}

#[test]
fn colocated_joins_match_a_single_shard() {
    let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (sharded, single) = join_twin(&dirs, 16);

    // Each shard joins its local rows; because the tables are co-partitioned every match is local,
    // so the concatenation equals the single-shard join. Compared against native SQLite.
    for sql in [
        // Plain inner join.
        "SELECT o.oid, c.region FROM orders o JOIN customers c ON o.cust = c.cid",
        // Join + WHERE + aggregate.
        "SELECT count(*) FROM orders o JOIN customers c ON o.cust = c.cid WHERE o.amt > 50",
        // Join + GROUP BY on a non-shard-key column (re-aggregated across shards).
        "SELECT c.region, count(*), sum(o.amt) FROM orders o JOIN customers c \
         ON o.cust = c.cid GROUP BY c.region",
        // Join + extra ON condition (pushed down, not needed for co-location).
        "SELECT count(*) FROM orders o JOIN customers c ON o.cust = c.cid AND o.amt > 30",
        // LEFT JOIN: customers c20..c24 have no orders and appear with a zero count.
        "SELECT c.cid, count(o.oid) FROM customers c LEFT JOIN orders o ON o.cust = c.cid \
         GROUP BY c.cid",
    ] {
        assert_grouped(&sharded, &single, sql);
    }

    // Ordered join result (deterministic — grouped by the unique region key).
    assert_grouped_ordered(
        &sharded,
        &single,
        "SELECT c.region, sum(o.amt) AS total FROM orders o JOIN customers c \
         ON o.cust = c.cid GROUP BY c.region ORDER BY c.region",
    );
}

#[test]
fn colocated_join_answer_is_invariant_to_shard_count() {
    // The same join over the same co-partitioned data must give the same answer however the data
    // is spread — the strongest statement that the join genuinely spans shards correctly.
    let sql = "SELECT c.region, count(*), sum(o.amt) FROM orders o JOIN customers c \
               ON o.cust = c.cid GROUP BY c.region";
    let mut baseline: Option<Vec<Vec<Value>>> = None;
    for shards in [1u32, 4, 16, 64] {
        let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
        let (sharded, _single) = join_twin(&dirs, shards);
        let answer = sorted(sharded.query_all_shards(sql).unwrap().rows);
        match &baseline {
            None => baseline = Some(answer),
            Some(b) => assert_eq!(&answer, b, "join answer changed at {shards} shards"),
        }
    }
}

#[test]
fn non_colocated_joins_are_refused() {
    let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (sharded, _single) = join_twin(&dirs, 8);

    for (sql, expect) in [
        // Join on a non-shard-key column (orders is routed by cust, not oid).
        (
            "SELECT o.oid FROM orders o JOIN customers c ON o.oid = c.cid",
            "shard keys",
        ),
        // Cross join (comma-separated tables).
        ("SELECT o.oid FROM orders o, customers c", "cross join"),
        // A second join whose ON is not on shard keys.
        (
            "SELECT o.oid FROM orders o JOIN customers c ON o.cust = c.cid \
             JOIN orders o2 ON o2.oid = c.cid",
            "shard keys",
        ),
        // NATURAL join has no explicit ON equality.
        (
            "SELECT o.oid FROM orders o NATURAL JOIN customers c",
            "NATURAL",
        ),
    ] {
        let err = sharded
            .query_all_shards(sql)
            .expect_err(&format!("{sql} should be refused"));
        assert!(
            err.to_string().contains(expect),
            "refusal for `{sql}` should mention `{expect}`, got: {err}"
        );
    }
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
fn distinct_matches_a_single_shard() {
    let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (sharded, single) = twin(&dirs, 16);

    // Unordered DISTINCT — compared as a multiset against native SQLite.
    for sql in [
        "SELECT DISTINCT n FROM t",
        "SELECT DISTINCT n % 5 FROM t",
        "SELECT DISTINCT n, s FROM t WHERE n IS NOT NULL",
        "SELECT DISTINCT s FROM t",
    ] {
        assert_grouped(&sharded, &single, sql);
    }

    // Ordered DISTINCT with LIMIT/OFFSET — distinct values are unique, so the order is total and
    // the row-for-row comparison against native SQLite is sound.
    for sql in [
        "SELECT DISTINCT n FROM t WHERE n IS NOT NULL ORDER BY n LIMIT 10",
        "SELECT DISTINCT n FROM t WHERE n IS NOT NULL ORDER BY n DESC LIMIT 5 OFFSET 3",
    ] {
        assert_grouped_ordered(&sharded, &single, sql);
    }
}

#[test]
fn union_matches_a_single_shard() {
    let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (sharded, single) = twin(&dirs, 16);

    // Multiset comparison against native SQLite. `n % 7` is low-cardinality, so the same value
    // recurs on many shards — exercising UNION's cross-shard dedup (a per-shard union alone would
    // leave duplicates) and UNION ALL's duplicate-keeping, including across the branch boundary.
    for sql in [
        "SELECT n % 7 FROM t WHERE n < 500 UNION SELECT n % 7 FROM t WHERE n > 200",
        "SELECT n % 7 FROM t WHERE n < 500 UNION ALL SELECT n % 7 FROM t WHERE n > 200",
        // `k` is a unique primary key: the no-cross-shard-duplicate path.
        "SELECT k FROM t WHERE n < 300 UNION SELECT k FROM t WHERE n > 100",
        // Three branches, all distinct UNION, over a low-cardinality key.
        "SELECT n % 10 FROM t WHERE n < 200 UNION SELECT n % 10 FROM t WHERE n >= 200 AND n < 400 \
         UNION SELECT n % 10 FROM t WHERE n >= 400",
    ] {
        assert_grouped(&sharded, &single, sql);
    }
}

#[test]
fn offset_matches_a_single_shard() {
    let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (sharded, single) = twin(&dirs, 16);

    // `k` is a unique primary key, so ORDER BY k is a total order and OFFSET is deterministic.
    for sql in [
        "SELECT k FROM t ORDER BY k LIMIT 10 OFFSET 5",
        "SELECT k FROM t ORDER BY k DESC LIMIT 20 OFFSET 30",
        "SELECT k, n FROM t ORDER BY k LIMIT 15 OFFSET 100",
        // OFFSET past most rows — the tail of the table.
        "SELECT k FROM t ORDER BY k LIMIT 5 OFFSET 395",
    ] {
        assert_grouped_ordered(&sharded, &single, sql);
    }
}

#[test]
fn set_operations_match_a_single_shard() {
    let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (sharded, single) = twin(&dirs, 16);

    // Each branch is fanned out independently and the results combined; compared as multisets
    // against native SQLite. Low-cardinality keys (`n % 7`, `n % 5`) make the same value recur on
    // many shards, so cross-shard set semantics are actually exercised.
    for sql in [
        // INTERSECT / EXCEPT — the pushdown-impossible cases, now correct via central combine.
        "SELECT n % 7 FROM t WHERE n < 500 INTERSECT SELECT n % 7 FROM t WHERE n > 200",
        "SELECT n % 7 FROM t WHERE n IS NOT NULL EXCEPT SELECT n % 7 FROM t WHERE n > 500",
        // Mixed operators in one query (left-associative, like SQLite).
        "SELECT n % 5 FROM t WHERE n < 300 UNION SELECT n % 5 FROM t WHERE n >= 300 \
         EXCEPT SELECT n % 5 FROM t WHERE n = 0",
        // An aggregate in each branch — computed globally before combining.
        "SELECT count(*) FROM t WHERE n < 500 UNION SELECT count(*) FROM t WHERE n >= 500",
        "SELECT max(n) FROM t WHERE n < 500 UNION ALL SELECT max(n) FROM t WHERE n >= 500",
        // A grouped query as a branch.
        "SELECT count(*) FROM t GROUP BY n % 3 UNION SELECT count(*) FROM t GROUP BY n % 4",
    ] {
        assert_grouped(&sharded, &single, sql);
    }

    // Ordered set operations resolve to a total order (unique values of n % 7), so a row-for-row
    // comparison against native SQLite is sound.
    for sql in [
        "SELECT n % 7 FROM t WHERE n < 500 INTERSECT SELECT n % 7 FROM t WHERE n > 200 ORDER BY 1",
        "SELECT n % 7 FROM t WHERE n IS NOT NULL EXCEPT SELECT n % 7 FROM t WHERE n > 500 \
         ORDER BY 1 DESC",
    ] {
        assert_grouped_ordered(&sharded, &single, sql);
    }
}

#[test]
fn unordered_offset_returns_the_right_count() {
    // OFFSET without ORDER BY is allowed — the rows are unspecified (as for a bare LIMIT), so the
    // contract is the row *count*, not which rows.
    let dirs = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (sharded, _single) = twin(&dirs, 16);

    let page = sharded
        .query_all_shards("SELECT k FROM t LIMIT 10 OFFSET 5")
        .unwrap();
    assert_eq!(page.rows.len(), 10);

    // OFFSET past most of the table returns the remainder.
    let tail = sharded
        .query_all_shards("SELECT k FROM t LIMIT 100 OFFSET 395")
        .unwrap();
    assert_eq!(tail.rows.len() as i64, ROWS - 395);
}

#[test]
fn a_runaway_grouping_is_capped_not_ooming() {
    // A high-cardinality grouping — grouping by a near-unique column — would materialise a group
    // per row on the coordinator. A configurable cap turns that into a loud refusal instead of an
    // OOM, while a low-cardinality grouping under the cap still answers.
    let dir = TempDir::new().unwrap();
    let m = ShardManager::open(
        dir.path(),
        ShardConfig {
            max_grouped_rows: 25,
            ..cfg(8)
        },
    )
    .unwrap();
    m.execute_all_shards("CREATE TABLE t (k TEXT PRIMARY KEY, n INTEGER) STRICT")
        .unwrap();
    for i in 0..50 {
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

    // 50 distinct values of n → 50 partial group rows, over the cap of 25.
    let err = m
        .query_all_shards("SELECT n, count(*) FROM t GROUP BY n")
        .expect_err("a 50-group query must be capped at 25");
    assert!(err.to_string().contains("partial group rows"), "{err}");

    // Grouping into two buckets stays well under the cap and still answers.
    let ok = m
        .query_all_shards("SELECT n % 2, count(*) FROM t GROUP BY n % 2")
        .unwrap();
    assert_eq!(ok.rows.len(), 2);
}

#[test]
fn queries_that_cannot_be_combined_are_refused() {
    // Each of these has a plausible-looking wrong answer available, which is exactly why it
    // must error instead.
    for (sql, expect) in [
        ("SELECT a.k FROM t a JOIN t b ON a.k = b.k", "join"),
        (
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "common table expression",
        ),
        ("SELECT group_concat(k) FROM t", "GROUP_CONCAT"),
        // An aggregate alongside a bare (non-grouped) column is still nondeterministic per shard.
        (
            "SELECT count(*), n FROM t",
            "neither grouped nor aggregated",
        ),
        // A derived table (subquery in FROM) needs coordinator-side materialisation.
        (
            "SELECT count(*) FROM (SELECT n FROM t WHERE n > 20)",
            "FROM",
        ),
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
        // A HAVING operand that is neither a grouping column nor an aggregate.
        (
            "SELECT n % 10, count(*) FROM t GROUP BY n % 10 HAVING s > 5",
            "neither grouped nor aggregated",
        ),
        // GROUP_CONCAT within a group has undefined cross-shard order.
        (
            "SELECT n % 10, group_concat(s) FROM t GROUP BY n % 10",
            "GROUP_CONCAT",
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
        .query_all_shards("SELECT group_concat(k) FROM t")
        .expect_err("GROUP_CONCAT must not be answered across shards");
    assert!(err.to_string().contains("GROUP_CONCAT"), "{err}");

    // Aggregates that decompose — including AVG — do work.
    sharded.query_all_shards("SELECT sum(n) FROM t").unwrap();
    sharded.query_all_shards("SELECT avg(n) FROM t").unwrap();
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

    // DISTINCT → a post-process that dedups; the shard query keeps DISTINCT.
    match plan("SELECT DISTINCT n FROM t").unwrap() {
        Plan::PostProcess(p) => {
            assert!(p.distinct);
            assert_eq!(p.offset, 0);
            assert!(p.order_by.is_empty());
            assert!(p.shard_sql.to_uppercase().contains("DISTINCT"));
        }
        other => panic!("expected a post-process plan, got {other:?}"),
    }

    // OFFSET → the shard LIMIT is widened to offset+limit and the OFFSET is stripped.
    match plan("SELECT k FROM t ORDER BY k LIMIT 10 OFFSET 5").unwrap() {
        Plan::PostProcess(p) => {
            assert!(!p.distinct);
            assert_eq!(p.offset, 5);
            assert_eq!(p.limit, Some(10));
            assert_eq!(p.order_by.len(), 1);
            assert!(
                p.shard_sql.contains("LIMIT 15"),
                "shard sql: {}",
                p.shard_sql
            );
            assert!(!p.shard_sql.to_uppercase().contains("OFFSET"));
        }
        other => panic!("expected a post-process plan, got {other:?}"),
    }

    // UNION ALL → a set-op plan: two branches combined by a keep-duplicates UNION.
    match plan("SELECT k FROM t UNION ALL SELECT k FROM t").unwrap() {
        Plan::SetOp(s) => {
            assert_eq!(s.branches.len(), 2);
            assert!(matches!(
                s.tree,
                SetTree::Op {
                    op: SetKind::Union,
                    all: true,
                    ..
                }
            ));
        }
        other => panic!("expected a set-op plan, got {other:?}"),
    }

    // INTERSECT → a set-op plan with a distinct intersect.
    match plan("SELECT k FROM t WHERE n < 5 INTERSECT SELECT k FROM t WHERE n > 2").unwrap() {
        Plan::SetOp(s) => {
            assert_eq!(s.branches.len(), 2);
            assert!(matches!(
                s.tree,
                SetTree::Op {
                    op: SetKind::Intersect,
                    all: false,
                    ..
                }
            ));
        }
        other => panic!("expected a set-op plan, got {other:?}"),
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
