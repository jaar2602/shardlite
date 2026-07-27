//! Shard-count alignment: one shard and many shards must mean the same thing.
//!
//! The contract, from [`docs/shard-alignment-plan.md`]:
//!
//! > A statement accepted at one shard returns the same answer at any shard count, or is refused
//! > at every shard count.
//!
//! Every statement in the corpus below is executed at several shard counts against identical
//! logical data, and the results must be byte-identical — including identical refusals. A refusal
//! at every count is a *pass*; a refusal at one count and an answer at another is a failure.
//!
//! # Why the seed data looks like this
//!
//! `grp = n % 10` and five repeating names are deliberate. With unique values per row a wrong
//! cross-shard combine still produces the right answer by accident, and the suite passes
//! vacuously. Low cardinality is what makes a broken GROUP BY or a broken merge observable.
//!
//! # Why N=3
//!
//! Three is not a power of two, so it exercises the linear-hashing split pointer rather than the
//! plain modulo path. It is also what exposes *data-dependent* divergence: a constraint can hold
//! at N=1 and N=3 and fail at N=4 purely because of where two values hash.

use shardlite::shard::classes::TableClass;
use shardlite::shard::{ShardConfig, ShardId, ShardManager};
use shardlite::storage::exec::{Executed, Outcome};
use tempfile::TempDir;

/// Shard counts every statement is checked against. 1 is the reference; 3 is non-power-of-two;
/// 4 and 8 spread the same 40 rows more thinly, so a per-shard bug has more shards to show up on.
const COUNTS: [u32; 4] = [1, 3, 4, 8];

struct Fixture {
    _dir: TempDir,
    manager: ShardManager,
}

fn open(shard_count: u32) -> Fixture {
    let dir = TempDir::new().expect("temp dir");
    let manager = ShardManager::open(
        dir.path(),
        ShardConfig {
            shard_count,
            ..ShardConfig::floor()
        },
    )
    .expect("open shard manager");
    Fixture { _dir: dir, manager }
}

/// Identical logical data at any shard count.
fn seed(m: &ShardManager) {
    m.run_routed("CREATE TABLE t (id INTEGER PRIMARY KEY, grp INTEGER, name TEXT, amt INTEGER)")
        .expect("create t");
    m.run_routed("CREATE TABLE r (grp INTEGER PRIMARY KEY, label TEXT)")
        .expect("create r");
    for n in 1..=40i64 {
        let name = ["ada", "bob", "cy", "dee", "eve"][(n % 5) as usize];
        m.run_routed(format!(
            "INSERT INTO t (id, grp, name, amt) VALUES ({n}, {}, '{name}', {})",
            n % 10,
            n % 7
        ))
        .expect("seed t");
    }
    for g in 0..10i64 {
        m.run_routed(format!("INSERT INTO r (grp, label) VALUES ({g}, 'g{g}')"))
            .expect("seed r");
    }
}

/// A statement's answer, normalised to a comparable string.
///
/// Rows are sorted when the statement has no `ORDER BY`, so that a difference in *ordering* is
/// only reported for statements that actually asked for an order. A difference in the row *set* is
/// always reported.
fn answer(m: &ShardManager, sql: &str) -> String {
    match m.query_all_shards(sql) {
        Ok(result) => {
            let mut rows: Vec<String> = result.rows.iter().map(|r| format!("{r:?}")).collect();
            if !sql.to_ascii_uppercase().contains("ORDER BY") {
                rows.sort();
            }
            format!("OK cols={:?} rows=[{}]", result.columns, rows.join(", "))
        }
        Err(e) => format!("ERR {e}"),
    }
}

fn write_answer(m: &ShardManager, sql: &str) -> String {
    match m.run_routed(sql) {
        Ok(Outcome::Ok(Executed::Changed(w))) => format!("CHANGED {}", w.rows_affected),
        Ok(Outcome::Ok(Executed::Rows(r))) => format!("ROWS {:?}", r.rows),
        Ok(Outcome::Rejected(msg)) => format!("REJECTED {msg}"),
        Err(e) => format!("ERR {e}"),
    }
}

/// Assert that every shard count in [`COUNTS`] gives the same answer, using N=1 as the reference.
fn assert_aligned(sql: &str) {
    let mut reference: Option<(u32, String)> = None;
    for count in COUNTS {
        let fixture = open(count);
        seed(&fixture.manager);
        let got = answer(&fixture.manager, sql);
        match &reference {
            None => reference = Some((count, got)),
            Some((ref_count, expected)) => assert_eq!(
                &got, expected,
                "\n  divergence for: {sql}\n  N={ref_count}: {expected}\n  N={count}: {got}\n"
            ),
        }
    }
}

/// Assert alignment for a sequence of statements applied in order, comparing every step.
fn assert_aligned_sequence(steps: &[&str]) {
    let mut reference: Option<(u32, Vec<String>)> = None;
    for count in COUNTS {
        let fixture = open(count);
        seed(&fixture.manager);
        let got: Vec<String> = steps
            .iter()
            .map(|s| {
                if s.to_ascii_uppercase().trim_start().starts_with("SELECT") {
                    answer(&fixture.manager, s)
                } else {
                    write_answer(&fixture.manager, s)
                }
            })
            .collect();
        match &reference {
            None => reference = Some((count, got)),
            Some((ref_count, expected)) => {
                for (i, step) in steps.iter().enumerate() {
                    assert_eq!(
                        got[i], expected[i],
                        "\n  divergence at step {i}: {step}\n  N={ref_count}: {}\n  N={count}: {}\n",
                        expected[i], got[i]
                    );
                }
            }
        }
    }
}

fn assert_all_aligned(corpus: &[&str]) {
    for sql in corpus {
        assert_aligned(sql);
    }
}

/// [`assert_aligned_sequence`], with table classes declared first — a class must be declared
/// before `CREATE TABLE`, because the constraint gate consults it.
fn assert_declared_aligned(declarations: &[(&str, TableClass)], steps: &[&str]) {
    let mut reference: Option<(u32, Vec<String>)> = None;
    for count in COUNTS {
        let fixture = open(count);
        for (table, class) in declarations {
            fixture
                .manager
                .declare_table_class(table, *class)
                .expect("declare table class");
        }
        let got: Vec<String> = steps
            .iter()
            .map(|s| {
                if s.to_ascii_uppercase().trim_start().starts_with("SELECT") {
                    answer(&fixture.manager, s)
                } else {
                    write_answer(&fixture.manager, s)
                }
            })
            .collect();
        match &reference {
            None => reference = Some((count, got)),
            Some((ref_count, expected)) => {
                for (i, step) in steps.iter().enumerate() {
                    assert_eq!(
                        got[i], expected[i],
                        "\n  divergence at step {i}: {step}\n  N={ref_count}: {}\n  N={count}: {}\n",
                        expected[i], got[i]
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The corpus
// ---------------------------------------------------------------------------

#[test]
fn projection_and_scalar_functions_align() {
    assert_all_aligned(&[
        "SELECT id FROM t",
        "SELECT id, name FROM t",
        "SELECT * FROM t",
        "SELECT lower(name) FROM t",
        "SELECT upper(name) FROM t",
        "SELECT length(name) FROM t",
        "SELECT abs(amt) FROM t",
        "SELECT substr(name, 1, 2) FROM t",
        "SELECT coalesce(name, 'z') FROM t",
        "SELECT ifnull(name, 'z') FROM t",
        "SELECT trim(name) FROM t",
        "SELECT replace(name, 'a', 'A') FROM t",
        "SELECT hex(amt) FROM t",
        "SELECT typeof(amt) FROM t",
        "SELECT round(amt / 2.0, 2) FROM t",
        "SELECT id * 2 + 1 FROM t",
        "SELECT id || '-' || name FROM t",
        "SELECT CASE WHEN amt > 3 THEN 'hi' ELSE 'lo' END FROM t",
        "SELECT nullif(grp, 0) FROM t",
        "SELECT json_valid(name) FROM t",
    ]);
}

/// D3 — `min`/`max` with two or more arguments are scalar, not aggregate. Merging them as
/// aggregates collapsed 40 rows into one, and the survivor differed by shard count.
#[test]
fn d3_two_argument_min_max_are_scalar() {
    assert_all_aligned(&[
        "SELECT min(amt, 5) FROM t",
        "SELECT max(amt, 5) FROM t",
        "SELECT min(amt, grp, 4) FROM t",
        "SELECT max(id, amt) FROM t",
    ]);

    // Not merely aligned — correct. One row in, one row out.
    let fixture = open(4);
    seed(&fixture.manager);
    let rows = fixture
        .manager
        .query_all_shards("SELECT min(amt, 5) FROM t")
        .expect("scalar min is not an aggregate")
        .rows;
    assert_eq!(
        rows.len(),
        40,
        "scalar min(amt,5) must return one row per row"
    );
}

#[test]
fn aggregates_align() {
    assert_all_aligned(&[
        "SELECT COUNT(*) FROM t",
        "SELECT SUM(amt) FROM t",
        "SELECT MIN(amt) FROM t",
        "SELECT MAX(amt) FROM t",
        "SELECT AVG(amt) FROM t",
        "SELECT TOTAL(amt) FROM t",
        "SELECT COUNT(DISTINCT grp) FROM t",
        "SELECT group_concat(name) FROM t",
        "SELECT json_group_array(name) FROM t",
        "SELECT COUNT(*) FROM t WHERE amt > 2",
    ]);
}

#[test]
fn grouping_aligns() {
    assert_all_aligned(&[
        "SELECT grp, COUNT(*) FROM t GROUP BY grp",
        "SELECT grp, SUM(amt) FROM t GROUP BY grp",
        "SELECT grp, AVG(amt) FROM t GROUP BY grp",
        "SELECT grp, MIN(amt), MAX(amt) FROM t GROUP BY grp",
        "SELECT grp, COUNT(*) FROM t GROUP BY grp HAVING COUNT(*) > 3",
        "SELECT name, COUNT(*) FROM t GROUP BY name",
        "SELECT lower(name), COUNT(*) FROM t GROUP BY lower(name)",
    ]);
}

#[test]
fn filters_align() {
    assert_all_aligned(&[
        "SELECT id FROM t WHERE grp = 3",
        "SELECT id FROM t WHERE id = 17",
        "SELECT id FROM t WHERE lower(name) = 'ada'",
        "SELECT id FROM t WHERE name LIKE 'a%'",
        "SELECT id FROM t WHERE id IN (1, 2, 3)",
        "SELECT id FROM t WHERE amt BETWEEN 2 AND 4",
        "SELECT id FROM t WHERE name IS NOT NULL",
        "SELECT id FROM t WHERE grp = 3 AND amt = 2",
    ]);
}

#[test]
fn distinct_and_set_operations_align() {
    assert_all_aligned(&[
        "SELECT DISTINCT grp FROM t",
        "SELECT DISTINCT name FROM t",
        "SELECT id FROM t WHERE grp = 1 UNION SELECT id FROM t WHERE grp = 2",
        "SELECT id FROM t WHERE grp = 1 UNION ALL SELECT id FROM t WHERE grp = 2",
        "SELECT COUNT(*) FROM (SELECT grp FROM t) AS s",
    ]);
}

#[test]
fn subqueries_and_joins_align() {
    assert_all_aligned(&[
        "SELECT id FROM t WHERE grp IN (SELECT grp FROM r WHERE label = 'g3')",
        "SELECT COUNT(*) FROM t WHERE EXISTS (SELECT 1 FROM r WHERE r.grp = t.grp)",
        "SELECT t.id, r.label FROM t JOIN r ON t.grp = r.grp ORDER BY t.id LIMIT 5",
    ]);
}

/// D1 — a `FROM`-less `SELECT` reads no shard data, so fanning it out returned one identical row
/// per shard. `SELECT 1` is what every connection pool and ORM liveness probe sends.
#[test]
fn d1_from_less_select_answers_once() {
    assert_all_aligned(&[
        "SELECT 1",
        "SELECT 1 + 1 AS n",
        "SELECT 'hello'",
        "SELECT date('2020-01-01')",
        "SELECT lower('ABC')",
        "SELECT (SELECT COUNT(*) FROM t)",
        "SELECT (SELECT MAX(id) FROM t) AS m",
    ]);

    for count in COUNTS {
        let fixture = open(count);
        seed(&fixture.manager);
        let rows = fixture
            .manager
            .query_all_shards("SELECT 1")
            .expect("SELECT 1")
            .rows;
        assert_eq!(
            rows.len(),
            1,
            "SELECT 1 returned {} rows at N={count}",
            rows.len()
        );
    }
}

/// D1 on the real client path, not just the fan-out helper.
#[test]
fn d1_from_less_select_answers_once_through_run_routed() {
    for count in COUNTS {
        let fixture = open(count);
        seed(&fixture.manager);
        match fixture.manager.run_routed("SELECT 1") {
            Ok(Outcome::Ok(Executed::Rows(r))) => {
                assert_eq!(r.rows.len(), 1, "run_routed(SELECT 1) at N={count}");
            }
            other => panic!("unexpected outcome at N={count}: {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Known-divergent: these fail today and are the finish line for later phases.
// ---------------------------------------------------------------------------

/// D2 — window functions are numbered per shard and concatenated, so `row_number()` restarts on
/// every shard. Fixed by routing any `OVER` clause to central execution.
#[test]
fn d2_window_functions_align() {
    assert_all_aligned(&[
        "SELECT id, row_number() OVER (ORDER BY id) FROM t ORDER BY id",
        "SELECT id, rank() OVER (ORDER BY grp) FROM t ORDER BY id",
        "SELECT id, SUM(amt) OVER () FROM t ORDER BY id",
    ]);
}

/// D4 + D8 — without a total order, which rows a `LIMIT` returns depends on shard count. D8 is
/// the sharper case: a `LIMIT` over a tied `ORDER BY` returns a different row *set*, not merely a
/// different order.
#[test]
fn d4_d8_ordering_is_total() {
    assert_all_aligned(&[
        "SELECT id FROM t LIMIT 3",
        "SELECT id FROM t LIMIT 10",
        "SELECT id, grp FROM t ORDER BY grp LIMIT 5",
        "SELECT id, grp FROM t ORDER BY grp DESC LIMIT 4",
        "SELECT id, grp FROM t ORDER BY grp",
        "SELECT id, name FROM t ORDER BY name LIMIT 6",
        "SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 10",
        "SELECT id, grp FROM t ORDER BY grp LIMIT 5 OFFSET 3",
        // A wildcard projection has no statically known width, so it is ordered by the shard key
        // rather than by ordinals — the one column `*` is guaranteed to include.
        "SELECT * FROM t LIMIT 3",
        "SELECT * FROM t LIMIT 7",
        "SELECT DISTINCT grp FROM t LIMIT 4",
        "SELECT name FROM t ORDER BY name LIMIT 6",
    ]);
}

/// D5 — `UNIQUE` on a non-shard-key column used to be enforced by luck: whether it held depended
/// on where two values hashed, so it held at N=1 and N=3 and failed at N=4. The constraint is now
/// refused at `CREATE TABLE`, identically at every shard count.
#[test]
fn d5_unique_on_non_key_column() {
    assert_aligned_sequence(&[
        "CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT UNIQUE)",
        "INSERT INTO u (id, email) VALUES (1, 'a@x')",
        "INSERT INTO u (id, email) VALUES (2, 'a@x')",
        "SELECT COUNT(*) FROM u",
    ]);

    // Refused, not silently weakened — and the message must name both ways out.
    for count in COUNTS {
        let fixture = open(count);
        let err = fixture
            .manager
            .run_routed("CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
            .expect_err("a non-key UNIQUE cannot be enforced on a sharded table")
            .to_string();
        assert!(err.contains("UNIQUE"), "at N={count}: {err}");
        assert!(err.contains("global"), "at N={count}: {err}");
    }
}

/// The escape hatch that makes D5's refusal actionable: a table declared `global` keeps full
/// SQLite semantics — `UNIQUE` really is enforced — identically at every shard count.
#[test]
fn a_global_table_enforces_unique_at_every_shard_count() {
    let mut reference: Option<(u32, Vec<String>)> = None;
    for count in COUNTS {
        let fixture = open(count);
        fixture
            .manager
            .declare_table_class("u", TableClass::Global)
            .expect("declare global");
        let got: Vec<String> = [
            "CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT UNIQUE)",
            "INSERT INTO u (id, email) VALUES (1, 'a@x')",
            "INSERT INTO u (id, email) VALUES (2, 'a@x')",
            "SELECT COUNT(*) FROM u",
        ]
        .iter()
        .map(|s| {
            if s.starts_with("SELECT") {
                answer(&fixture.manager, s)
            } else {
                write_answer(&fixture.manager, s)
            }
        })
        .collect();

        assert!(
            got[2].contains("UNIQUE constraint failed"),
            "a global table must enforce UNIQUE at N={count}, got {}",
            got[2]
        );
        assert!(got[3].contains("Integer(1)"), "at N={count}: {}", got[3]);
        match &reference {
            None => reference = Some((count, got)),
            Some((ref_count, expected)) => {
                assert_eq!(
                    &got, expected,
                    "global table diverged N={ref_count} vs N={count}"
                )
            }
        }
    }
}

/// A global table has no shard key, so a DB-assigned key works exactly as it does on plain
/// SQLite — at every shard count. This is what an ORM needs.
///
/// Asserting alignment alone would pass vacuously: without global routing the insert is *refused*
/// at every shard count, which is aligned and useless. So the rows themselves are asserted.
#[test]
fn a_global_table_accepts_db_assigned_keys() {
    let steps = [
        "CREATE TABLE k (id INTEGER PRIMARY KEY, v TEXT)",
        "INSERT INTO k (v) VALUES ('a')",
        "INSERT INTO k (v) VALUES ('b')",
        "SELECT id, v FROM k ORDER BY id",
    ];
    assert_declared_aligned(&[("k", TableClass::Global)], &steps);

    for count in COUNTS {
        let fixture = open(count);
        fixture
            .manager
            .declare_table_class("k", TableClass::Global)
            .expect("declare global");
        for sql in &steps[..3] {
            fixture
                .manager
                .run_routed(*sql)
                .unwrap_or_else(|e| panic!("at N={count}, `{sql}` failed: {e}"));
        }
        let rows = fixture
            .manager
            .query_all_shards("SELECT id, v FROM k ORDER BY id")
            .expect("read back")
            .rows;
        assert_eq!(
            format!("{rows:?}"),
            r#"[[Integer(1), Text("a")], [Integer(2), Text("b")]]"#,
            "a global table must assign keys like plain SQLite at N={count}"
        );
    }
}

/// D6 — a multi-row `INSERT` is atomic at one shard and partially applied above it, reporting
/// failure in both cases. Resolved by two-phase commit; see docs/cross-shard-atomicity-plan.md.
#[test]
fn d6_multi_row_insert_is_atomic() {
    assert_aligned_sequence(&[
        "CREATE TABLE p (id INTEGER PRIMARY KEY, v TEXT)",
        "INSERT INTO p (id, v) VALUES (3, 'seed')",
        "INSERT INTO p (id, v) VALUES (1,'a'),(2,'b'),(3,'dup'),(4,'d'),(5,'e')",
        "SELECT COUNT(*) FROM p",
        "SELECT id FROM p ORDER BY id",
    ]);
}

/// D6 — the same defect on the DDL broadcast path: a `CREATE TABLE` that fails partway leaves the
/// earlier shards with the new schema.
///
/// The failure has to be arranged on a *late* shard. Making the broadcast fail on shard 0 proves
/// nothing, because it aborts before applying anything anywhere — which is what an earlier version
/// of this test did, and why it passed while the defect was still present.
#[test]
fn d6_ddl_broadcast_is_atomic() {
    for count in COUNTS {
        let fixture = open(count);
        let last = ShardId(count - 1);
        fixture
            .manager
            .execute_one(last, "CREATE TABLE d1 (id INTEGER PRIMARY KEY)")
            .expect("pre-create on the last shard");

        // The broadcast now fails when it reaches the last shard, after applying to the earlier
        // ones. Under two-phase commit it must leave no trace.
        let _ = fixture
            .manager
            .run_routed("CREATE TABLE d1 (id INTEGER PRIMARY KEY)");

        let carriers = (0..count)
            .filter(|s| {
                matches!(
                    fixture.manager.query(
                        ShardId(*s),
                        "SELECT COUNT(*) FROM sqlite_master WHERE name = 'd1'",
                    ),
                    Ok(Outcome::Ok(Executed::Rows(ref r)))
                        if format!("{:?}", r.rows).contains("Integer(1)")
                )
            })
            .count();
        assert_eq!(
            carriers, 1,
            "at N={count}, a failed CREATE TABLE broadcast left the schema on {carriers} shards; \
             only the pre-created one should have it"
        );
    }
}

/// D7 — a `FOREIGN KEY` to an existing parent used to be *rejected* above one shard, because the
/// check runs on the child's shard and the parent lives elsewhere. The constraint is now refused
/// at `CREATE TABLE`, identically at every shard count.
#[test]
fn d7_foreign_key_to_existing_parent() {
    assert_aligned_sequence(&[
        "CREATE TABLE par (id INTEGER PRIMARY KEY, n TEXT)",
        "CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES par(id))",
        "INSERT INTO par (id, n) VALUES (1, 'p')",
        "INSERT INTO ch (id, pid) VALUES (7, 1)",
        "SELECT COUNT(*) FROM ch",
    ]);

    for count in COUNTS {
        let fixture = open(count);
        fixture
            .manager
            .run_routed("CREATE TABLE par (id INTEGER PRIMARY KEY, n TEXT)")
            .expect("parent table");
        let err = fixture
            .manager
            .run_routed("CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES par(id))")
            .expect_err("a FOREIGN KEY cannot be enforced on a sharded table")
            .to_string();
        assert!(err.contains("FOREIGN KEY"), "at N={count}: {err}");
        assert!(err.contains("global"), "at N={count}: {err}");
    }
}

/// The escape hatch for D7: declared global, both tables live on shard 0, so a valid reference is
/// accepted and an invalid one rejected — identically at every shard count.
#[test]
fn global_tables_enforce_foreign_keys_at_every_shard_count() {
    assert_declared_aligned(
        &[("par", TableClass::Global), ("ch", TableClass::Global)],
        &[
            "CREATE TABLE par (id INTEGER PRIMARY KEY, n TEXT)",
            "CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES par(id))",
            "INSERT INTO par (id, n) VALUES (1, 'p')",
            // A valid reference is accepted...
            "INSERT INTO ch (id, pid) VALUES (7, 1)",
            // ...and a dangling one is rejected.
            "INSERT INTO ch (id, pid) VALUES (8, 999)",
            "SELECT id, pid FROM ch ORDER BY id",
        ],
    );
}

// ---------------------------------------------------------------------------
// Behaviours that are aligned today; these guard against regression.
// ---------------------------------------------------------------------------

#[test]
fn writes_align() {
    assert_aligned_sequence(&[
        "CREATE TABLE w (id INTEGER PRIMARY KEY, v INTEGER)",
        "INSERT INTO w (id, v) VALUES (1, 1)",
        "INSERT INTO w (id, v) VALUES (2, 1)",
        "UPDATE w SET v = 9",
        "SELECT SUM(v) FROM w",
        "UPDATE w SET v = 5 WHERE id = 1",
        "SELECT id, v FROM w ORDER BY id",
        "DELETE FROM w WHERE id = 2",
        "SELECT COUNT(*) FROM w",
        "DELETE FROM w",
        "SELECT COUNT(*) FROM w",
    ]);
}

#[test]
fn constraints_that_shard_cleanly_align() {
    assert_aligned_sequence(&[
        "CREATE TABLE c (id INTEGER PRIMARY KEY, amt INTEGER CHECK (amt >= 0))",
        "INSERT INTO c (id, amt) VALUES (1, 5)",
        "INSERT INTO c (id, amt) VALUES (2, -5)",
        "SELECT COUNT(*) FROM c",
    ]);
}

#[test]
fn conflict_resolution_aligns() {
    assert_aligned_sequence(&[
        "CREATE TABLE oc (id INTEGER PRIMARY KEY, v INTEGER)",
        "INSERT INTO oc (id, v) VALUES (1, 1)",
        "INSERT INTO oc (id, v) VALUES (1, 2) ON CONFLICT(id) DO UPDATE SET v = 99",
        "SELECT v FROM oc WHERE id = 1",
        "INSERT INTO oc (id, v) VALUES (1, 3) ON CONFLICT(id) DO NOTHING",
        "SELECT v FROM oc WHERE id = 1",
    ]);
}

/// Refusals are part of the contract: a statement refused at one shard count must be refused at
/// every shard count, with the same message.
#[test]
fn refusals_align() {
    assert_all_aligned(&[
        "WITH x AS (SELECT id FROM t) SELECT COUNT(*) FROM x",
        "SELECT id, grp FROM t ORDER BY amt",
        "SELECT * FROM t, r",
    ]);
}

// ---------------------------------------------------------------------------
// Strict mode: refuses more, never answers differently.
// ---------------------------------------------------------------------------

fn open_strict(shard_count: u32) -> Fixture {
    let dir = TempDir::new().expect("temp dir");
    let manager = ShardManager::open(
        dir.path(),
        ShardConfig {
            shard_count,
            strict: true,
            ..ShardConfig::floor()
        },
    )
    .expect("open strict shard manager");
    Fixture { _dir: dir, manager }
}

fn seed_strict(m: &ShardManager) {
    m.declare_table_class("t", TableClass::Sharded).unwrap();
    m.run_routed("CREATE TABLE t (id INTEGER PRIMARY KEY, grp INTEGER, name TEXT, amt INTEGER)")
        .expect("create t");
    for n in 1..=40i64 {
        let name = ["ada", "bob", "cy", "dee", "eve"][(n % 5) as usize];
        m.run_routed(format!(
            "INSERT INTO t (id, grp, name, amt) VALUES ({n}, {}, '{name}', {})",
            n % 10,
            n % 7
        ))
        .expect("seed t");
    }
}

/// Each row of the strict table in the plan: a choice shardlite would otherwise make silently.
#[test]
fn strict_refuses_every_implicit_choice() {
    for count in COUNTS {
        let fixture = open_strict(count);
        seed_strict(&fixture.manager);

        // An undeclared table class would default to sharded.
        let err = fixture
            .manager
            .run_routed("CREATE TABLE undeclared (id INTEGER PRIMARY KEY)")
            .expect_err("class must be declared under strict")
            .to_string();
        assert!(err.contains("declared table class"), "at N={count}: {err}");

        // A LIMIT with no ORDER BY does not say which rows to keep.
        let err = fixture
            .manager
            .query_all_shards("SELECT id FROM t LIMIT 3")
            .expect_err("LIMIT without ORDER BY must be refused under strict")
            .to_string();
        assert!(
            err.contains("LIMIT without ORDER BY"),
            "at N={count}: {err}"
        );

        // A LIMIT over a tied ORDER BY does not say which tied row survives.
        let err = fixture
            .manager
            .query_all_shards("SELECT id, grp FROM t ORDER BY grp LIMIT 3")
            .expect_err("a non-total order with LIMIT must be refused under strict")
            .to_string();
        assert!(err.contains("total order"), "at N={count}: {err}");

        // A plan that materialises on the coordinator is a silent cost.
        let err = fixture
            .manager
            .query_all_shards("SELECT z.id FROM (SELECT id FROM t) z")
            .expect_err("central execution must be refused under strict")
            .to_string();
        assert!(err.contains("pushed down"), "at N={count}: {err}");
    }
}

/// The rule that keeps strict compatible with the one-behaviour contract: strict shrinks what is
/// accepted, and never changes what an accepted statement returns.
#[test]
fn strict_never_changes_an_answer() {
    let accepted_by_both = [
        "SELECT id FROM t ORDER BY id LIMIT 5",
        "SELECT id, grp FROM t ORDER BY grp, id LIMIT 7",
        "SELECT COUNT(*) FROM t",
        "SELECT grp, SUM(amt) FROM t GROUP BY grp",
        "SELECT lower(name) FROM t",
        "SELECT id FROM t WHERE grp = 3",
        "SELECT min(amt, 5) FROM t",
        "SELECT 1",
    ];

    for count in COUNTS {
        let lax = open(count);
        seed_strict(&lax.manager);
        let strict = open_strict(count);
        seed_strict(&strict.manager);

        for sql in accepted_by_both {
            let relaxed = answer(&lax.manager, sql);
            let stricter = answer(&strict.manager, sql);
            assert!(
                !stricter.starts_with("ERR"),
                "at N={count}, strict refused a statement it should accept: {sql} => {stricter}"
            );
            assert_eq!(
                relaxed, stricter,
                "\n  strict changed the answer for: {sql}\n  default: {relaxed}\n  strict:  {stricter}\n"
            );
        }
    }
}

/// Strict belongs to the database, not to the connection that opened it: reopening without asking
/// for strict must not relax a database that was created strict.
#[test]
fn strict_is_sticky_across_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let m = ShardManager::open(
            dir.path(),
            ShardConfig {
                shard_count: 4,
                strict: true,
                ..ShardConfig::floor()
            },
        )
        .unwrap();
        assert!(m.is_strict());
    }
    let reopened = ShardManager::open(
        dir.path(),
        ShardConfig {
            shard_count: 4,
            strict: false,
            ..ShardConfig::floor()
        },
    )
    .unwrap();
    assert!(
        reopened.is_strict(),
        "a strict database must stay strict when reopened without asking for it"
    );
}
