# Enhancement plan — cross-shard `GROUP BY` via two-phase aggregation

> Status: **plan, for review.** Target chosen with the user: lift the `GROUP BY` refusal by
> teaching the cross-shard planner to decompose a grouped aggregate into a per-shard *partial*
> aggregation and a coordinator *re-aggregation*. Unlocks `GROUP BY`, aggregate-alongside-group-
> columns, and `AVG` — the decomposable refusals the failure-modes chapter flags as "planner
> decisions, not architectural blocks."

## What it unlocks

Queries of this shape, fanned out correctly across shards:

```sql
SELECT region, COUNT(*), SUM(amount), MIN(ts), MAX(ts), AVG(amount)
FROM orders
WHERE ts >= ?              -- WHERE is already pushed down today
GROUP BY region
ORDER BY 2 DESC
LIMIT 20;
```

Specifically, this plan delivers, across shards:
- `GROUP BY` on one or more columns/expressions.
- The **grouping columns alongside aggregates** (today refused at `plan.rs:199` because there was
  no group semantics to make plain columns meaningful — now there is).
- The decomposable aggregates **`COUNT`, `SUM`, `MIN`, `MAX`** per group.
- **`AVG`** per group, by decomposing it to `SUM`/`COUNT` (the note calls this out explicitly).
- `ORDER BY` (by ordinal or alias) and `LIMIT` over the final grouped result.
- `GROUP BY` with **no aggregate** — i.e. `SELECT region FROM t GROUP BY region`, which is
  `DISTINCT region` and merges for free as a side effect.

`HAVING` is a small, well-scoped **second increment** (it needs a coordinator-side predicate
evaluator over complete groups); see the Increments section.

## Grounding — the current code (verified)

- Refusal today: a single guard — `src/query/plan.rs:169-175` (`GROUP BY`), plus `HAVING` at
  `:176`, and "aggregate alongside other columns" at `:199-206`.
- Planner contract: `enum Plan { SingleShard, Concat, Merge, Aggregate }` — `plan.rs:67-81`;
  `Combine { Sum, Min, Max }` — `plan.rs:51-56`; `SortKey` — `plan.rs:59-65`; `Unsupported` —
  `plan.rs:84`. Entry `plan(sql) -> Result<Plan, Unsupported>` — `plan.rs:102`.
- Aggregate identification: `aggregate_of(&SelectItem)` — `plan.rs:256` (maps `COUNT/SUM/TOTAL`
  → `Sum`, `MIN` → `Min`, `MAX` → `Max`; refuses `AVG`, `GROUP_CONCAT`).
- Merge execution: `merge_results(&Plan, Vec<QueryResult>)` — `src/query/merge.rs:11`;
  `combine_values` — `merge.rs:127`; `compare_values` (SQLite storage-class order) — `merge.rs:102`;
  `compare_rows` — `merge.rs:62`.
- Orchestration: `ShardManager::query_all_shards(&self, sql)` — `src/shard/mod.rs:504-536`. It
  parses once, then **runs the same `sql` on every shard** and calls `merge_results`. Fan-out is a
  sequential `for` loop; a `"no such table"` shard is skipped.
- Parser: `sqlparser = "0.62"` with `SQLiteDialect` — a full AST is already in hand
  (`plan.rs:43-48,102`), and sqlparser AST fragments implement `Display`, so we can re-serialize
  rewritten SQL.
- Tests: `tests/query_planner.rs` — a `twin()` helper builds a sharded DB **and** a single-shard
  ground-truth DB with identical data and asserts fan-out equals single-shard. This is the exact
  harness the enhancement will extend.

**Blast radius: `src/query/` plus one call site.** The wire protocol, gateway, drivers, and CLI
are untouched — a grouped `SELECT` is still just a read that returns rows.

## Design — two-phase aggregation

Classic map-reduce. Each shard computes **partial** groups locally (SQLite groups correctly
*within* a shard); the coordinator **re-aggregates** those partials by group key.

### Phase 1 — per-shard partial aggregation (a rewrite)

The user's SQL cannot be sent verbatim, for two reasons: `AVG` per shard is a mean-of-means
(unrecoverable), and `ORDER BY`/`LIMIT`/`HAVING` must not be applied before groups are complete.
So the planner **synthesizes a per-shard query** from the parsed parts:

- Keep `FROM` and `WHERE` unchanged (serialize the AST fragments via `Display`).
- Keep `GROUP BY <group exprs>`.
- **Drop** `ORDER BY`, `LIMIT`, `OFFSET`, `HAVING` (all are coordinator-side for a grouped query).
- Replace the projection with a canonical partial projection:
  - one column per group expression: `<group_expr> AS __g{i}` — added **whether or not** the user
    selected it, so grouping by an unselected column (`SELECT COUNT(*) … GROUP BY region`) works;
  - one or two columns per aggregate: `COUNT/SUM/MIN/MAX(<arg>) AS __a{j}`; and for `AVG(x)`, the
    pair `SUM(x) AS __a{j}s, COUNT(x) AS __a{j}c`.

Built by string assembly from `Display`-serialized `Expr` fragments (never by hand-constructing
`Function` AST nodes — more robust and version-stable). Example rewrite of the query above:

```sql
SELECT region AS __g0,
       COUNT(*) AS __a0, SUM(amount) AS __a1, MIN(ts) AS __a2, MAX(ts) AS __a3,
       SUM(amount) AS __a4s, COUNT(amount) AS __a4c
FROM orders WHERE ts >= ? GROUP BY region
```

Each shard returns one partial row per local group.

### Phase 2 — coordinator re-aggregation

A new `Plan::Grouped` carries the recipe for folding partial rows into the final answer:

```rust
enum Plan {
    // … existing variants unchanged …
    Grouped(Box<Grouped>),
}

struct Grouped {
    shard_sql: String,          // the rewritten per-shard query (Phase 1)
    group_cols: Vec<usize>,     // indices of __g columns in the partial row
    outputs: Vec<OutputCol>,    // one per FINAL user column, in user order
    output_names: Vec<String>,  // final column headers (alias or expr text)
    order_by: Vec<SortKey>,     // resolved against FINAL outputs (reuses output_column_index)
    limit: Option<usize>,
}

enum OutputCol {
    Group(usize),                              // passthrough of a __g column
    Combine { kind: Combine, col: usize },     // fold one __a column across the group
    Avg { sum_col: usize, count_col: usize },  // SUM(sums) / SUM(counts)
}
```

`merge_results` gains a `Grouped` arm that:
1. Groups partial rows by the tuple of `group_cols` values, using **`compare_values`-based
   equality** (via a `BTreeMap` keyed by an ordered `GroupKey(Vec<Value>)`). This matches SQLite's
   `GROUP BY` semantics, including that NULLs group together and `1` and `1.0` are the same group —
   important when shard A emits `Integer(1)` and shard B `Real(1.0)` for the same group.
2. For each group, folds each `OutputCol`: `Group` → the key value; `Combine` → `combine_values`
   over that column's partials (reused as-is); `Avg` → `SUM(sums)/SUM(counts)`, or `NULL` when the
   count is 0 (matches SQLite's `AVG` of an all-NULL group).
3. Emits final rows in user column order with `output_names` as headers.
4. Applies `ORDER BY` (reusing `compare_rows`) then `LIMIT` — **coordinator-only**, never pushed
   down (a per-shard `LIMIT` would drop groups whose rows are spread across shards).

`combine_values`, `compare_values`, `compare_rows`, and `SortKey` are all reused unchanged.

### Contract change — kept minimal

`plan(sql) -> Result<Plan, Unsupported>` keeps its signature. The rewritten per-shard SQL lives
**inside** `Plan::Grouped`, so `query_all_shards` needs one branch: run `g.shard_sql` on each shard
when the plan is `Grouped`, else run the original `sql` as today (`shard/mod.rs:515`). Every other
plan variant and every existing test is untouched except `plans_are_what_they_claim_to_be`
(`tests/query_planner.rs:233`), which gains cases.

## What stays refused (and why) — still no approximation

The governing rule holds: anything not provably correct is refused with an explicit, >60-char
message, never approximated.

- **`COUNT(DISTINCT x)`, `SUM(DISTINCT x)`, `AVG(DISTINCT x)`** — a value can appear on several
  shards, so per-shard distinct counts/sums double-count. This is the `DISTINCT` problem; refuse
  aggregates carrying a `DISTINCT` argument.
- **A bare non-group column** in a grouped query (SQLite's "bare column" picking an arbitrary row)
  — nondeterministic and unreassemblable across shards; every projection item must be a group
  expression or a decomposable aggregate.
- **`GROUP_CONCAT` / `STRING_AGG`** in a group — concatenation order across shards is undefined
  (unchanged from today).
- **`JOIN`, `UNION`/`INTERSECT`/`EXCEPT`, CTEs, subqueries, `OFFSET`, non-literal `LIMIT`** —
  unchanged; each can still hide a shape fan-out cannot reassemble.
- **`ORDER BY` an unaliased aggregate expression** (`ORDER BY COUNT(*)`) — resolve by ordinal
  (`ORDER BY 2`) or alias instead; `output_column_index` already supports both. (Refused, with a
  message pointing at the workaround, rather than silently mis-sorted.)

## Correctness strategy — differential testing against SQLite

The single strongest tool is already in the repo: `twin()` compares fan-out against a single-shard
database holding identical data — SQLite's own answer is the oracle.

- **Differential matrix** (assert sharded == single-shard): `GROUP BY` on one and multiple keys;
  each of `COUNT(*)`, `COUNT(c)`, `SUM`, `MIN`, `MAX`, `AVG`, and all together; with `WHERE`; with
  `ORDER BY` (asc/desc, ordinal, alias) and `LIMIT`; group keys with NULLs; **mixed int/real group
  keys across shards**; a group present on only one shard; a group spread across all shards; group
  by an unselected column; `GROUP BY` with no aggregate (= `DISTINCT`). Run at 1/4/16/64 shards to
  assert shard-count invariance (extending `the_shard_count_does_not_change_the_answer`).
- **Refusal tests**: `COUNT(DISTINCT)`, bare non-group column, `GROUP_CONCAT`, `ORDER BY COUNT(*)`
  — each returns `Unsupported` with the documented keyword, reaching the caller as an error.
- **Revert-verification** (per project ethos and CLAUDE rule 5): the differential tests are
  non-vacuous by construction — remove the coordinator re-aggregation and a query grouped across
  shards returns duplicate per-shard group rows, failing the equality assertion. Confirm this by
  reverting the `Grouped` merge arm and watching the test fail.
- **`plans_are_what_they_claim_to_be`**: assert the emitted `Grouped` recipe (group_cols, outputs,
  rewritten shard_sql) for representative queries, so a future refactor cannot silently change the
  decomposition.

## Risks and bounds — stated, not discovered later

1. **Coordinator memory / group cardinality.** Phase 2 holds up to (Σ per-shard local group counts)
   partial rows — bounded by (distinct groups × shards), and always ≤ the raw rows the existing
   `Concat`/`Merge` plans already materialize. But a very high-cardinality `GROUP BY` (millions of
   groups) is real memory on the coordinator. Recommend a **configurable group cap that refuses
   loudly** when exceeded ("grouped result exceeded N groups; target a single shard or narrow the
   `WHERE`") — consistent with refuse-over-approximate. Default generous (e.g. 1M groups).
2. **Not a consistent snapshot.** Inherited, unchanged: each shard is read at its own moment, so a
   fan-out can see a write on one shard and miss a concurrent one on another. `GROUP BY` does not
   change this; document it as the existing chapter already does.
3. **AST re-serialization fidelity.** We serialize `Expr`/`FROM`/`WHERE` fragments via `Display`
   under `SQLiteDialect`. Low risk (sqlparser round-trips SQLite), but the differential tests are
   the backstop: any mis-serialization changes the answer and fails a twin() assertion.
4. **`AVG` result type.** SQLite `AVG` yields a float; the `Avg` output emits `Real` (or `NULL`),
   matching single-shard — verified by the differential test.

## Increments

**A — cross-shard `GROUP BY` with `COUNT/SUM/MIN/MAX/AVG`, `ORDER BY`, `LIMIT`** (this plan's core).
Localized to `src/query/plan.rs` (new `plan_grouped` path + refusals), `src/query/merge.rs` (the
`Grouped` merge arm), and one branch in `shard/mod.rs`. Plus the differential + refusal tests.

**B — `HAVING`** (fast follow). Applied at the coordinator on complete groups via a small bounded
evaluator over the `sqlparser` `Expr`: comparisons (`= != < <= > >=`) and boolean combinations of
{a decomposable aggregate | a group column | a literal}, resolved against each group's computed
values. Anything outside that grammar is refused. Stripped from the per-shard query in Phase 1
(already dropped there). Tested the same way — `HAVING COUNT(*) > k`, `HAVING SUM(x) >= v` — against
the single-shard twin.

## Definition of done

1. The differential matrix passes at 1/4/16/64 shards (sharded == single-shard for every listed
   grouped query).
2. The still-refused shapes return `Unsupported` with documented messages, reaching the caller.
3. Revert-verification: removing the `Grouped` merge arm fails a differential test.
4. `clippy` and `fmt` clean; the module doc-comment in `plan.rs:25-34` updated to move `GROUP BY`,
   `AVG`, and aggregate-with-group-columns from "refused" to "merged", with the new refusals
   (`DISTINCT` aggregates, bare non-group columns) listed.
5. The failure-modes / cross-shard chapters in the book noted as updated (the `GROUP BY` and `AVG`
   rows move from "refused" to "supported via two-phase aggregation").

---

# Roadmap for the remaining refusals

`GROUP BY` is the first of a set. Almost every other refusal reduces to a small number of
coordinator primitives, so the sequencing below reuses this increment's machinery rather than
re-inventing it. The non-negotiable holds throughout: **parse fully, prove safety, refuse loudly
(with a memory cap on the materialising ones) — never approximate.**

The three primitives:
1. **dedup-on-merge** — collapse equal rows on the coordinator (`compare_values` equality). Built
   by Increment A.
2. **sort-and-slice-on-merge** — the existing `Plan::Merge` (per-shard sort → coordinator sort →
   truncate), extended with a global skip.
3. **materialise-a-fan-out-safe-sub-result centrally** — pull a bounded, provably-safe sub-result
   fully to the coordinator and finish the operation there. New, and memory-cap-gated.

## Tier 1 — same family as `GROUP BY` — **DONE** (`DISTINCT`, `UNION`/`UNION ALL`, `OFFSET`)

Implemented on the `GROUP BY` machinery as a `Plan::PostProcess` reshape (dedup / sort / skip /
truncate on the coordinator, with the per-shard query rewritten to strip `OFFSET` and widen
`LIMIT` to `offset+limit`). Verified against native SQLite as an independent oracle; the dedup and
re-aggregation paths are revert-verified. Still refused: `INTERSECT`/`EXCEPT`, mixing `UNION` with
`UNION ALL`, `OFFSET` without `ORDER BY`, and any aggregate inside a `DISTINCT`/`UNION`.

| Refusal | Approach | Cost / caveat |
|---|---|---|
| **`DISTINCT`** (`SELECT DISTINCT a, b`) | It *is* `GROUP BY a, b` with no aggregate: push `SELECT DISTINCT` to each shard (local dedup), then coordinator dedups the merge with the same `compare_values` equality. | Reuses Increment A's grouping path almost verbatim. `COUNT(DISTINCT x)` stays refused (double-counts). |
| **`UNION` / `UNION ALL`** | Each shard computes its side(s) locally; coordinator concatenates. `UNION ALL` = pure concat; `UNION` = concat + dedup. Both sides must be fan-out-safe. | Recurse the planner into each side. |
| **`OFFSET`** (with `ORDER BY`) | Push `LIMIT offset+count` (no offset) to each shard, coordinator sorts, drops `offset`, takes `count`. | Deep pagination ships `offset+count` rows per shard — keyset pagination stays the recommendation. `OFFSET` without `ORDER BY` is globally undefined → refuse. |

## Tier 2 — need the central-materialisation primitive (cap-gated)

| Refusal | Approach | Cost / caveat |
|---|---|---|
| **`INTERSECT` / `EXCEPT`** | Not push-down-able — a row can be in `A` on one shard and `B` on another, so per-shard `A∩B` misses cross-shard matches. Materialise both fan-out-safe sides on the coordinator and do the set-op there. | Memory-bound in both sides; cap-gated. |
| **Uncorrelated scalar subquery** (`WHERE x > (SELECT AVG(x) FROM t)`) | Two-pass: compute the inner across shards, substitute the literal, re-plan and re-fan-out the outer. | Needs multi-pass planning + value substitution. |
| **Derived-table / `IN (SELECT …)` subquery** | Materialise the fan-out-safe inner centrally, run the outer over the (small) materialised set. | Same central primitive; cap-gated. |
| **Aggregate + bare column, no `GROUP BY`** | Mostly *subsumed by Increment A* — the fix is "add `GROUP BY`". The `MIN`/`MAX`-with-bare-columns rule is a decomposable **argmin/argmax** (each shard returns its extreme row; coordinator picks the global one) — a small optional feature. | The general bare-column form stays refused: nondeterministic even on one shard. |

## Tier 3 — the architectural line

**`JOIN`** is not a planner decision. meshdb never moves data between shards, so a join fans out
cleanly only when matching rows are **co-located** — both tables sharded on the same key and joined
on it. meshdb routes by an app-supplied key, so co-location is possible by convention but not
*provable* today, which is exactly why the join is refused. The tractable path is an explicit
**co-partitioning declaration**: the app asserts "these tables are co-sharded on this key", and
meshdb trusts that to push `A_shard ⋈ B_shard` per shard and concatenate. General joins (different
keys) need either a data shuffle — a subsystem the architecture deliberately excludes — or a
central hash-join of both fully-materialised sides (correct, expensive, cap-gated).

## Correctly permanent (not gaps)

- **Unparseable SQL** — the planner must understand a query to split and merge it; if it cannot
  parse it, it refuses. That is the safety boundary. (Widening `sqlparser` coverage shrinks the
  tail, never to zero.)
- **Correlated subqueries** — reduce to a join and inherit its limits.

## Sequencing

**A. `GROUP BY`** (this document) → **fold in `DISTINCT` + `UNION`/`UNION ALL` + `OFFSET`** (nearly
free on A's machinery) → **B. `HAVING`** → **the central-materialisation primitive**
(`INTERSECT`/`EXCEPT` + uncorrelated scalar subqueries) → **co-located `JOIN`** as its own opt-in
feature. Each step keeps parse-prove-refuse and adds a memory cap wherever it materialises.
