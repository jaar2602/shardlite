# Shard-count alignment — one shard and many shards must mean the same thing

> **Status: implemented and verified. All eight measured divergences are fixed.**
>
> Guarded by [`tests/alignment.rs`](../tests/alignment.rs) — a fixed corpus run at N=1, 3, 4 and 8
> requiring byte-identical results, including identical refusals — and by
> [`tests/cross_shard_atomicity.rs`](../tests/cross_shard_atomicity.rs) for the write-atomicity
> half. Every guard was checked by removing its fix and confirming the test fails.
>
> Every divergence below was measured against `1029f0c` before the fixes. Counts and outputs are
> quoted as observed. MySQL/Postgres compatibility is explicitly out of scope; see the closing
> section.
>
> | | Divergence | Fix |
> |---|---|---|
> | D1 | `FROM`-less `SELECT` fanned out | `Plan::SingleShard` |
> | D2 | window functions numbered per shard | routed to central execution |
> | D3 | 2-arg scalar `min`/`max` merged as aggregates | arity checked before name |
> | D4 | unordered `LIMIT` | deterministic total order |
> | D8 | tied `ORDER BY` returned different rows | deterministic total order |
> | D5 | non-key `UNIQUE` enforced by luck | DDL constraint gate + `global` class |
> | D7 | valid `FOREIGN KEY` rejected | DDL constraint gate + `global` class |
> | D6 | multi-shard write partially applied | two-phase commit |
>
> **One boundary remains.** A fan-out whose participants live on *other nodes* still applies per
> shard, because parking a transaction on a remote shard needs a Prepare/Commit RPC that is not
> built yet. Single-node and full-replica writes are atomic; a write that leaves this node is not.
> That is the one place the one-behaviour rule is not yet satisfied, and it is Phase 3 of
> [cross-shard-atomicity-plan.md](cross-shard-atomicity-plan.md).

## The contract

> **A statement accepted at one shard returns the same answer at any shard count, or is refused at
> every shard count.**

That is the whole goal. It is deliberately weaker than "shardlite supports all of SQLite" and
deliberately stronger than "shardlite supports a documented subset": a user must never discover
that growing from 1 to 2 shards changed what their existing SQL does.

Three consequences shape everything below.

**One behaviour, never a conditional one.** Where a divergence could be resolved either by making
the behaviour uniform or by documenting the difference, it is made uniform. "Undefined by SQL, so
it may vary" is not an acceptable resolution here, because a user who observes rowid order at one
shard will depend on it.

The rule constrains **answers**, and it constrains them along two axes:

- *Semantics, not mechanism.* A path may skip provably unnecessary work — a one-participant commit
  needs no coordinator log, because SQLite's own transaction already provides the guarantee.
- *Answers, not acceptance.* A statement that is **accepted** must return the same result
  everywhere. Whether a statement is accepted at all may legitimately vary, provided the variation
  is a declared property of the database and never silently changes a result.

That second clause is what makes [strict mode](#strict-mode--no-implicit-choices) compatible with
the rule rather than an exception to it. Strict mode only ever *refuses more*; it never answers
differently.

**Alignment is achieved by raising N>1 to match N=1, never by lowering N=1.** Where raising is
impossible — a constraint that cannot span independent SQLite files — the guarantee is attached to
a *declaration at `CREATE TABLE`* rather than to the shard count, so both ends obey the
declaration and neither obeys N. A declaration is not a conditional: it is fixed once, at DDL, and
holds identically at every shard count thereafter.

**The single-shard planner bypass is dropped.** It was the top item on the previous roadmap. Under
this contract it is backwards: today the planner runs unconditionally, so CTEs, `GROUP_CONCAT`,
correlated subqueries and `ANY`/`ALL` are refused *equally* at N=1 and N=4 — aligned, if
unimpressive. A bypass would manufacture roughly six new divergences in exchange for a nicer
single-shard demo. It is the single most tempting wrong move available here.

## Measured baseline

A 46-statement read corpus and 10 write scenarios, seeded with identical logical data and run at
N=1, N=3 and N=4. The seed is deliberately low-cardinality (`grp = n % 10`, five repeating names)
so that a wrong cross-shard combine cannot pass vacuously.

**Reads: 41 of 46 identical across all three shard counts. 5 divergent.**
**Writes: 7 of 10 scenarios identical. 3 divergent.**

A follow-up probe found a sixth read divergence (D8) that the corpus missed because every
`ORDER BY` in it sorted on a unique column. It is listed with the others below.

The alignment problem is much smaller than the general "SQL surface" problem, and it is
concentrated in two places: a handful of planner bugs, and constraint enforcement.

### Read divergences

**D1 · A `FROM`-less `SELECT` returns one row per shard.**

```
run_routed("SELECT 1")   N=1 → [[1]]              N=4 → [[1],[1],[1],[1]]
SELECT 1+1 AS n          N=1 → [[2]]              N=4 → [[2],[2],[2],[2]]
SELECT date('2020-01-01')                         N=4 → four identical rows
SELECT (SELECT COUNT(*) FROM t)  N=1 → [[8]]      N=4 → [[8],[8],[8],[8]]
```

There is no table, so there is nothing to fan out over, but the statement still routes to
`Route::Passthrough` and every shard answers. This is the highest-traffic statement in existence —
`SELECT 1` is what every connection pool, health check and ORM liveness probe sends. Confirmed on
the real client path (`run_routed`), not just the fan-out helper.

**D2 · Window functions are numbered per shard and concatenated.**

```
SELECT id, row_number() OVER (ORDER BY id) FROM t ORDER BY id
  N=1  (1,1) (2,2) (3,3) (4,4) (5,5) …
  N=3  (1,1) (2,2) (3,1) (4,3) (5,2) …
  N=4  (1,1) (2,1) (3,1) (4,1) (5,2) …
```

Each shard numbers from 1 and the results concatenate. `rank()` behaves the same way. Cause:
`is_aggregate_shaped` in `src/query/plan.rs:1859` tests `f.over.is_none()`, so a windowed call
escapes classification entirely and falls through to `Plan::Concat`.

**D3 · Two-argument scalar `min`/`max` are merged as aggregates.**

```
SELECT min(amt, 5) FROM t     N=1 → [[1]]   N=3 → [[0]]   N=4 → [[1]]
```

The correct answer is 40 rows. All three shard counts are wrong, and they disagree with each
other. `MIN`/`MAX` are matched by name in `classify_aggregate` and `aggregate_of` before arity is
examined, and SQLite's two-argument `min`/`max` are scalar functions.

**D4 · `LIMIT` without `ORDER BY` returns different rows.**

```
SELECT id FROM t LIMIT 3      N=1 → 1,2,3   N=3 → 1,2,4   N=4 → 1,5,9
```

SQL does not define row order without `ORDER BY`; N=1 merely happens to give rowid order. Permitted
by the standard, but not permitted by this contract — every SQLite user has internalised the
accidental ordering, and "it varies" is exactly the conditional behaviour the rule forbids.

**D8 · `ORDER BY` on a non-unique column returns different rows.**

```
SELECT id,grp FROM t ORDER BY grp DESC LIMIT 4
  N=1 → ids 2, 5, 8, 11
  N=4 → ids 5, 17, 8, 20
```

The corpus missed this because every `ORDER BY` in it sorted on a unique column. Ties have no
deterministic tiebreak, so each shard breaks them by its own local scan order and the merge
preserves that. **Note this returns a different result *set*, not merely a different order** — with
`LIMIT` over a tie, which rows survive depends on the shard count.

Same root cause as D4: no total order. The two collapse into one fix.

### Write divergences

**D5 · `UNIQUE` on a non-shard-key column is enforced by luck.**

```
CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT UNIQUE)
INSERT (1,'a@x'); INSERT (2,'a@x')
  N=1  REJECTED  → 1 row
  N=3  REJECTED  → 1 row      ← both rows happened to hash to the same shard
  N=4  CHANGED 1 → 2 rows
```

Note the N=3 result. Enforcement is not merely weaker at higher shard counts — it is
**data-dependent**. Whether a constraint holds depends on where two particular values hash. That
is worse than a documented limitation and is the strongest argument in this document for moving
the decision to DDL time.

**D6 · A multi-row `INSERT` is atomic at N=1 and partially applied above it.**

```
seed id=3, then INSERT (1,'a'),(2,'b'),(3,'dup'),(4,'d'),(5,'e')
  N=1  Rejected → 1 row  (atomic; nothing applied)
  N=3  Rejected → 4 rows
  N=4  Rejected → 5 rows
```

Every shard count reports `Rejected`, and every shard count above 1 has committed rows anyway. A
client that retries on failure double-inserts. Cause: `Route::Split` in `src/shard/mod.rs:1326`
loops `execute_one` per sub-batch and reports only the last failure.

**D7 · A `FOREIGN KEY` to an existing parent is rejected.**

```
INSERT INTO par (id,n) VALUES (1,'p');  INSERT INTO ch (id,pid) VALUES (7,1)
  N=1  CHANGED 1 → 1 row
  N=3  REJECTED FOREIGN KEY constraint failed → 0 rows
  N=4  REJECTED FOREIGN KEY constraint failed → 0 rows
```

`foreign_keys` is ON (`src/storage/pragma.rs:96`) and the check runs on the child's shard, which
does not hold the parent row. The failure direction is at least loud rather than silent — a valid
write is refused rather than an invalid one accepted — but it is still a divergence.

### Already aligned — do not touch

Verified identical at N=1, N=3 and N=4. This list matters as much as the divergence list, because
it bounds the work:

- Every planner refusal: `lower(name)`, `length()`, `abs()`, CTEs, `GROUP_CONCAT`,
  `COUNT(DISTINCT …)`, correlated subqueries. Refused identically everywhere.
- All 15 aggregate and `GROUP BY` shapes tested, including `AVG`, `HAVING`, and grouping by a
  scalar expression.
- `ORDER BY … LIMIT` **on a unique column**, `OFFSET`, `DISTINCT`, `LIKE`, `IN`, co-located
  `JOIN`, `UNION`, derived tables, `EXISTS`. (Ordering on a non-unique column is D8.)
- `CHECK` constraints (row-local, so they shard cleanly).
- `UPDATE` / `DELETE` with no key predicate (broadcast).
- `INSERT … ON CONFLICT DO UPDATE`, `AUTOINCREMENT`, `last_insert_rowid`.
- Composite-`PRIMARY KEY` tables, and the refusal of `BEGIN`/`COMMIT`.
- DB-assigned identity (`INSERT INTO t (name) VALUES (…)`) is refused at *every* N — a serious
  usability problem, but not an alignment problem. It is tracked below under "adjacent".

## Alignment mechanisms

Each divergence gets exactly one mechanism. Choosing the mechanism is the design decision; the
implementation follows from it.

| | Divergence | Mechanism | Direction |
|---|---|---|---|
| D1 | `FROM`-less `SELECT` | fix the router | N>1 rises to N=1 |
| D2 | window functions | evaluate on the coordinator | N>1 rises to N=1 |
| D3 | 2-arg `min`/`max` | fix the classifier | both rise to SQLite |
| D4 | unordered `LIMIT` | impose a deterministic total order | both rise to a defined answer |
| D8 | tied `ORDER BY` | impose a deterministic total order | both rise to a defined answer |
| D5 | non-key `UNIQUE` | declare at `CREATE TABLE` | both obey the declaration |
| D7 | cross-shard `FK` | declare at `CREATE TABLE` | both obey the declaration |
| D6 | multi-row `INSERT` atomicity | two-phase commit | N>1 rises to N=1 |

D6 is settled: see [cross-shard-atomicity-plan.md](cross-shard-atomicity-plan.md). It is no longer
an open question, and the same machinery covers `Route::All` broadcasts and DDL, which have the
identical defect.

## Strict mode — no implicit choices — **implemented**

Alignment removes the divergences. It does not remove the **guesses** — the places where shardlite
picks something the SQL did not specify. Strict mode is where a developer opts out of being
guessed for.

> **Strict mode refuses any statement where shardlite would otherwise choose on the user's behalf.**

One criterion, applied uniformly. It is not a semantics switch and not a compatibility level: a
statement accepted under strict returns byte-identical results to the same statement under default.
Strict only shrinks the accepted set.

| Guess made by default | Strict requires |
|---|---|
| `LIMIT` with no `ORDER BY` → ordered by shard key (D4) | an explicit `ORDER BY` |
| `ORDER BY` with ties → shard key appended (D8) | an `ORDER BY` that is already a total order |
| `CREATE TABLE` with no class → `sharded` | an explicit `sharded` / `global` / `reference` |
| single-column `PRIMARY KEY` silently adopted as the shard key (`route.rs:52`) | the shard key named explicitly |
| absent identity → a server-generated key | the key supplied by the client |
| a plan that falls back to coordinator materialisation (`Plan::Central`) | a query that pushes down, or an explicit opt-in |

The last row is the one worth having beyond correctness: a query that silently materialises on the
coordinator works fine on a laptop with 40 rows and falls over in production. Strict surfaces that
at development time, which is exactly the class of problem no test catches.

Why this is a mode and not simply the default: every row above is a real convenience, and several
are what make an ORM work at all. Forcing every user to declare a table class and name a shard key
before their first `INSERT` would be a worse product. Strict is for the developer who wants the
guesses to be errors.

**Default it to on for new databases.** Recorded in the manifest at `shardlite init --strict`,
fixed for the life of the database, never a runtime toggle — a per-connection or per-session switch
would make acceptance vary within one database, which the rule does forbid. We are pre-1.0 with no
compatibility burden, so the aggressive default costs nothing and teaches the model early.

**Done means:** every row in the table above has a test asserting refusal under strict and
acceptance under default; and a corpus run asserts that every statement accepted under *both* modes
returns identical results — strict never changes an answer.

## Phase 0 · The differential suite — **implemented**

Landed as [`tests/alignment.rs`](../tests/alignment.rs): 23 tests over a fixed corpus at N=1, 3, 4
and 8, asserting byte-identical results and identical refusals.

Two lessons from building it, both worth keeping:

- **A vacuous test is worse than no test.** `a_global_table_accepts_db_assigned_keys` initially
  asserted only alignment, and passed with the fix removed — because without global routing the
  insert is *refused* at every shard count, which is aligned and useless. It now asserts the rows.
  Every guard here was checked by removing its fix and confirming the test fails.
- **The corpus missed D8** because every `ORDER BY` in it sorted on a unique column. Ties are where
  ordering bugs live.

**This landed first, and it is the definition of done for every other phase.**

Promote the throwaway probe into `tests/alignment.rs`: a fixed corpus of statements, seeded
identically, executed at N=1, N=3 and N=4, asserting that every statement produces byte-identical
results — or identical refusals — across all three.

Design requirements learned from building the probe:

- **Low-cardinality seed data.** `grp = n % 10` and five repeating names. With unique values per
  row, cross-shard combine bugs pass vacuously.
- **Include a non-power-of-two count.** N=3 exercises the linear-hashing split pointer, and it is
  what exposed D5's data-dependence — N=1 and N=3 agreed while N=4 differed.
- **Normalise row order** for statements without `ORDER BY`, and cover ordering separately.
- **Assert on refusals too**, by message. "Refused at both" is a passing result under this
  contract; "refused at one" is a failure.
- **Every divergence below gets a test that fails today**, marked with its D-number, so the phases
  have a mechanical finish line rather than a judgement call.

Without this, alignment is a claim. With it, alignment is a build failure when someone breaks it —
which is the only form that survives future work on the planner.

**Done means:** `cargo test --test alignment` fails on exactly the seven divergences below and
passes on the 48 aligned behaviours.

## Phase 1 · Planner and router alignment — **implemented**

Pure bug fixes. No format change, no new concepts, no policy decisions. Each is independently
shippable.

### D1 · A `FROM`-less `SELECT` must not fan out — **implemented**

`src/query/route.rs` — `route_statement_with`, and the `Route::Passthrough` read arm in
`src/shard/mod.rs:1337`.

A `SELECT` with no `FROM` clause references no shard data. Route it to a single shard rather than
to the fan-out. `table_of` already returns `None` for this shape; the gap is that `None` currently
means "broadcast" instead of "nothing to broadcast over".

Highest value-per-line item in the document. `SELECT 1` returning four rows breaks connection
pools and ORM liveness checks before a user writes a single query of their own.

**Done means:** `SELECT 1`, `SELECT 1+1`, `SELECT date(…)`, `SELECT random()` and
`SELECT (SELECT COUNT(*) FROM t)` each return exactly one row at N=1, 3 and 4.

### D2 · Window functions evaluate on the coordinator — **implemented**

`src/query/plan.rs`.

Any `OVER` clause — in the projection or in `ORDER BY` — must route to `Plan::Central`:
materialise the sources on the coordinator and evaluate there. Refuse when the sources exceed the
existing source cap, naming the cap.

This is the most severe item on the list. It returns a plausible wrong answer, which violates both
the planner's own module contract ("anything this planner cannot answer *correctly* is refused,
never approximated") and the project's governing rule of refusing rather than approximating.

**Done means:** `row_number() OVER (ORDER BY id)` over 40 rows yields `1..40` exactly once each at
N=1, 3 and 4; `rank()` likewise; an over-cap source refuses identically at all three.

### D3 · Arity before name in aggregate classification — **implemented**

`src/query/plan.rs` — `is_aggregate_shaped`, `classify_aggregate`, `aggregate_of`.

Two changes in one place:

1. Check arity before treating `min`/`max` as aggregates. One argument is the aggregate; two or
   more is SQLite's scalar `min`/`max`.
2. Replace the `is_aggregate_shaped` heuristic — "arity ≤ 1 and the argument is a bare identifier"
   — with an explicit set of SQLite aggregate names. Anything outside the set is a scalar.

The second change is not required for alignment, since scalar functions are refused equally at
every N. Include it anyway: it is the same function, the same edit, and it removes the most common
false refusal in the product (`lower(name)` is rejected as "this aggregate" today, while
`substr(name,1,2)` and `WHERE lower(name)=…` both work — the heuristic is what makes that
incoherent).

**Done means:** `min(amt,5)` returns one row per input row at all three counts; ~25 scalar
functions in projection position return identical rows at all three; `GROUP_CONCAT` and friends
still refuse, by name rather than by shape.

### D4 + D8 · A deterministic total order — **implemented**

`src/query/plan.rs` — sort-key construction for `Concat` and `PostProcess`.

Both divergences are the same defect: no total order, so each shard falls back to its local scan
order and the merge inherits it. One rule fixes both:

**Every fan-out read is ordered by a total order. The shard key is appended to the sort key as the
final tiebreak; a query with no `ORDER BY` is ordered by the shard key alone.**

- `SELECT id,grp FROM t ORDER BY grp DESC LIMIT 4` becomes `ORDER BY grp DESC, id` — the same four
  rows at every shard count.
- `SELECT id FROM t LIMIT 3` becomes `ORDER BY id LIMIT 3` — `1,2,3` everywhere, which is also what
  N=1 returns today.

The cost is small and bounded. Where the shard key is an `INTEGER PRIMARY KEY` it *is* the rowid,
so the added sort term matches the natural scan order and costs nothing. Where it is not, each
shard sorts at most `LIMIT` rows before the merge. The merge already sorts.

Two honest consequences. This makes shardlite's `LIMIT` **stricter than SQLite's** — defined where
the standard permits anything — which is a deliberate trade under the one-behaviour rule. And for a
table whose shard key is not the rowid, N=1's result changes, because it currently returns rowid
order. That is the rule working as intended: one defined answer everywhere beats a familiar
accident at one shard count.

Considered and rejected: documenting it as undefined (the conditional behaviour the rule forbids),
and refusing `LIMIT` without `ORDER BY` (refuses valid SQL for a problem that has a cheap fix).

**Done means:** the D4 and D8 statements return byte-identical results at N=1, 3, 4 and 16;
`ORDER BY` on a unique column is unchanged; and a `LIMIT` over a tie returns the same *set* of rows
at every count.

## Phase 2 · Constraint alignment by declaration — **implemented**

D5 and D7 cannot be fixed by raising N>1 — a `UNIQUE` index cannot span independent SQLite files.
They are aligned instead by removing shard count from the decision entirely.

### Table classes

`src/query/plan.rs` (`ShardKeys`), `src/shard/mod.rs`, `src/query/route.rs`.

`ShardKeys` is `HashMap<String, String>` — table to shard-key column. Widen it to a catalog
carrying a class:

- **`sharded`** — today's behaviour. Constraints are shard-local at every N, including 1.
- **`global`** — lives on shard 0, read from shard 0 without fan-out. Full SQLite semantics
  permanently: `UNIQUE`, `FOREIGN KEY`, real transactions, consistent snapshots — identical at
  N=1 and N=100.
- **`reference`** — replicated to every shard by the DDL broadcast that already exists, so a
  `sharded ⋈ reference` join stays co-located.

Same decomposition as Citus (distributed / reference / local) and Vitess (sharded / unsharded).

### The DDL gate

At `CREATE TABLE` on a `sharded` table, refuse — **regardless of the current shard count** — any
constraint that cannot hold at N>1:

- `UNIQUE`, or a unique index, on a column set not containing the shard key.
- A `FOREIGN KEY` whose parent is a different sharded table, or a sharded → `global` reference
  (unenforceable from a shard).

The message names both remedies: include the shard key in the constraint, or declare the table
`global`.

This is Citus's rule — `create_distributed_table` refuses rather than letting a later rebalance
quietly stop enforcing a constraint. Applying it at N=1 is what makes D5 and D7 alignment fixes
rather than documentation. A schema that passes the gate behaves identically at every shard count,
because the constraints it kept are the ones that shard.

**Done means:** the D5 and D7 scenarios refuse at `CREATE TABLE` at N=1, N=3 and N=4 with
identical messages; the same schema declared `global` enforces `UNIQUE` and `FK` identically at
all three; the class survives a restart and a linear split.

## Phase 3 · Write atomicity — **implemented**

### D6 · Multi-row `INSERT` atomicity

**Settled: two-phase commit.** Full design, blast radius and phasing in
[cross-shard-atomicity-plan.md](cross-shard-atomicity-plan.md).

Two alternatives were considered and rejected against the one-behaviour rule. *Reporting partial
application honestly* leaves N=1 atomic and N>1 partial — it makes the divergence visible rather
than removing it, and a client told "failed" while rows exist is the confusion this contract
exists to prevent. *Refusing a spanning multi-row `INSERT`* is data-dependent: whether five rows
span shards depends on their values, so the same statement would sometimes work and sometimes
refuse at the same shard count — the identical defect as D5's luck-based `UNIQUE`.

Under the same rule, the machinery applies to **every** multi-shard write, not just `Route::Split`:
`Route::All` broadcasts and DDL have the identical partial-application defect. DDL is measurably
affected today — a `CREATE TABLE` that fails on shard 3 of 4 leaves shards 0–2 with the new schema
and no automatic repair, detected by `schema_agreement()` but not prevented.

**Done means:** the D6 scenario reports `Rejected` with zero rows applied at every shard count; a
partially-failing broadcast `UPDATE` and a partially-failing DDL likewise leave no trace.

## Adjacent, and deliberately not in this plan

Real problems, measured, and **not alignment problems** — they behave identically at every shard
count. Listing them so they are not lost, and so the alignment work is not quietly widened:

- **DB-assigned identity.** `INSERT INTO t (name) VALUES ('x')` is refused at every N, blocking
  every ORM. The largest usability gap in the product. Depends on table classes (Phase 2), so it
  is naturally the first thing after this plan.
- **A missing table returns an empty result.** `SELECT * FROM nope` → `rows=0 cols=[]` at every N.
  Silently wrong, but consistently so.
- **MySQL DDL builds the wrong table.** `INT AUTO_INCREMENT PRIMARY KEY` is accepted and is not a
  rowid alias; `SERIAL` gets NUMERIC affinity; `DECIMAL(10,2)` is a float. Accepted identically
  everywhere.
- **Fan-out reads are not a consistent snapshot**, at any N>1. Architectural, documented, and not
  closable without global timestamps.

## What this unlocks

MySQL and Postgres compatibility is the next goal, not this one — and this plan is the right
prerequisite for it rather than a detour. A dialect layer that normalises MySQL SQL into canonical
SQLite is only worth building on a surface where the SQLite semantics are themselves stable across
shard counts; otherwise every dialect feature inherits a shard-count caveat and the compatibility
matrix becomes two-dimensional.

Two pieces of this plan carry over directly: the differential suite (Phase 0) extends to a
dialect-conformance suite by swapping the corpus, and the single-normalization-pass boundary
implied by D1's router fix is where dialect rewriting will eventually sit.

## Ordering

| Phase | Work | Why here |
|---|---|---|
| 0 | Differential suite | Defines done for everything else; converts alignment from a claim into a build failure. |
| 1 | D1, D2, D3, D4, D8 | Pure bug fixes, no decisions. D2 is the most severe (wrong answers); D1 is the highest value per line. |
| 2 | Table classes + DDL gate → D5, D7 | The structural half. Needs a format change and a class concept, so it follows the cheap fixes. |
| 2b | Strict mode | Each strict refusal corresponds to a guess introduced in Phase 1 or 2, so it lands with them rather than after. The manifest flag itself is trivial; the value is that no guess ships without its strict refusal. |
| 3 | D6 → two-phase commit | Settled but by far the largest piece; it has its own plan and its own phasing. |

Phase 1 alone removes five of the eight divergences and two of the three wrong answers, and needs
no format change, no new concepts and no decisions from you.
