//! Deciding how — or whether — a query can be answered across shards.
//!
//! # The governing rule
//!
//! Anything this planner cannot answer *correctly* is **refused**, never approximated.
//! Shards are separate SQLite databases with no cross-shard transaction and no shared
//! snapshot, so a query that needs data to meet in one place cannot be answered by fanning
//! out and stitching results together. Returning a plausible-looking wrong number would be
//! far worse than an error, because nothing downstream could tell the difference.
//!
//! # What can be merged, and why
//!
//! | Shape | Merge |
//! |---|---|
//! | plain `SELECT` | concatenate rows |
//! | `ORDER BY` (+ `LIMIT`) | per-shard sort, then k-way merge, then truncate |
//! | `COUNT(*)`, `COUNT(c)` | sum the counts |
//! | `SUM(c)` | sum the sums |
//! | `MIN(c)` / `MAX(c)` | min of minima / max of maxima |
//! | `GROUP BY g, …` (+ `HAVING`) | two-phase: per-shard partial groups, re-aggregate, then filter |
//! | `AVG(c)` (bare or grouped) | decompose to `SUM(c)`/`COUNT(c)`, divide at the coordinator |
//! | `DISTINCT` | per-shard dedup, then dedup the union on the coordinator |
//! | `OFFSET` (± `ORDER BY`) | each shard ships its top `offset+limit`; skip then take |
//! | `UNION` / `INTERSECT` / `EXCEPT` (any mix) | fan out each branch, then combine on the coordinator |
//! | scalar / `IN` / `EXISTS` subquery | evaluate it globally, substitute its result, re-plan the outer |
//! | co-located `JOIN` | join each shard's local rows; concatenate (see [`ShardKeys`]) |
//! | any other `JOIN` / derived table (`FROM (SELECT …)`) | materialise each source, run the query on the coordinator (see [`Central`]) |
//! | `COUNT/SUM/AVG/MIN/MAX(DISTINCT c)` | materialise the (pre-`DISTINCT`'d) column centrally, compute once (see [`Central`]) |
//!
//! `COUNT`/`SUM`/`MIN`/`MAX` are *associative* — combining per-shard partials reproduces the
//! global answer. `AVG` is not, but it *decomposes* into two that are, so it is answered by
//! carrying `SUM` and `COUNT` per shard and dividing once (bare, this is a group over the whole
//! table). `GROUP BY` is the same idea per group (see [`Grouped`]), and `HAVING` filters the
//! complete groups on the coordinator; `DISTINCT` and `OFFSET` are coordinator-side reshapes of the
//! concatenated rows (see [`PostProcess`]); a set operation evaluates each branch as its own
//! fan-out and combines the results (see [`SetOp`]); a scalar / `IN` / `EXISTS` subquery is
//! evaluated on its own and its result (a value, a value list, or a boolean) substituted in before
//! the outer query is planned (see [`substitute_subqueries`]).
//!
//! A join that is *not* co-located, a derived table in `FROM`, and a `DISTINCT` aggregate cannot be
//! pushed down — but rather than refuse them, the source rows are materialised globally and the
//! query is run once on the coordinator (see [`Central`]). A `DISTINCT` aggregate materialises just
//! the referenced column, pre-`DISTINCT`'d per shard when every aggregate is insensitive to row
//! multiplicity, so a `count(DISTINCT c)` over a large table ships distinct values, not rows. This
//! is bounded: a source larger than the configured cap is refused rather than pulled into memory.
//!
//! # What is refused, and why each one
//!
//! - **`GROUP_CONCAT`, and any aggregate not listed above** — order-dependent (its cross-shard
//!   concatenation order is undefined) or simply not a shape the coordinator can compute.
//! - **A selected column that is neither grouped nor aggregated** — a bare column is a partial,
//!   arbitrary row per shard.
//! - **`ANY` / `ALL` subqueries** and a **correlated** subquery (it names the outer row and cannot
//!   run on its own).
//! - **A source larger than the materialisation cap** — central execution is bounded, so a `FROM`
//!   source that exceeds the cap is refused rather than buffered without limit.
//! - **CTEs** — can hide any of the above.
//!
//! # What it cannot fix
//!
//! Even an accepted query is **not a consistent snapshot**. Each shard is read at its own
//! moment, so a fan-out can observe a write on one shard and miss a concurrent one on
//! another. There is no cross-shard atomicity in this design and there never will be, so
//! this is a property of the answer, not a gap to close later.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Distinct, DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, GroupByWithModifier, JoinConstraint, JoinOperator, LimitClause,
    OrderByKind, Query, Select, SelectItem, SetExpr, SetOperator, SetQuantifier,
    Statement as SqlStatement, TableFactor, UnaryOperator, Value as SqlValue, ValueWithSpan,
    visit_expressions, visit_expressions_mut,
};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

/// How to combine one aggregate column across shards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Combine {
    Sum,
    Min,
    Max,
}

/// A sort key, as an index into the output columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SortKey {
    pub column: usize,
    pub descending: bool,
    /// SQLite orders NULLs first ascending, last descending, unless told otherwise.
    pub nulls_first: bool,
}

/// How a query should be executed across shards.
#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    /// Run on one shard only; nothing to merge.
    SingleShard,
    /// Fan out and concatenate.
    Concat { limit: Option<usize> },
    /// Fan out, then k-way merge on `keys`.
    Merge {
        keys: Vec<SortKey>,
        limit: Option<usize>,
    },
    /// Fan out, then combine a single aggregate column.
    Aggregate { combine: Combine },
    /// Fan out a rewritten partial-aggregation query, then re-aggregate by group key.
    Grouped(Box<Grouped>),
    /// Fan out a rewritten query, then dedup / sort / skip / truncate on the coordinator. Covers
    /// `DISTINCT` and `OFFSET`.
    PostProcess(Box<PostProcess>),
    /// Evaluate each branch of a set operation as its own fan-out, then combine on the coordinator.
    /// Covers `UNION` / `UNION ALL` / `INTERSECT` / `EXCEPT`, in any mix.
    SetOp(Box<SetOp>),
    /// The query contains subqueries (scalar `(SELECT …)`, `IN (SELECT …)`, or `EXISTS (…)`):
    /// evaluate each globally, substitute its result, then re-plan and fan out the substituted
    /// (subquery-free) query (see [`substitute_subqueries`]).
    Subqueries,
    /// The `FROM` cannot be pushed to each shard (a derived table, or a non-co-located / cross
    /// join): materialise each source fully on the coordinator and run the query there. Correct but
    /// memory-heavy — the fallback of last resort, cap-gated at execution.
    Central(Box<Central>),
}

/// A human-facing summary of how a query runs across shards, for EXPLAIN. `heavy` marks a plan that
/// pulls matching rows to the coordinator (central execution) — correct, but memory-heavy and worth
/// surfacing so an operator can restructure the query or target a single shard.
#[derive(Debug, Clone)]
pub struct PlanDescription {
    pub strategy: &'static str,
    pub note: &'static str,
    pub heavy: bool,
}

impl Plan {
    /// Describe the plan's cross-shard strategy without running it.
    pub fn describe(&self) -> PlanDescription {
        let (strategy, note, heavy) = match self {
            Plan::SingleShard => (
                "single shard",
                "Runs on one shard, routed by key — no fan-out.",
                false,
            ),
            Plan::Concat { .. } => (
                "fan-out · concat",
                "Rows are gathered from every shard and concatenated.",
                false,
            ),
            Plan::Merge { .. } => (
                "fan-out · ordered merge",
                "Each shard sorts locally; the coordinator k-way merges the streams.",
                false,
            ),
            Plan::Aggregate { .. } => (
                "fan-out · combined aggregate",
                "Each shard computes a partial aggregate; the coordinator combines the partials.",
                false,
            ),
            Plan::Grouped(_) => (
                "fan-out · grouped",
                "Each shard produces partial groups; the coordinator re-aggregates by group key.",
                false,
            ),
            Plan::PostProcess(_) => (
                "fan-out · post-processed",
                "Fan-out, then dedup / sort / offset on the coordinator (DISTINCT / OFFSET).",
                false,
            ),
            Plan::SetOp(_) => (
                "fan-out · set operation",
                "Each branch fans out; the coordinator combines them (UNION / INTERSECT / EXCEPT).",
                false,
            ),
            Plan::Subqueries => (
                "subquery fan-out",
                "Subqueries are evaluated globally and substituted, then the query fans out.",
                false,
            ),
            Plan::Central(_) => (
                "central execution",
                "Matching rows are pulled to the coordinator and the query runs there — correct \
                 but memory-heavy, and heavier than a pushed-down aggregate. Consider restructuring \
                 the query or targeting a single shard.",
                true,
            ),
        };
        PlanDescription {
            strategy,
            note,
            heavy,
        }
    }
}

/// A query answered by materialising its `FROM` sources on the coordinator and running it against
/// an in-memory SQLite. Each source is fanned out on its own and loaded into a table; `central_sql`
/// then references those tables by name.
#[derive(Debug, Clone, PartialEq)]
pub struct Central {
    /// The tables to materialise, each a `(coordinator table name, fan-out SQL)`.
    pub sources: Vec<CentralSource>,
    /// The query to run on the coordinator, with each source referenced by its table name.
    pub central_sql: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CentralSource {
    pub table: String,
    pub sql: String,
}

/// A coordinator-side reshape of concatenated shard rows: optionally dedup them, sort them, skip
/// `offset`, then keep `limit`. The per-shard `shard_sql` is the caller's query with `OFFSET`
/// removed and any `LIMIT` widened to `offset + limit` so each shard ships enough rows.
#[derive(Debug, Clone, PartialEq)]
pub struct PostProcess {
    pub shard_sql: String,
    /// Collapse rows equal under SQLite value comparison — `DISTINCT` and a distinct `UNION`.
    pub distinct: bool,
    /// Sort keys over the output columns (empty when there is no `ORDER BY`).
    pub order_by: Vec<SortKey>,
    /// Rows to skip after sorting — a global `OFFSET`.
    pub offset: usize,
    /// Rows to keep after skipping.
    pub limit: Option<usize>,
}

/// A set operation (`UNION` / `INTERSECT` / `EXCEPT`, in any mix), evaluated by fanning out each
/// branch as its own full cross-shard query and then combining the results. Because each branch is
/// evaluated globally first, a branch may be anything fan-out-safe — including an aggregate or a
/// grouped query — and the combine is correct across shards.
#[derive(Debug, Clone, PartialEq)]
pub struct SetOp {
    /// The SQL of each leaf `SELECT`, left to right; `tree` refers to these by index.
    pub branches: Vec<String>,
    /// How to combine the branch results.
    pub tree: SetTree,
    /// Sort keys over the output columns (the first branch's), empty when there is no `ORDER BY`.
    pub order_by: Vec<SortKey>,
    pub offset: usize,
    pub limit: Option<usize>,
}

/// The shape of a set-operation expression, over branch indices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetTree {
    Leaf(usize),
    Op {
        op: SetKind,
        /// `ALL` keeps duplicates; otherwise the operation is distinct.
        all: bool,
        left: Box<SetTree>,
        right: Box<SetTree>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetKind {
    Union,
    Intersect,
    Except,
}

/// A two-phase `GROUP BY`. Each shard runs `shard_sql` to produce its *partial* groups (SQLite
/// groups correctly within a shard), and the coordinator re-aggregates those partials by the
/// grouping columns to reproduce the global answer.
#[derive(Debug, Clone, PartialEq)]
pub struct Grouped {
    /// The rewritten query every shard runs: the grouping columns plus decomposed partial
    /// aggregates, with `ORDER BY` / `LIMIT` / `HAVING` stripped — those are coordinator-side.
    pub shard_sql: String,
    /// Positions of the grouping columns within a partial row; together they are the group key.
    pub group_cols: Vec<usize>,
    /// One entry per output column. The first `visible_count` are the user's selected columns, in
    /// order; any beyond that are hidden columns a `HAVING` predicate needs, dropped from the
    /// final result.
    pub outputs: Vec<OutputCol>,
    /// How many of `outputs` are visible (the user's projection); the rest are HAVING scratch.
    pub visible_count: usize,
    /// The `HAVING` predicate, evaluated per group after re-aggregation.
    pub having: Option<HavingExpr>,
    /// Final column headers (length `visible_count`).
    pub output_names: Vec<String>,
    /// Sort keys over the *visible* output columns (empty when there is no `ORDER BY`).
    pub order_by: Vec<SortKey>,
    /// Groups to skip after ordering — a global `OFFSET`.
    pub offset: usize,
    /// Applied at the coordinator after re-aggregation — never pushed to a shard, where a
    /// per-shard `LIMIT` would drop groups whose rows are spread across shards.
    pub limit: Option<usize>,
}

/// How one final output column is produced from a group's partial rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputCol {
    /// A grouping column: take the group-key value at this partial position.
    Group(usize),
    /// Fold one partial aggregate column across the group with `combine_values`.
    Combine { kind: Combine, col: usize },
    /// `SUM(partial sums) / SUM(partial counts)`; `NULL` when the total count is zero.
    Avg { sum_col: usize, count_col: usize },
}

/// A parsed `HAVING` predicate, evaluated per group on the coordinator against the group's
/// computed output row. Aggregates and columns that `HAVING` references but the projection does
/// not are added to the group's outputs as *hidden* columns (dropped from the final result);
/// `HavingValue::Column` indexes into that full row.
#[derive(Debug, Clone, PartialEq)]
pub enum HavingExpr {
    And(Box<HavingExpr>, Box<HavingExpr>),
    Or(Box<HavingExpr>, Box<HavingExpr>),
    Not(Box<HavingExpr>),
    Compare {
        left: HavingValue,
        op: CompareOp,
        right: HavingValue,
    },
    IsNull {
        value: HavingValue,
        negated: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum HavingValue {
    /// A value in the group's computed output row, at this index.
    Column(usize),
    /// A constant.
    Literal(crate::storage::exec::Value),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// How a projected aggregate decomposes across shards.
enum AggChoice {
    /// One partial column, folded by `Combine` (COUNT/SUM/MIN/MAX).
    Simple(Combine),
    /// Two partial columns — `SUM(arg)` and `COUNT(arg)` — divided at the coordinator.
    Avg,
}

/// Why a query cannot be answered across shards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsupported {
    pub what: &'static str,
    pub why: &'static str,
}

impl std::fmt::Display for Unsupported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} cannot be answered across shards: {}. Target a single shard instead, or \
             restructure the query.",
            self.what, self.why
        )
    }
}

/// The app's declaration of how tables are routed: table name → its shard-key column, both
/// lowercased. A join on two tables' shard keys keeps every matching pair on one shard (they route
/// to the same shard), so it is *co-located* and can be pushed to each shard and concatenated.
///
/// This is a **trust assertion**, not something shardlite can verify: the app is responsible for
/// having routed those columns consistently. Declaring co-partitioning for tables that are not
/// actually co-located yields a join that silently misses cross-shard matches — the one place the
/// planner trusts rather than proves.
pub type ShardKeys = std::collections::HashMap<String, String>;

/// Decide how `sql` can be run across shards, with no co-partitioning declared — joins are refused.
pub fn plan(sql: &str) -> std::result::Result<Plan, Unsupported> {
    plan_with(sql, &ShardKeys::new())
}

/// Decide how `sql` can be run across shards. `shard_keys` declares which tables are co-partitioned
/// (see [`ShardKeys`]); a join on their shard keys is allowed, every other join refused.
pub fn plan_with(sql: &str, shard_keys: &ShardKeys) -> std::result::Result<Plan, Unsupported> {
    let dialect = SQLiteDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).map_err(|_| Unsupported {
        what: "this statement",
        why: "it could not be parsed, and a fan-out must understand a query before it can \
              merge the results safely",
    })?;

    if statements.len() != 1 {
        return Err(Unsupported {
            what: "multiple statements",
            why: "each would need its own plan",
        });
    }

    let query = match statements.pop() {
        Some(SqlStatement::Query(q)) => q,
        _ => {
            return Err(Unsupported {
                what: "a non-query statement",
                why: "only SELECT is fanned out; writes are routed to a single shard",
            });
        }
    };

    plan_query(&query, shard_keys)
}

fn plan_query(query: &Query, shard_keys: &ShardKeys) -> std::result::Result<Plan, Unsupported> {
    if query.with.is_some() {
        return Err(Unsupported {
            what: "a common table expression",
            why: "a CTE can contain a join, grouping or aggregate that a fan-out cannot \
                  reassemble",
        });
    }
    if query.fetch.is_some() {
        return Err(Unsupported {
            what: "FETCH",
            why: "it has the same problem as OFFSET",
        });
    }

    // A scalar `(SELECT …)`, `IN (SELECT …)` or `EXISTS (…)` subquery is evaluated globally on its
    // own and its result (a value, a value list, or a boolean) substituted in before the outer
    // query is planned (two-pass). `ANY` / `ALL` subqueries are refused. Either way a subquery is
    // never pushed verbatim to a shard, where it would run against that shard alone.
    match classify_subqueries(query) {
        SubqueryClass::None => {}
        SubqueryClass::Substitutable => return Ok(Plan::Subqueries),
        SubqueryClass::Unsupported => {
            return Err(Unsupported {
                what: "an ANY or ALL subquery",
                why: "only scalar, IN and EXISTS subqueries can be evaluated separately and \
                      substituted",
            });
        }
    }

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        // UNION / UNION ALL fan out; INTERSECT / EXCEPT do not (see plan_set_op). Each branch is
        // re-planned on its own fan-out, so a co-located join inside a branch is validated there.
        SetExpr::SetOperation { .. } => return plan_set_op(query),
        _ => {
            return Err(Unsupported {
                what: "this query form",
                why: "only a plain SELECT can be fanned out",
            });
        }
    };

    // If the FROM cannot be pushed to each shard — a derived table, or a join that is not
    // co-located — fall back to central execution: materialise each source on the coordinator and
    // run the whole query there. Otherwise the FROM is a single table or a co-located join and the
    // pushdown planning below applies.
    if check_join(select, shard_keys).is_err() {
        return plan_central(query);
    }

    // DISTINCT dedups on the coordinator; it is no longer refused.
    if select.distinct.is_some() {
        return plan_distinct(query, select, shard_keys);
    }
    // A grouped query takes the dedicated two-phase path; a plain SELECT falls through.
    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) if !exprs.is_empty() => {
            return plan_grouped(query, select, exprs, modifiers, shard_keys);
        }
        GroupByExpr::All(_) => {
            return Err(Unsupported {
                what: "GROUP BY ALL",
                why: "the grouping columns cannot be resolved to a fan-out-safe plan",
            });
        }
        _ => {}
    }
    if select.having.is_some() {
        return Err(Unsupported {
            what: "HAVING",
            why: "it filters groups, which are partial per shard",
        });
    }
    // The FROM is already known pushable (a single table or a co-located join, checked above), so
    // the join — if any — rides along in the shard SQL and WHERE / aggregates compose on top.

    // A bare aggregate (no GROUP BY) is a group over the whole table.
    if let Some(plan) = plan_bare_aggregate(query, select, shard_keys)? {
        return Ok(plan);
    }

    let (limit, offset) = limit_and_offset(query)?;
    let keys = resolve_sort_keys(&query.order_by, &select.projection)?;

    // OFFSET is skipped on the coordinator; each shard ships its top (offset+limit) rows. Without
    // an ORDER BY the rows skipped and returned are unspecified — exactly as for a bare LIMIT — but
    // still a valid answer, so it is allowed rather than refused.
    if offset > 0 {
        return Ok(Plan::PostProcess(Box::new(PostProcess {
            shard_sql: shard_sql_with_limit(query, limit.map(|n| n + offset)),
            distinct: false,
            order_by: keys,
            offset,
            limit,
        })));
    }

    if keys.is_empty() {
        Ok(Plan::Concat { limit })
    } else {
        Ok(Plan::Merge { keys, limit })
    }
}

/// Plan a bare aggregate (aggregates in the projection, no `GROUP BY`) as a group over the whole
/// table. A single associative aggregate takes the fast [`Plan::Aggregate`] path; `AVG`, several
/// aggregates, or an ordered aggregate go through the grouped machinery with an empty group key.
/// Returns `Ok(None)` when the projection has no aggregate.
fn plan_bare_aggregate(
    query: &Query,
    select: &Select,
    shard_keys: &ShardKeys,
) -> std::result::Result<Option<Plan>, Unsupported> {
    let mut has_aggregate = false;
    for item in &select.projection {
        let expr = match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
            _ => continue,
        };
        // classify_aggregate refuses GROUP_CONCAT and unknown aggregates via `?`.
        if let Expr::Function(f) = expr
            && classify_aggregate(f)?.is_some()
        {
            has_aggregate = true;
        }
    }
    if !has_aggregate {
        return Ok(None);
    }

    // Fast path: one associative, non-DISTINCT aggregate, no ORDER BY / LIMIT. A DISTINCT aggregate
    // must NOT take this path — summing per-shard `count(DISTINCT c)` double-counts a value that
    // appears on more than one shard — so it falls through to plan_grouped, which routes it to
    // central execution.
    if select.projection.len() == 1
        && query.order_by.is_none()
        && query.limit_clause.is_none()
        && !item_is_distinct_aggregate(&select.projection[0])
        && let Some(combine) = aggregate_of(&select.projection[0]).ok().flatten()
    {
        return Ok(Some(Plan::Aggregate { combine }));
    }

    // Everything else — AVG, several aggregates, ordering — is a group over the whole table.
    Ok(Some(plan_grouped(query, select, &[], &[], shard_keys)?))
}

/// Plan a `SELECT DISTINCT`: each shard dedups locally (shrinking transfer), and the coordinator
/// dedups the union of the shards' rows. It is `GROUP BY` on every selected column with no
/// aggregate — the same dedup-on-merge primitive.
fn plan_distinct(
    query: &Query,
    select: &Select,
    shard_keys: &ShardKeys,
) -> std::result::Result<Plan, Unsupported> {
    if let Some(Distinct::On(_)) = &select.distinct {
        return Err(Unsupported {
            what: "DISTINCT ON",
            why: "it keeps an arbitrary row per group, which a fan-out cannot reproduce",
        });
    }

    // A DISTINCT over aggregates is redundant — an aggregate query yields at most one row per
    // group — so plan it as the underlying aggregate. The shard still runs the DISTINCT form
    // (harmless: `SELECT DISTINCT count(*)` per shard is just that shard's count).
    let has_aggregate = select
        .projection
        .iter()
        .any(|item| matches!(aggregate_of(item), Ok(Some(_)) | Err(_)));
    if has_aggregate {
        let mut without = query.clone();
        if let SetExpr::Select(s) = without.body.as_mut() {
            s.distinct = None;
        }
        return plan_query(&without, shard_keys);
    }

    require_plain_select(select, shard_keys)?;

    let (limit, offset) = limit_and_offset(query)?;
    let keys = resolve_sort_keys(&query.order_by, &select.projection)?;
    // A widened LIMIT is only safe to push when ordered (each shard's top-k contains the global
    // top-k); unordered, dedup the whole thing and limit at the coordinator.
    let push = if keys.is_empty() {
        None
    } else {
        limit.map(|n| n + offset)
    };
    Ok(Plan::PostProcess(Box::new(PostProcess {
        shard_sql: shard_sql_with_limit(query, push),
        distinct: true,
        order_by: keys,
        offset,
        limit,
    })))
}

/// Plan any set operation (`UNION` / `INTERSECT` / `EXCEPT`, in any mix) as multi-pass: each leaf
/// `SELECT` becomes its own branch to fan out independently, and `tree` records how to combine
/// them. Because each branch is evaluated globally before combining, a branch may itself be an
/// aggregate or grouped query — the per-shard fragmentation that makes a pushed-down set operation
/// wrong does not arise.
fn plan_set_op(query: &Query) -> std::result::Result<Plan, Unsupported> {
    let mut branches: Vec<String> = Vec::new();
    let mut leaf_selects: Vec<&Select> = Vec::new();
    let tree = build_set_tree(query.body.as_ref(), &mut branches, &mut leaf_selects)?;

    let first = *leaf_selects.first().ok_or(Unsupported {
        what: "an empty set operation",
        why: "there is nothing to combine",
    })?;

    let (limit, offset) = limit_and_offset(query)?;
    // An ORDER BY on a compound select resolves against the first branch's output columns.
    let order_by = resolve_sort_keys(&query.order_by, &first.projection)?;

    Ok(Plan::SetOp(Box::new(SetOp {
        branches,
        tree,
        order_by,
        offset,
        limit,
    })))
}

/// Walk a set-operation expression into a [`SetTree`] over branch indices, collecting each leaf's
/// SQL (for independent fan-out) and its `Select` (for output-column resolution).
fn build_set_tree<'a>(
    body: &'a SetExpr,
    branches: &mut Vec<String>,
    leaf_selects: &mut Vec<&'a Select>,
) -> std::result::Result<SetTree, Unsupported> {
    match body {
        SetExpr::Select(s) => {
            let index = branches.len();
            branches.push(s.to_string());
            leaf_selects.push(s);
            Ok(SetTree::Leaf(index))
        }
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            // `BY NAME` matches columns by name rather than position — different semantics, and
            // non-standard; refuse it rather than treat it as positional.
            if matches!(
                set_quantifier,
                SetQuantifier::ByName | SetQuantifier::AllByName | SetQuantifier::DistinctByName
            ) {
                return Err(Unsupported {
                    what: "a BY NAME set operation",
                    why: "columns are combined by position across shards, not by name",
                });
            }
            let kind = match op {
                SetOperator::Union => SetKind::Union,
                SetOperator::Intersect => SetKind::Intersect,
                SetOperator::Except | SetOperator::Minus => SetKind::Except,
            };
            let all = matches!(set_quantifier, SetQuantifier::All);
            let left = Box::new(build_set_tree(left, branches, leaf_selects)?);
            let right = Box::new(build_set_tree(right, branches, leaf_selects)?);
            Ok(SetTree::Op {
                op: kind,
                all,
                left,
                right,
            })
        }
        _ => Err(Unsupported {
            what: "this query form in a set operation",
            why: "only plain SELECT branches can be combined",
        }),
    }
}

/// A projection that produces plain rows: no aggregate, grouping, or `HAVING` (a co-located join
/// is allowed). Required of a `DISTINCT` query and of every `UNION` branch, where a per-shard
/// aggregate or grouping would be only a fragment of the whole.
fn require_plain_select(
    select: &Select,
    shard_keys: &ShardKeys,
) -> std::result::Result<(), Unsupported> {
    if !matches!(&select.group_by, GroupByExpr::Expressions(e, _) if e.is_empty()) {
        return Err(Unsupported {
            what: "GROUP BY combined with DISTINCT or a set operation",
            why: "the grouping is partial per shard, so it cannot be pushed down here",
        });
    }
    if select.having.is_some() {
        return Err(Unsupported {
            what: "HAVING here",
            why: "it filters groups that are partial per shard",
        });
    }
    check_join(select, shard_keys)?;
    for item in &select.projection {
        if aggregate_of(item)?.is_some() {
            return Err(Unsupported {
                what: "an aggregate with DISTINCT or a set operation",
                why: "a per-shard aggregate is only a fragment of the whole, so it cannot be \
                      combined here",
            });
        }
    }
    Ok(())
}

/// Allow a **co-located** join — a join on the tables' declared shard keys, so every matching pair
/// routes to one shard — and refuse every other join. A co-located join is not rewritten: it stays
/// in the shard SQL (the `FROM` serialises the whole join), so each shard joins its local rows and
/// the results concatenate. Refused: cross joins, joins on non-shard-key columns, joins touching an
/// undeclared table, `USING`/`NATURAL`, non-equality conditions, and derived-table joins — in each
/// the matching rows may live on different shards and nothing moves them together.
fn check_join(select: &Select, shard_keys: &ShardKeys) -> std::result::Result<(), Unsupported> {
    if select.from.len() > 1 {
        return Err(Unsupported {
            what: "a cross join (comma-separated tables)",
            why: "it pairs rows regardless of key, so matching rows may live on different shards",
        });
    }
    let Some(table) = select.from.first() else {
        return Ok(()); // no FROM at all (e.g. `SELECT 1`)
    };
    // A derived table (subquery in FROM) would be pushed to each shard, where an inner aggregate or
    // grouping is only a per-shard fragment — a silent wrong answer. It needs coordinator-side
    // materialisation, which is not built yet, so refuse it.
    if !matches!(&table.relation, TableFactor::Table { .. }) {
        return Err(Unsupported {
            what: "a subquery or table-valued function in FROM",
            why: "a derived table needs coordinator-side materialisation, not built yet; run it \
                  as a separate query",
        });
    }
    if table.joins.is_empty() {
        return Ok(()); // a single table — nothing to co-locate
    }

    // Each table alias → its declared shard-key column. A joined table with no declared shard key
    // makes the join uncheckable and is refused.
    let mut alias_key: HashMap<String, String> = HashMap::new();
    add_join_relation(&table.relation, shard_keys, &mut alias_key)?;
    for join in &table.joins {
        add_join_relation(&join.relation, shard_keys, &mut alias_key)?;
        let on = join_on_condition(&join.join_operator)?;
        if !on_equates_shard_keys(on, &alias_key) {
            return Err(Unsupported {
                what: "a join that is not on the tables' shard keys",
                why: "matching rows may live on different shards; join co-partitioned tables on \
                      their declared shard keys",
            });
        }
    }
    Ok(())
}

/// Record a joined relation's alias → shard key, refusing a non-table relation or an undeclared
/// table.
fn add_join_relation(
    relation: &TableFactor,
    shard_keys: &ShardKeys,
    alias_key: &mut HashMap<String, String>,
) -> std::result::Result<(), Unsupported> {
    let TableFactor::Table { name, alias, .. } = relation else {
        return Err(Unsupported {
            what: "a subquery or derived table in a join",
            why: "only named, co-partitioned tables can be joined across shards",
        });
    };
    let table_name = name.to_string().to_ascii_lowercase();
    let shard_key = shard_keys.get(&table_name).ok_or(Unsupported {
        what: "a join on a table with no declared shard key",
        why: "declare the table's shard key (co-partitioning) before joining it across shards",
    })?;
    let alias_name = alias
        .as_ref()
        .map(|a| a.name.value.to_ascii_lowercase())
        .unwrap_or(table_name);
    alias_key.insert(alias_name, shard_key.to_ascii_lowercase());
    Ok(())
}

/// The `ON` expression of a join, or a refusal for a join type or constraint that cannot be
/// pushed down (`CROSS`, `USING`, `NATURAL`).
fn join_on_condition(op: &JoinOperator) -> std::result::Result<&Expr, Unsupported> {
    let constraint = match op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c) => c,
        _ => {
            return Err(Unsupported {
                what: "this join type",
                why: "only INNER, LEFT, RIGHT and FULL joins on shard keys are supported",
            });
        }
    };
    match constraint {
        JoinConstraint::On(expr) => Ok(expr),
        _ => Err(Unsupported {
            what: "a USING or NATURAL join",
            why: "the join must have an explicit ON equality on the shard keys",
        }),
    }
}

/// Whether the `ON` condition guarantees the join is on the two tables' shard keys: some top-level
/// `AND` conjunct is `X.a = Y.b` where `X`, `Y` are different tables and `a`, `b` are their shard
/// keys. Extra conjuncts (e.g. `AND B.active`) are fine — they only filter and are pushed down.
fn on_equates_shard_keys(on: &Expr, alias_key: &HashMap<String, String>) -> bool {
    and_conjuncts(on).into_iter().any(|conj| {
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } = conj
            && let (Some((xa, xc)), Some((ya, yc))) = (compound_pair(left), compound_pair(right))
        {
            xa != ya
                && alias_key.get(&xa).is_some_and(|k| *k == xc)
                && alias_key.get(&ya).is_some_and(|k| *k == yc)
        } else {
            false
        }
    })
}

/// Split an expression into its top-level `AND` conjuncts (looking through parentheses).
fn and_conjuncts(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut v = and_conjuncts(left);
            v.extend(and_conjuncts(right));
            v
        }
        Expr::Nested(inner) => and_conjuncts(inner),
        other => vec![other],
    }
}

/// A two-part qualified reference `alias.column`, lowercased.
fn compound_pair(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => Some((
            parts[0].value.to_ascii_lowercase(),
            parts[1].value.to_ascii_lowercase(),
        )),
        _ => None,
    }
}

/// Build a central-execution plan for a query whose `FROM` cannot be pushed down. Each `FROM`
/// source becomes a table to materialise on the coordinator — a real table via `SELECT * FROM t`, a
/// derived table via its inner query — and the query is rewritten to reference each source by a
/// plain table name.
fn plan_central(query: &Query) -> std::result::Result<Plan, Unsupported> {
    let mut central = query.clone();
    let SetExpr::Select(select) = central.body.as_mut() else {
        return Err(Unsupported {
            what: "central execution of this query form",
            why: "only a plain SELECT can be run centrally",
        });
    };

    let mut sources: Vec<CentralSource> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for table in &mut select.from {
        collect_central_source(&mut table.relation, &mut sources, &mut seen)?;
        for join in &mut table.joins {
            collect_central_source(&mut join.relation, &mut sources, &mut seen)?;
        }
    }
    if sources.is_empty() {
        return Err(Unsupported {
            what: "a query with no source table to materialise",
            why: "there is nothing to run centrally",
        });
    }

    Ok(Plan::Central(Box::new(Central {
        sources,
        central_sql: central.to_string(),
    })))
}

/// Record one `FROM` relation as a source to materialise, rewriting a derived table into a plain
/// reference to its (aliased) name. A non-table, non-derived source (a table function) is refused.
fn collect_central_source(
    relation: &mut TableFactor,
    sources: &mut Vec<CentralSource>,
    seen: &mut HashSet<String>,
) -> std::result::Result<(), Unsupported> {
    match relation {
        TableFactor::Table { name, .. } => {
            let table = name.to_string();
            if seen.insert(table.to_ascii_lowercase()) {
                sources.push(CentralSource {
                    sql: format!("SELECT * FROM {table}"),
                    table,
                });
            }
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let alias = alias.as_ref().ok_or(Unsupported {
                what: "a derived table without an alias",
                why: "a subquery in FROM must be named with AS so it can be referenced",
            })?;
            let name = alias.name.value.clone();
            if seen.insert(name.to_ascii_lowercase()) {
                sources.push(CentralSource {
                    table: name.clone(),
                    sql: subquery.to_string(),
                });
            }
            *relation = named_table(&name);
        }
        _ => {
            return Err(Unsupported {
                what: "this FROM source",
                why: "only named tables and derived tables can be materialised for central \
                      execution",
            });
        }
    }
    Ok(())
}

/// A `TableFactor::Table` referring to `name`, built by parsing a trivial query so the variant's
/// many fields need not be constructed by hand. `name` is quoted so any identifier is accepted.
fn named_table(name: &str) -> TableFactor {
    let sql = format!("SELECT 1 FROM \"{}\"", name.replace('"', "\"\""));
    let statements = Parser::parse_sql(&SQLiteDialect {}, &sql).expect("a trivial query parses");
    match statements.into_iter().next() {
        Some(SqlStatement::Query(query)) => match *query.body {
            SetExpr::Select(select) => select.from.into_iter().next().expect("one table").relation,
            _ => unreachable!("the trivial query is a SELECT"),
        },
        _ => unreachable!("the trivial query is a query statement"),
    }
}

/// The caller's query rewritten for per-shard execution: `OFFSET` removed and any `LIMIT` set to
/// `limit` (already widened to include the offset), or removed when `limit` is `None`. `ORDER BY`
/// is kept so a widened `LIMIT` still selects the right rows.
fn shard_sql_with_limit(query: &Query, limit: Option<usize>) -> String {
    let mut q = query.clone();
    q.limit_clause = None;
    let base = q.to_string();
    match limit {
        Some(n) => format!("{base} LIMIT {n}"),
        None => base,
    }
}

/// Resolve an `ORDER BY` to sort keys over `projection`, or an empty list when there is none.
fn resolve_sort_keys(
    order_by: &Option<sqlparser::ast::OrderBy>,
    projection: &[SelectItem],
) -> std::result::Result<Vec<SortKey>, Unsupported> {
    let Some(order) = order_by else {
        return Ok(Vec::new());
    };
    let OrderByKind::Expressions(exprs) = &order.kind else {
        return Err(Unsupported {
            what: "ORDER BY ALL",
            why: "the sort columns cannot be resolved to output columns",
        });
    };
    let mut keys = Vec::with_capacity(exprs.len());
    for e in exprs {
        let column = output_column_index(&e.expr, projection).ok_or(Unsupported {
            what: "ORDER BY on an expression that is not in the select list",
            why: "a merge can only sort by values it can see in the rows, so the sort column \
                  must be selected (use an alias or an ordinal)",
        })?;
        let descending = e.options.asc == Some(false);
        keys.push(SortKey {
            column,
            // SQLite's default: NULLs first ascending, last descending.
            nulls_first: e.options.nulls_first.unwrap_or(!descending),
            descending,
        });
    }
    Ok(keys)
}

/// A query's `(LIMIT, OFFSET)` as literals. `OFFSET` is no longer a blanket refusal — an ordered
/// query can honour it at the coordinator.
fn limit_and_offset(query: &Query) -> std::result::Result<(Option<usize>, usize), Unsupported> {
    let Some(clause) = &query.limit_clause else {
        return Ok((None, 0));
    };
    let LimitClause::LimitOffset { limit, offset, .. } = clause else {
        return Err(Unsupported {
            what: "this LIMIT form",
            why: "only a literal row count can be pushed down to each shard",
        });
    };
    let lim = match limit {
        None => None,
        Some(e) => Some(literal_usize(e).ok_or(Unsupported {
            what: "a non-literal LIMIT",
            why: "only a literal row count can be pushed down to each shard",
        })?),
    };
    let off = match offset {
        None => 0,
        Some(o) => literal_usize(&o.value).ok_or(Unsupported {
            what: "a non-literal OFFSET",
            why: "only a literal row offset is supported",
        })?,
    };
    Ok((lim, off))
}

/// A non-negative integer literal, or `None` for anything else.
fn literal_usize(e: &Expr) -> Option<usize> {
    match e {
        Expr::Value(ValueWithSpan {
            value: SqlValue::Number(n, _),
            ..
        }) => n.parse::<usize>().ok(),
        _ => None,
    }
}

/// Plan a `GROUP BY` as two-phase aggregation: build the per-shard partial-aggregation query and
/// the coordinator recipe for folding those partials back into the global answer.
fn plan_grouped(
    query: &Query,
    select: &Select,
    group_exprs: &[Expr],
    modifiers: &[GroupByWithModifier],
    shard_keys: &ShardKeys,
) -> std::result::Result<Plan, Unsupported> {
    if !modifiers.is_empty() {
        return Err(Unsupported {
            what: "ROLLUP, CUBE or GROUPING SETS",
            why: "these produce super-aggregate rows that a fan-out cannot reassemble",
        });
    }
    // A co-located join is allowed; it rides along in the shard SQL below (the FROM serialises the
    // whole join), and each shard groups its local join result.
    check_join(select, shard_keys)?;
    let from = select.from.first().ok_or(Unsupported {
        what: "a grouped query with no FROM",
        why: "there is nothing to fan out",
    })?;

    let group_sql: Vec<String> = group_exprs.iter().map(Expr::to_string).collect();
    let group_norm: Vec<String> = group_exprs.iter().map(normalize_expr).collect();
    let k = group_exprs.len();

    // The per-shard partial projection: the grouping columns first (positions 0..k), then one or
    // two partial columns per aggregate. The coordinator consumes these by position.
    let mut partial: Vec<String> = group_sql.clone();
    let mut outputs: Vec<OutputCol> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    let mut next = k;
    let mut needs_central = false;

    for item in &select.projection {
        let expr = match item {
            SelectItem::UnnamedExpr(e) => e,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
                return Err(Unsupported {
                    what: "* in a grouped query",
                    why: "a wildcard selects bare columns a fan-out cannot reassemble; list the \
                          group columns and aggregates explicitly",
                });
            }
            SelectItem::ExprWithAliases { .. } => {
                return Err(Unsupported {
                    what: "a column-list alias in a grouped query",
                    why: "multi-alias projections are not supported in a fan-out",
                });
            }
        };
        names.push(output_name(item, expr));

        if let Expr::Function(f) = expr {
            // A DISTINCT aggregate cannot be combined from per-shard partials; the whole query is
            // run on the coordinator instead (decided after the loop, so a bare column below is
            // still refused rather than silently materialised).
            if is_distinct_aggregate(f) {
                needs_central = true;
                continue;
            }
            if push_aggregate(f, &mut partial, &mut outputs, &mut next)?.is_some() {
                continue;
            }
        }

        // Not a combinable aggregate: it must be a grouping expression, OR a scalar function that
        // wraps an aggregate (e.g. ROUND(SUM(total), 2)). The latter can't be reassembled from
        // per-shard partials but IS computable centrally, so route the whole query there rather than
        // refuse. A truly bare column (no aggregate, not grouped) is still refused — central
        // execution would silently pick an arbitrary row for it.
        let norm = normalize_expr(expr);
        match group_norm.iter().position(|g| *g == norm) {
            Some(gi) => outputs.push(OutputCol::Group(gi)),
            None if expr_contains_aggregate(expr) => needs_central = true,
            None => {
                return Err(Unsupported {
                    what: "a column that is neither grouped nor aggregated",
                    why: "in a grouped query every selected column must appear in GROUP BY or be \
                          a COUNT, SUM, MIN, MAX or AVG",
                });
            }
        }
    }

    // A DISTINCT aggregate anywhere (projection or HAVING) forces central execution: materialise the
    // rows on the coordinator and let SQLite compute the aggregate once. Bare-column reassembly is
    // already validated above, so an unreassemblable projection still refuses.
    if needs_central
        || select
            .having
            .as_ref()
            .is_some_and(expr_has_distinct_aggregate)
    {
        return plan_central_grouped(query, select);
    }

    // The projection columns are the visible ones; HAVING may append hidden columns after them.
    let visible_count = outputs.len();
    let having = match &select.having {
        None => None,
        Some(expr) => Some(build_having(
            expr,
            &mut partial,
            &mut outputs,
            &mut next,
            &group_norm,
        )?),
    };

    // LIMIT and OFFSET are coordinator-only for a grouped query; ORDER BY resolves against the
    // visible output columns.
    let (limit, offset) = limit_and_offset(query)?;
    let order_by = resolve_sort_keys(&query.order_by, &select.projection)?;

    let where_clause = select
        .selection
        .as_ref()
        .map(|e| format!(" WHERE {e}"))
        .unwrap_or_default();
    // A bare aggregate (k == 0) has no GROUP BY clause — it groups over the whole table.
    let group_clause = if k == 0 {
        String::new()
    } else {
        format!(" GROUP BY {}", group_sql.join(", "))
    };
    let shard_sql = format!(
        "SELECT {} FROM {}{}{}",
        partial.join(", "),
        from,
        where_clause,
        group_clause,
    );

    Ok(Plan::Grouped(Box::new(Grouped {
        shard_sql,
        group_cols: (0..k).collect(),
        outputs,
        visible_count,
        having,
        output_names: names,
        order_by,
        offset,
        limit,
    })))
}

/// Whether a projection item is a DISTINCT aggregate — such an aggregate must never take the
/// associative fast path, where summing per-shard partials would double-count.
fn item_is_distinct_aggregate(item: &SelectItem) -> bool {
    let expr = match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
        _ => return false,
    };
    matches!(expr, Expr::Function(f) if is_distinct_aggregate(f))
}

/// The aggregate's argument list carries a `DISTINCT` marker (`count(DISTINCT c)`).
fn aggregate_is_distinct(f: &sqlparser::ast::Function) -> bool {
    matches!(&f.args, FunctionArguments::List(list)
        if list.duplicate_treatment == Some(DuplicateTreatment::Distinct))
}

/// A DISTINCT aggregate over one of the built-in combinable names. It cannot be combined from
/// per-shard partials (the same value can appear on several shards), but the coordinator can
/// compute it once over the materialised rows.
fn is_distinct_aggregate(f: &sqlparser::ast::Function) -> bool {
    aggregate_is_distinct(f)
        && matches!(
            f.name.to_string().to_ascii_uppercase().as_str(),
            "COUNT" | "SUM" | "TOTAL" | "MIN" | "MAX" | "AVG"
        )
}

/// Whether an expression tree contains a DISTINCT aggregate — routes a `HAVING count(DISTINCT c) …`
/// to central execution.
fn expr_has_distinct_aggregate(expr: &Expr) -> bool {
    let mut found = false;
    let _ = visit_expressions(expr, |e| {
        if let Expr::Function(f) = e
            && is_distinct_aggregate(f)
        {
            found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    found
}

/// Whether an expression contains an aggregate anywhere in its tree — e.g. the `SUM` inside
/// `ROUND(SUM(total), 2)`. A scalar function that *wraps* an aggregate cannot be combined from
/// per-shard partials (the wrapper would apply before the merge), but it *is* computable centrally,
/// so this routes such a projection to central execution instead of refusing it.
fn expr_contains_aggregate(expr: &Expr) -> bool {
    let mut found = false;
    let _ = visit_expressions(expr, |e| {
        if let Expr::Function(f) = e
            && matches!(
                f.name.to_string().to_ascii_uppercase().as_str(),
                "COUNT" | "SUM" | "TOTAL" | "MIN" | "MAX" | "AVG" | "GROUP_CONCAT" | "STRING_AGG"
            )
        {
            found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    found
}

/// Plan a grouped/aggregate query that uses a DISTINCT aggregate by running it on the coordinator
/// over materialised rows. For a single table the source is narrowed to just the referenced columns,
/// and pre-`DISTINCT`'d when every aggregate is insensitive to row multiplicity — collapsing the
/// materialised set from row count to distinct cardinality, which is what keeps a `count(DISTINCT c)`
/// over a large table under the materialisation cap. A join defers to the generic central path,
/// which materialises each source as `SELECT *`.
fn plan_central_grouped(query: &Query, select: &Select) -> std::result::Result<Plan, Unsupported> {
    let single_table = match select.from.first() {
        Some(t) if select.from.len() == 1 && t.joins.is_empty() => match &t.relation {
            TableFactor::Table { name, .. } => Some(name.to_string()),
            _ => None,
        },
        _ => None,
    };
    let Some(table) = single_table else {
        return plan_central(query);
    };

    let cols = referenced_columns(query);
    let projection = if cols.is_empty() {
        "*".to_string()
    } else {
        cols.join(", ")
    };
    let distinct = if distinct_preshrink_safe(select) {
        "DISTINCT "
    } else {
        ""
    };
    let where_clause = select
        .selection
        .as_ref()
        .map(|e| format!(" WHERE {e}"))
        .unwrap_or_default();
    let source_sql = format!("SELECT {distinct}{projection} FROM {table}{where_clause}");

    Ok(Plan::Central(Box::new(Central {
        sources: vec![CentralSource {
            sql: source_sql,
            table,
        }],
        central_sql: query.to_string(),
    })))
}

/// Every column identifier referenced anywhere in the query, quoted and de-duplicated in first-seen
/// order — used to project a central source down to just the columns the coordinator query reads.
fn referenced_columns(query: &Query) -> Vec<String> {
    let mut cols: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let _ = visit_expressions(query, |e| {
        let name = match e {
            Expr::Identifier(id) => Some(id.value.clone()),
            Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()),
            _ => None,
        };
        if let Some(name) = name
            && seen.insert(name.clone())
        {
            cols.push(format!("\"{}\"", name.replace('"', "\"\"")));
        }
        ControlFlow::<()>::Continue(())
    });
    cols
}

/// DISTINCT pre-shrink of the source is valid only when every aggregate is insensitive to how many
/// times a row appears — all aggregates are DISTINCT, or MIN / MAX. A plain COUNT / SUM / AVG / TOTAL
/// (including `COUNT(*)`) counts multiplicity, so duplicate rows must be kept.
fn distinct_preshrink_safe(select: &Select) -> bool {
    fn sensitive(f: &sqlparser::ast::Function) -> bool {
        !aggregate_is_distinct(f)
            && matches!(
                f.name.to_string().to_ascii_uppercase().as_str(),
                "COUNT" | "SUM" | "TOTAL" | "AVG"
            )
    }
    let mut safe = true;
    {
        let mut check = |e: &Expr| {
            if let Expr::Function(f) = e
                && sensitive(f)
            {
                safe = false;
            }
            ControlFlow::<()>::Continue(())
        };
        for item in &select.projection {
            if let SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } = item {
                let _ = visit_expressions(e, &mut check);
            }
        }
        if let Some(h) = &select.having {
            let _ = visit_expressions(h, &mut check);
        }
    }
    safe
}

/// Add the partial column(s) and output entry for an aggregate function, advancing the partial
/// column cursor. Returns the new output's index, or `None` if `f` is not a decomposable
/// aggregate (in which case the caller treats it as a grouping expression).
fn push_aggregate(
    f: &sqlparser::ast::Function,
    partial: &mut Vec<String>,
    outputs: &mut Vec<OutputCol>,
    next: &mut usize,
) -> std::result::Result<Option<usize>, Unsupported> {
    let Some(choice) = classify_aggregate(f)? else {
        return Ok(None);
    };
    let arg = single_arg_sql(f)?;
    let index = outputs.len();
    match choice {
        AggChoice::Simple(kind) => {
            // Keep the original function per shard (COUNT stays COUNT, not SUM); the coordinator's
            // Combine folds the partials.
            partial.push(format!("{}({})", f.name, arg));
            outputs.push(OutputCol::Combine { kind, col: *next });
            *next += 1;
        }
        AggChoice::Avg => {
            partial.push(format!("SUM({arg})"));
            partial.push(format!("COUNT({arg})"));
            outputs.push(OutputCol::Avg {
                sum_col: *next,
                count_col: *next + 1,
            });
            *next += 2;
        }
    }
    Ok(Some(index))
}

/// Parse a `HAVING` predicate into a coordinator-evaluable form. Aggregates and grouping columns
/// it references are added to `outputs` (as hidden columns) and their partials to `partial`, so
/// the group's computed row carries every value the predicate needs. The grammar is bounded:
/// comparisons and boolean combinations of {a decomposable aggregate | a grouping column | a
/// literal}, plus `IS [NOT] NULL`. Anything else is refused.
fn build_having(
    expr: &Expr,
    partial: &mut Vec<String>,
    outputs: &mut Vec<OutputCol>,
    next: &mut usize,
    group_norm: &[String],
) -> std::result::Result<HavingExpr, Unsupported> {
    match expr {
        Expr::Nested(inner) => build_having(inner, partial, outputs, next, group_norm),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: inner,
        } => Ok(HavingExpr::Not(Box::new(build_having(
            inner, partial, outputs, next, group_norm,
        )?))),
        Expr::IsNull(inner) => Ok(HavingExpr::IsNull {
            value: build_having_value(inner, partial, outputs, next, group_norm)?,
            negated: false,
        }),
        Expr::IsNotNull(inner) => Ok(HavingExpr::IsNull {
            value: build_having_value(inner, partial, outputs, next, group_norm)?,
            negated: true,
        }),
        Expr::BinaryOp { left, op, right } => {
            let compare_op = match op {
                BinaryOperator::And => {
                    return Ok(HavingExpr::And(
                        Box::new(build_having(left, partial, outputs, next, group_norm)?),
                        Box::new(build_having(right, partial, outputs, next, group_norm)?),
                    ));
                }
                BinaryOperator::Or => {
                    return Ok(HavingExpr::Or(
                        Box::new(build_having(left, partial, outputs, next, group_norm)?),
                        Box::new(build_having(right, partial, outputs, next, group_norm)?),
                    ));
                }
                BinaryOperator::Eq => CompareOp::Eq,
                BinaryOperator::NotEq => CompareOp::Ne,
                BinaryOperator::Lt => CompareOp::Lt,
                BinaryOperator::LtEq => CompareOp::Le,
                BinaryOperator::Gt => CompareOp::Gt,
                BinaryOperator::GtEq => CompareOp::Ge,
                _ => {
                    return Err(Unsupported {
                        what: "this operator in HAVING",
                        why: "HAVING supports comparisons (= != < <= > >=) and AND/OR/NOT of them",
                    });
                }
            };
            Ok(HavingExpr::Compare {
                left: build_having_value(left, partial, outputs, next, group_norm)?,
                op: compare_op,
                right: build_having_value(right, partial, outputs, next, group_norm)?,
            })
        }
        _ => Err(Unsupported {
            what: "this HAVING expression",
            why: "HAVING supports comparisons and AND/OR/NOT over aggregates, grouping columns \
                  and literals",
        }),
    }
}

/// Resolve one operand of a `HAVING` comparison to a column of the group's computed row (adding a
/// hidden output if needed) or a literal.
fn build_having_value(
    expr: &Expr,
    partial: &mut Vec<String>,
    outputs: &mut Vec<OutputCol>,
    next: &mut usize,
    group_norm: &[String],
) -> std::result::Result<HavingValue, Unsupported> {
    match expr {
        Expr::Nested(inner) => build_having_value(inner, partial, outputs, next, group_norm),
        Expr::Function(f) => {
            if let Some(index) = push_aggregate(f, partial, outputs, next)? {
                Ok(HavingValue::Column(index))
            } else {
                // A non-aggregate function may still be a grouping expression, e.g. substr(name,1,1).
                group_column_value(expr, outputs, group_norm)
            }
        }
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            group_column_value(expr, outputs, group_norm)
        }
        Expr::Value(_) => Ok(HavingValue::Literal(literal_value(expr)?)),
        // A negative numeric literal parses as -(number).
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr: inner,
        } if matches!(inner.as_ref(), Expr::Value(_)) => match literal_value(inner)? {
            crate::storage::exec::Value::Integer(n) => Ok(HavingValue::Literal(
                crate::storage::exec::Value::Integer(-n),
            )),
            crate::storage::exec::Value::Real(x) => {
                Ok(HavingValue::Literal(crate::storage::exec::Value::Real(-x)))
            }
            _ => Err(Unsupported {
                what: "this HAVING operand",
                why: "only numeric, text and NULL literals are supported",
            }),
        },
        // Any other expression may be a grouping expression, e.g. `(n % 5)` — match it by text.
        _ => group_column_value(expr, outputs, group_norm),
    }
}

/// Reference a grouping column from `HAVING`, appending a hidden `Group` output for it.
fn group_column_value(
    expr: &Expr,
    outputs: &mut Vec<OutputCol>,
    group_norm: &[String],
) -> std::result::Result<HavingValue, Unsupported> {
    let norm = normalize_expr(expr);
    match group_norm.iter().position(|g| *g == norm) {
        Some(gi) => {
            let index = outputs.len();
            outputs.push(OutputCol::Group(gi));
            Ok(HavingValue::Column(index))
        }
        None => Err(Unsupported {
            what: "a HAVING column that is neither grouped nor aggregated",
            why: "HAVING can only reference a grouping column or an aggregate",
        }),
    }
}

/// A literal expression as a [`Value`](crate::storage::exec::Value).
fn literal_value(expr: &Expr) -> std::result::Result<crate::storage::exec::Value, Unsupported> {
    use crate::storage::exec::Value;
    let Expr::Value(ValueWithSpan { value, .. }) = expr else {
        return Err(Unsupported {
            what: "this HAVING literal",
            why: "only numeric, text and NULL literals are supported",
        });
    };
    match value {
        SqlValue::Number(n, _) => n
            .parse::<i64>()
            .map(Value::Integer)
            .or_else(|_| n.parse::<f64>().map(Value::Real))
            .map_err(|_| Unsupported {
                what: "this HAVING number",
                why: "it is not a valid integer or real",
            }),
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
            Ok(Value::Text(s.clone()))
        }
        SqlValue::Boolean(b) => Ok(Value::Integer(i64::from(*b))),
        SqlValue::Null => Ok(Value::Null),
        _ => Err(Unsupported {
            what: "this HAVING literal",
            why: "only numeric, text and NULL literals are supported",
        }),
    }
}

/// Whether any expression in `query` is (or contains) a subquery — walked completely via
/// sqlparser's visitor, so nothing nested is missed.
/// The largest number of rows a subquery may materialise on the coordinator: a scalar or `IN`
/// subquery returning more than this is refused rather than held in memory (an `IN` list that big
/// is a bug, and an unbounded one an OOM).
const MAX_SUBQUERY_ROWS: usize = 100_000;

/// What subqueries a query contains, walked completely by sqlparser's visitor.
enum SubqueryClass {
    /// No subqueries.
    None,
    /// Only substitutable subqueries — scalar `(SELECT …)`, `IN (SELECT …)`, or `EXISTS (…)`.
    Substitutable,
    /// A subquery form that is not substitutable (`ANY` / `ALL`) — refused.
    Unsupported,
}

fn classify_subqueries(query: &Query) -> SubqueryClass {
    let mut substitutable = false;
    let mut unsupported = false;
    let _ = visit_expressions(query, |e| match e {
        Expr::AnyOp { .. } | Expr::AllOp { .. } => {
            unsupported = true;
            ControlFlow::Break(())
        }
        Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => {
            substitutable = true;
            ControlFlow::Continue(())
        }
        _ => ControlFlow::Continue(()),
    });
    if unsupported {
        SubqueryClass::Unsupported
    } else if substitutable {
        SubqueryClass::Substitutable
    } else {
        SubqueryClass::None
    }
}

/// Two-pass subquery evaluation: walk `sql` and, for each subquery, call `eval` with its SQL and
/// splice its result in — a scalar `(SELECT …)` becomes a literal, `IN (SELECT …)` a literal list
/// (or a boolean when empty), `EXISTS (…)` a boolean — then return the rewritten (subquery-free)
/// SQL for the caller to re-plan and fan out. The walk is **post-order**, so a nested subquery is
/// evaluated and substituted before its parent (the parent is then subquery-free when evaluated),
/// and a correlated subquery surfaces naturally as a "no such column" error from `eval`.
pub fn substitute_subqueries<F>(sql: &str, mut eval: F) -> std::result::Result<String, String>
where
    F: FnMut(&str) -> std::result::Result<crate::storage::exec::QueryResult, String>,
{
    let dialect = SQLiteDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;
    let stmt = statements
        .first_mut()
        .ok_or_else(|| "empty statement".to_string())?;

    let mut error: Option<String> = None;
    let _ = visit_expressions_mut(stmt, |expr| match substitute_one(expr, &mut eval) {
        Ok(()) => ControlFlow::Continue(()),
        Err(e) => {
            error = Some(e);
            ControlFlow::Break(())
        }
    });
    if let Some(e) = error {
        return Err(e);
    }
    Ok(stmt.to_string())
}

/// Replace one subquery expression in place with its evaluated result. A no-op for a non-subquery.
fn substitute_one<F>(expr: &mut Expr, eval: &mut F) -> std::result::Result<(), String>
where
    F: FnMut(&str) -> std::result::Result<crate::storage::exec::QueryResult, String>,
{
    match expr {
        // Scalar subquery → a single value (one column, at most one row; no rows is NULL).
        Expr::Subquery(subquery) => {
            let result = eval(&subquery.to_string())?;
            if result.columns.len() != 1 {
                return Err("a scalar subquery must return exactly one column".to_string());
            }
            let value = match result.rows.len() {
                0 => crate::storage::exec::Value::Null,
                1 => result.rows[0]
                    .first()
                    .cloned()
                    .unwrap_or(crate::storage::exec::Value::Null),
                n => {
                    return Err(format!(
                        "a scalar subquery must return at most one row, got {n}"
                    ));
                }
            };
            *expr = literal_to_expr(&value)?;
        }
        // IN subquery → a literal list; an empty result is a constant false (true when negated).
        Expr::InSubquery {
            expr: lhs,
            subquery,
            negated,
        } => {
            let result = eval(&subquery.to_string())?;
            if result.columns.len() != 1 {
                return Err("an IN subquery must return exactly one column".to_string());
            }
            if result.rows.len() > MAX_SUBQUERY_ROWS {
                return Err(format!(
                    "an IN subquery returned more than {MAX_SUBQUERY_ROWS} values, too many to \
                     hold on the coordinator; narrow it with a WHERE filter"
                ));
            }
            if result.rows.is_empty() {
                *expr = bool_expr(*negated);
            } else {
                let list = result
                    .rows
                    .iter()
                    .map(|row| {
                        literal_to_expr(row.first().unwrap_or(&crate::storage::exec::Value::Null))
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                *expr = Expr::InList {
                    expr: lhs.clone(),
                    list,
                    negated: *negated,
                };
            }
        }
        // EXISTS → a boolean; correlated forms error inside `eval` (unknown outer column).
        Expr::Exists { subquery, negated } => {
            let result = eval(&subquery.to_string())?;
            let exists = !result.rows.is_empty();
            *expr = bool_expr(exists != *negated);
        }
        _ => {}
    }
    Ok(())
}

/// A SQL boolean literal (`1` / `0`) as an expression.
fn bool_expr(value: bool) -> Expr {
    Expr::Value(SqlValue::Number(i64::from(value).to_string(), false).with_empty_span())
}

/// A [`Value`](crate::storage::exec::Value) as an sqlparser literal expression, for splicing into a
/// query in place of a scalar subquery.
fn literal_to_expr(v: &crate::storage::exec::Value) -> std::result::Result<Expr, String> {
    use crate::storage::exec::Value;
    let value = match v {
        Value::Null => SqlValue::Null,
        Value::Integer(n) => SqlValue::Number(n.to_string(), false),
        Value::Real(f) if f.is_finite() => SqlValue::Number(real_to_sql(*f), false),
        // A non-finite float has no SQL literal; treat it as NULL (an aggregate never yields one).
        Value::Real(_) => SqlValue::Null,
        Value::Text(s) => SqlValue::SingleQuotedString(s.clone()),
        Value::Blob(_) => {
            return Err("a scalar subquery returning a blob cannot be substituted".to_string());
        }
    };
    Ok(Expr::Value(value.with_empty_span()))
}

/// Render a finite `f64` as a SQL numeric literal that keeps its real type (always has a `.`), so
/// substituting it back reproduces the single-shard value exactly.
fn real_to_sql(f: f64) -> String {
    let s = format!("{f}");
    if s.contains(['.', 'e', 'E']) {
        s
    } else {
        format!("{s}.0")
    }
}

/// A canonical, case-insensitive text form of an expression, for matching a projected column
/// against a `GROUP BY` expression. SQLite identifiers are case-insensitive, and `to_string`
/// gives a canonical spelling, so `Region` and `region` (or `substr(name,1,1)` twice) match.
fn normalize_expr(e: &Expr) -> String {
    e.to_string().to_ascii_lowercase()
}

/// The header for a final output column: the alias if any, else the identifier, else the
/// expression text (matching SQLite's default column naming closely enough for a header).
fn output_name(item: &SelectItem, expr: &Expr) -> String {
    if let SelectItem::ExprWithAlias { alias, .. } = item {
        return alias.value.clone();
    }
    match expr {
        Expr::Identifier(id) => id.value.clone(),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|p| p.value.clone())
            .unwrap_or_else(|| expr.to_string()),
        _ => expr.to_string(),
    }
}

/// Classify a function as a decomposable aggregate, or `None` if it is an ordinary scalar
/// function (which, in a grouped query, must then be a grouping expression).
fn classify_aggregate(
    f: &sqlparser::ast::Function,
) -> std::result::Result<Option<AggChoice>, Unsupported> {
    let name = f.name.to_string().to_ascii_uppercase();
    match name.as_str() {
        "COUNT" | "SUM" | "TOTAL" => Ok(Some(AggChoice::Simple(Combine::Sum))),
        "MIN" => Ok(Some(AggChoice::Simple(Combine::Min))),
        "MAX" => Ok(Some(AggChoice::Simple(Combine::Max))),
        "AVG" => Ok(Some(AggChoice::Avg)),
        "GROUP_CONCAT" | "STRING_AGG" => Err(Unsupported {
            what: "GROUP_CONCAT",
            why: "the order of concatenation across shards is not defined",
        }),
        _ if is_aggregate_shaped(f) => Err(Unsupported {
            what: "this aggregate",
            why: "only COUNT, SUM, MIN, MAX and AVG combine correctly from partial results",
        }),
        _ => Ok(None),
    }
}

/// The single argument of an aggregate, rendered back to SQL (`*` for `COUNT(*)`). Refuses a
/// `DISTINCT` argument — a value can appear on several shards, so per-shard distinct counts and
/// sums double-count — and anything that is not a one-argument call.
fn single_arg_sql(f: &sqlparser::ast::Function) -> std::result::Result<String, Unsupported> {
    let FunctionArguments::List(list) = &f.args else {
        return Err(Unsupported {
            what: "an aggregate without a plain argument list",
            why: "only single-argument COUNT, SUM, MIN, MAX and AVG are combined",
        });
    };
    if list.duplicate_treatment == Some(DuplicateTreatment::Distinct) {
        return Err(Unsupported {
            what: "DISTINCT inside an aggregate",
            why: "the same value can appear on several shards, so a per-shard distinct count or \
                  sum would double-count",
        });
    }
    if list.args.len() != 1 {
        return Err(Unsupported {
            what: "an aggregate with more than one argument",
            why: "only single-argument COUNT, SUM, MIN, MAX and AVG are combined",
        });
    }
    match &list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok("*".to_string()),
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e.to_string()),
        _ => Err(Unsupported {
            what: "this aggregate argument form",
            why: "only a column, an expression or * is supported",
        }),
    }
}

/// `Ok(Some(_))` if this projection item is a combinable aggregate, `Ok(None)` if it is a
/// plain column, `Err` if it is an aggregate that cannot be combined.
fn aggregate_of(item: &SelectItem) -> std::result::Result<Option<Combine>, Unsupported> {
    let expr = match item {
        SelectItem::UnnamedExpr(e) => e,
        SelectItem::ExprWithAlias { expr, .. } => expr,
        // `*` and `t.*` are plain column lists.
        // Wildcards and multi-alias forms are plain column lists.
        _ => return Ok(None),
    };

    let Expr::Function(f) = expr else {
        // Anything else is a plain expression over one row.
        return Ok(None);
    };

    let name = f.name.to_string().to_ascii_uppercase();
    match name.as_str() {
        // Associative: a combination of partial results is the global result.
        "COUNT" | "SUM" | "TOTAL" => Ok(Some(Combine::Sum)),
        "MIN" => Ok(Some(Combine::Min)),
        "MAX" => Ok(Some(Combine::Max)),
        "AVG" => Err(Unsupported {
            what: "AVG",
            why: "a mean of per-shard means is not the mean, since shards hold different \
                  numbers of rows. Select SUM and COUNT and divide",
        }),
        "GROUP_CONCAT" | "STRING_AGG" => Err(Unsupported {
            what: "GROUP_CONCAT",
            why: "the order of concatenation across shards is not defined",
        }),
        _ if is_aggregate_shaped(f) => Err(Unsupported {
            what: "this aggregate",
            why: "only COUNT, SUM, MIN and MAX combine correctly from partial results",
        }),
        _ => Ok(None),
    }
}

/// A crude check for "looks like an aggregate call", used only to decide whether an unknown
/// function should be refused rather than treated as a scalar.
fn is_aggregate_shaped(f: &sqlparser::ast::Function) -> bool {
    matches!(&f.args, FunctionArguments::List(list) if list.args.len() <= 1)
        && f.over.is_none()
        && matches!(
            &f.args,
            FunctionArguments::List(list)
                if list.args.iter().any(|a| matches!(
                    a,
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                        | FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(_)))
                ))
        )
}

/// Which output column an `ORDER BY` term refers to.
fn output_column_index(expr: &Expr, projection: &[SelectItem]) -> Option<usize> {
    // `ORDER BY 2` — an ordinal.
    if let Expr::Value(ValueWithSpan {
        value: SqlValue::Number(n, _),
        ..
    }) = expr
        && let Ok(i) = n.parse::<usize>()
        && i >= 1
        && i <= projection.len()
    {
        return Some(i - 1);
    }

    let wanted = match expr {
        Expr::Identifier(id) => id.value.to_ascii_lowercase(),
        Expr::CompoundIdentifier(parts) => parts.last()?.value.to_ascii_lowercase(),
        _ => return None,
    };

    for (i, item) in projection.iter().enumerate() {
        let name = match item {
            SelectItem::ExprWithAlias { alias, .. } => alias.value.to_ascii_lowercase(),
            SelectItem::UnnamedExpr(Expr::Identifier(id)) => id.value.to_ascii_lowercase(),
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                parts.last()?.value.to_ascii_lowercase()
            }
            // A bare `*` selects every column, so the sort column is present but its
            // position is not knowable from the AST alone.
            _ => continue,
        };
        if name == wanted {
            return Some(i);
        }
    }
    None
}
