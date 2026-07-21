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
//! | `GROUP BY g, …` | two-phase: per-shard partial groups, then re-aggregate by key |
//! | `AVG(c)` (bare or grouped) | decompose to `SUM(c)`/`COUNT(c)`, divide at the coordinator |
//! | `DISTINCT` | per-shard dedup, then dedup the union on the coordinator |
//! | `UNION` / `UNION ALL` | per-shard evaluate, then concat (ALL) or dedup on the coordinator |
//! | `OFFSET` (with `ORDER BY`) | each shard ships its top `offset+limit`; skip then take |
//!
//! `COUNT`/`SUM`/`MIN`/`MAX` are *associative* — combining per-shard partials reproduces the
//! global answer. `AVG` is not, but it *decomposes* into two that are, so it is answered by
//! carrying `SUM` and `COUNT` per shard and dividing once. `GROUP BY` is the same idea per
//! group (see [`Grouped`]); `DISTINCT`, `UNION`, and `OFFSET` are coordinator-side reshapes of the
//! concatenated rows (see [`PostProcess`]).
//!
//! # What is refused, and why each one
//!
//! - **`JOIN`** — rows that must meet live on different shards, and nothing moves them.
//! - **`HAVING`** — filtering groups across shards needs a coordinator-side evaluator, not yet
//!   built (a later increment).
//! - **`COUNT(DISTINCT c)` and other `DISTINCT` aggregates** — the same value can appear on
//!   several shards, so a per-shard distinct count double-counts.
//! - **`GROUP_CONCAT`, and any aggregate not listed above** — not associative or order-dependent.
//! - **A selected column that is neither grouped nor aggregated** — a bare column is a partial,
//!   arbitrary row per shard.
//! - **`OFFSET` without `ORDER BY`** — the rows skipped are undefined without a global order.
//! - **`INTERSECT` / `EXCEPT`** — a row can be on one side on one shard and the other side on
//!   another, so per-shard evaluation misses cross-shard matches.
//! - **Subqueries** — each would run against one shard alone, not the whole table.
//! - **CTEs** — can hide any of the above.
//!
//! # What it cannot fix
//!
//! Even an accepted query is **not a consistent snapshot**. Each shard is read at its own
//! moment, so a fan-out can observe a write on one shard and miss a concurrent one on
//! another. There is no cross-shard atomicity in this design and there never will be, so
//! this is a property of the answer, not a gap to close later.

use std::ops::ControlFlow;

use sqlparser::ast::{
    Distinct, DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
    GroupByExpr, GroupByWithModifier, LimitClause, OrderByKind, Query, Select, SelectItem, SetExpr,
    SetOperator, SetQuantifier, Statement as SqlStatement, Value as SqlValue, ValueWithSpan,
    visit_expressions,
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
    /// `DISTINCT`, `UNION` / `UNION ALL`, and `OFFSET`.
    PostProcess(Box<PostProcess>),
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
    /// One entry per final output column, in the order the user selected them.
    pub outputs: Vec<OutputCol>,
    /// Final column headers.
    pub output_names: Vec<String>,
    /// Sort keys over the *final* output columns (empty when there is no `ORDER BY`).
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

/// Decide how `sql` can be run across shards.
pub fn plan(sql: &str) -> std::result::Result<Plan, Unsupported> {
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

    plan_query(&query)
}

fn plan_query(query: &Query) -> std::result::Result<Plan, Unsupported> {
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

    // A subquery anywhere would be pushed verbatim to each shard, where it runs against that shard
    // alone rather than the whole table — a silent wrong answer. Refuse it in every plan (a UNION
    // branch or a WHERE clause included). Walked completely by sqlparser's visitor.
    if has_subquery(query) {
        return Err(Unsupported {
            what: "a subquery",
            why: "it would run against each shard alone, not the whole table, so the merged \
                  result would be wrong; compute the subquery separately and inline its value",
        });
    }

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        // UNION / UNION ALL fan out; INTERSECT / EXCEPT do not (see plan_set_op).
        SetExpr::SetOperation { .. } => return plan_set_op(query),
        _ => {
            return Err(Unsupported {
                what: "this query form",
                why: "only a plain SELECT can be fanned out",
            });
        }
    };

    // DISTINCT dedups on the coordinator; it is no longer refused.
    if select.distinct.is_some() {
        return plan_distinct(query, select);
    }
    // A grouped query takes the dedicated two-phase path; a plain SELECT falls through.
    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) if !exprs.is_empty() => {
            return plan_grouped(query, select, exprs, modifiers);
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
    if select.from.len() > 1 || select.from.first().is_some_and(|f| !f.joins.is_empty()) {
        return Err(Unsupported {
            what: "a join",
            why: "rows that must meet may live on different shards, and nothing moves them \
                  together",
        });
    }

    // An aggregate must be alone in the select list: combining it with plain columns needs
    // the grouping semantics refused above.
    let mut aggregates: Vec<Combine> = Vec::new();
    for item in &select.projection {
        if let Some(c) = aggregate_of(item)? {
            aggregates.push(c);
        }
    }

    if !aggregates.is_empty() {
        if select.projection.len() != 1 {
            return Err(Unsupported {
                what: "an aggregate alongside other columns",
                why: "the plain columns would need a GROUP BY to be meaningful, and groups \
                      are partial per shard",
            });
        }
        if query.order_by.is_some() {
            return Err(Unsupported {
                what: "ORDER BY on an aggregate",
                why: "the aggregate collapses to a single row, so ordering is meaningless",
            });
        }
        return Ok(Plan::Aggregate {
            combine: aggregates[0],
        });
    }

    let (limit, offset) = limit_and_offset(query)?;
    let keys = resolve_sort_keys(&query.order_by, &select.projection)?;

    // OFFSET needs a global order and coordinator-side skipping; it cannot be pushed per shard.
    if offset > 0 {
        if keys.is_empty() {
            return Err(Unsupported {
                what: "OFFSET without ORDER BY",
                why: "the rows skipped per shard are not the rows to skip globally; add an \
                      ORDER BY so the offset is well defined",
            });
        }
        // Each shard ships its top (offset + limit) rows so the coordinator can skip then take.
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

/// Plan a `SELECT DISTINCT`: each shard dedups locally (shrinking transfer), and the coordinator
/// dedups the union of the shards' rows. It is `GROUP BY` on every selected column with no
/// aggregate — the same dedup-on-merge primitive.
fn plan_distinct(query: &Query, select: &Select) -> std::result::Result<Plan, Unsupported> {
    if let Some(Distinct::On(_)) = &select.distinct {
        return Err(Unsupported {
            what: "DISTINCT ON",
            why: "it keeps an arbitrary row per group, which a fan-out cannot reproduce",
        });
    }
    require_plain_select(select)?;

    let (limit, offset) = limit_and_offset(query)?;
    let keys = resolve_sort_keys(&query.order_by, &select.projection)?;
    if offset > 0 && keys.is_empty() {
        return Err(Unsupported {
            what: "OFFSET without ORDER BY",
            why: "the rows skipped per shard are not the rows to skip globally; add an ORDER BY",
        });
    }
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

/// Plan a `UNION` / `UNION ALL`: every shard evaluates the whole set operation over its own rows,
/// and the coordinator concatenates (`UNION ALL`) or dedups (`UNION`). `INTERSECT` / `EXCEPT` are
/// refused — a row can be on one side on one shard and the other side on another, so per-shard
/// evaluation misses cross-shard matches. Mixing `UNION` and `UNION ALL` in one query is refused
/// because their duplicate handling cannot both be honoured in a single coordinator pass.
fn plan_set_op(query: &Query) -> std::result::Result<Plan, Unsupported> {
    let mut leaves: Vec<&Select> = Vec::new();
    let mut distinct: Option<bool> = None;
    collect_union_leaves(query.body.as_ref(), &mut leaves, &mut distinct)?;
    for &leaf in &leaves {
        require_plain_select(leaf)?;
    }
    let first = *leaves.first().ok_or(Unsupported {
        what: "an empty set operation",
        why: "there is nothing to fan out",
    })?;

    let (limit, offset) = limit_and_offset(query)?;
    // An ORDER BY on a compound select resolves against the first branch's output columns.
    let keys = resolve_sort_keys(&query.order_by, &first.projection)?;
    if offset > 0 && keys.is_empty() {
        return Err(Unsupported {
            what: "OFFSET without ORDER BY",
            why: "the rows skipped per shard are not the rows to skip globally; add an ORDER BY",
        });
    }
    let push = if keys.is_empty() {
        None
    } else {
        limit.map(|n| n + offset)
    };
    Ok(Plan::PostProcess(Box::new(PostProcess {
        shard_sql: shard_sql_with_limit(query, push),
        distinct: distinct.unwrap_or(true),
        order_by: keys,
        offset,
        limit,
    })))
}

/// Flatten a set-operation tree into its leaf SELECTs, requiring every operator to be `UNION`
/// with a consistent `ALL`/distinct quantifier.
fn collect_union_leaves<'a>(
    body: &'a SetExpr,
    leaves: &mut Vec<&'a Select>,
    distinct: &mut Option<bool>,
) -> std::result::Result<(), Unsupported> {
    match body {
        SetExpr::Select(s) => {
            leaves.push(s);
            Ok(())
        }
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            if *op != SetOperator::Union {
                return Err(Unsupported {
                    what: "INTERSECT or EXCEPT",
                    why: "a row can be on one side on one shard and the other side on another, so \
                          per-shard evaluation misses cross-shard matches",
                });
            }
            let is_distinct = !matches!(
                set_quantifier,
                SetQuantifier::All | SetQuantifier::AllByName
            );
            match distinct {
                None => *distinct = Some(is_distinct),
                Some(prev) if *prev == is_distinct => {}
                Some(_) => {
                    return Err(Unsupported {
                        what: "mixing UNION and UNION ALL",
                        why: "their duplicate handling differs and a single coordinator pass \
                              cannot honour both; use one or the other",
                    });
                }
            }
            collect_union_leaves(left, leaves, distinct)?;
            collect_union_leaves(right, leaves, distinct)
        }
        _ => Err(Unsupported {
            what: "this query form in a set operation",
            why: "only plain SELECT branches can be fanned out",
        }),
    }
}

/// A projection that produces plain rows: no aggregate, grouping, `HAVING`, or join. Required of
/// a `DISTINCT` query and of every `UNION` branch, where a per-shard aggregate or grouping would
/// be only a fragment of the whole.
fn require_plain_select(select: &Select) -> std::result::Result<(), Unsupported> {
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
    if select.from.len() > 1 || select.from.first().is_some_and(|f| !f.joins.is_empty()) {
        return Err(Unsupported {
            what: "a join",
            why: "rows that must meet may live on different shards, and nothing moves them \
                  together",
        });
    }
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
) -> std::result::Result<Plan, Unsupported> {
    if !modifiers.is_empty() {
        return Err(Unsupported {
            what: "ROLLUP, CUBE or GROUPING SETS",
            why: "these produce super-aggregate rows that a fan-out cannot reassemble",
        });
    }
    if select.having.is_some() {
        return Err(Unsupported {
            what: "HAVING",
            why: "filtering groups across shards needs a coordinator-side evaluator that is not \
                  built yet; select the aggregate and filter on the client",
        });
    }
    if select.from.len() > 1 || select.from.first().is_some_and(|f| !f.joins.is_empty()) {
        return Err(Unsupported {
            what: "a join",
            why: "rows that must meet may live on different shards, and nothing moves them \
                  together",
        });
    }
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

        if let Expr::Function(f) = expr
            && let Some(choice) = classify_aggregate(f)?
        {
            let arg = single_arg_sql(f)?;
            match choice {
                AggChoice::Simple(kind) => {
                    // Keep the original function per shard (COUNT stays COUNT, not SUM); the
                    // coordinator's Combine folds the partials.
                    partial.push(format!("{}({})", f.name, arg));
                    outputs.push(OutputCol::Combine { kind, col: next });
                    next += 1;
                }
                AggChoice::Avg => {
                    partial.push(format!("SUM({arg})"));
                    partial.push(format!("COUNT({arg})"));
                    outputs.push(OutputCol::Avg {
                        sum_col: next,
                        count_col: next + 1,
                    });
                    next += 2;
                }
            }
            continue;
        }

        // Not an aggregate: it must be one of the grouping expressions.
        let norm = normalize_expr(expr);
        match group_norm.iter().position(|g| *g == norm) {
            Some(gi) => outputs.push(OutputCol::Group(gi)),
            None => {
                return Err(Unsupported {
                    what: "a column that is neither grouped nor aggregated",
                    why: "in a grouped query every selected column must appear in GROUP BY or be \
                          a COUNT, SUM, MIN, MAX or AVG",
                });
            }
        }
    }

    // LIMIT and OFFSET are coordinator-only for a grouped query; ORDER BY resolves against the
    // final output columns.
    let (limit, offset) = limit_and_offset(query)?;
    let order_by = resolve_sort_keys(&query.order_by, &select.projection)?;
    if offset > 0 && order_by.is_empty() {
        return Err(Unsupported {
            what: "OFFSET without ORDER BY",
            why: "the groups skipped are undefined without a global order; add an ORDER BY",
        });
    }

    let where_clause = select
        .selection
        .as_ref()
        .map(|e| format!(" WHERE {e}"))
        .unwrap_or_default();
    let shard_sql = format!(
        "SELECT {} FROM {}{} GROUP BY {}",
        partial.join(", "),
        from,
        where_clause,
        group_sql.join(", "),
    );

    Ok(Plan::Grouped(Box::new(Grouped {
        shard_sql,
        group_cols: (0..k).collect(),
        outputs,
        output_names: names,
        order_by,
        offset,
        limit,
    })))
}

/// Whether any expression in `query` is (or contains) a subquery — walked completely via
/// sqlparser's visitor, so nothing nested is missed.
fn has_subquery(query: &Query) -> bool {
    visit_expressions(query, |e| {
        if matches!(
            e,
            Expr::Subquery(_)
                | Expr::Exists { .. }
                | Expr::InSubquery { .. }
                | Expr::AnyOp { .. }
                | Expr::AllOp { .. }
        ) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    })
    .is_break()
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
