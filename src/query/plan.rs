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
//!
//! Those aggregates are exactly the ones that are *associative* — combining per-shard
//! partial results reproduces the global answer. `AVG` is not, which is why it is refused
//! rather than computed from a mean of means; the caller can ask for `SUM` and `COUNT`.
//!
//! # What is refused, and why each one
//!
//! - **`JOIN`** — rows that must meet live on different shards, and nothing moves them.
//! - **`GROUP BY` / `HAVING`** — a group can span shards, so per-shard groups are partial.
//! - **`DISTINCT`** — the same value can appear on several shards.
//! - **`AVG`, and any aggregate not listed above** — not associative.
//! - **`OFFSET`** — the rows skipped per shard are not the rows to skip globally.
//! - **Subqueries, set operations, CTEs** — each can hide any of the above.
//! - **More than one aggregate, or an aggregate mixed with plain columns** — combining them
//!   needs the group semantics that are refused above.
//!
//! # What it cannot fix
//!
//! Even an accepted query is **not a consistent snapshot**. Each shard is read at its own
//! moment, so a fan-out can observe a write on one shard and miss a concurrent one on
//! another. There is no cross-shard atomicity in this design and there never will be, so
//! this is a property of the answer, not a gap to close later.

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, OrderByKind, Query,
    SelectItem, SetExpr, Statement as SqlStatement, Value as SqlValue, ValueWithSpan,
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

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        SetExpr::SetOperation { .. } => {
            return Err(Unsupported {
                what: "UNION, INTERSECT or EXCEPT",
                why: "the operation is defined over whole result sets, and each shard holds \
                      only a fragment of each side",
            });
        }
        _ => {
            return Err(Unsupported {
                what: "this query form",
                why: "only a plain SELECT can be fanned out",
            });
        }
    };

    if select.distinct.is_some() {
        return Err(Unsupported {
            what: "DISTINCT",
            why: "the same value can appear on several shards, so per-shard deduplication \
                  leaves duplicates behind",
        });
    }
    if !matches!(&select.group_by, GroupByExpr::Expressions(e, _) if e.is_empty()) {
        return Err(Unsupported {
            what: "GROUP BY",
            why: "a group can span shards, so each shard produces a partial group and \
                  combining them needs a re-aggregation this planner does not do",
        });
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

    let limit = query
        .limit_clause
        .as_ref()
        .and_then(literal_limit)
        .transpose()?;

    match &query.order_by {
        None => Ok(Plan::Concat { limit }),
        Some(order) => {
            let OrderByKind::Expressions(exprs) = &order.kind else {
                return Err(Unsupported {
                    what: "ORDER BY ALL",
                    why: "the sort columns cannot be resolved to output columns",
                });
            };
            let mut keys = Vec::with_capacity(exprs.len());
            for e in exprs {
                let column =
                    output_column_index(&e.expr, &select.projection).ok_or(Unsupported {
                        what: "ORDER BY on an expression that is not in the select list",
                        why: "a merge can only sort by values it can see in the rows, so the \
                              sort column must be selected",
                    })?;
                let descending = e.options.asc == Some(false);
                keys.push(SortKey {
                    column,
                    // SQLite's default: NULLs first ascending, last descending.
                    nulls_first: e.options.nulls_first.unwrap_or(!descending),
                    descending,
                });
            }
            Ok(Plan::Merge { keys, limit })
        }
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

fn literal_limit(
    clause: &sqlparser::ast::LimitClause,
) -> Option<std::result::Result<usize, Unsupported>> {
    let sqlparser::ast::LimitClause::LimitOffset { limit, offset, .. } = clause else {
        return Some(Err(Unsupported {
            what: "this LIMIT form",
            why: "only a literal row count can be pushed down to each shard",
        }));
    };
    if offset.is_some() {
        return Some(Err(Unsupported {
            what: "OFFSET",
            why: "the rows skipped on each shard are not the rows to skip globally",
        }));
    }
    match limit.as_ref()? {
        Expr::Value(ValueWithSpan {
            value: SqlValue::Number(n, _),
            ..
        }) => Some(n.parse::<usize>().map_err(|_| Unsupported {
            what: "this LIMIT value",
            why: "it is not a literal row count",
        })),
        _ => Some(Err(Unsupported {
            what: "a non-literal LIMIT",
            why: "only a literal row count can be pushed down to each shard",
        })),
    }
}
