//! Combining per-shard results into one.

use std::collections::{BTreeMap, BTreeSet};

use crate::storage::exec::{QueryResult, Value};

use super::plan::{
    Combine, CompareOp, Grouped, HavingExpr, HavingValue, OutputCol, Plan, PostProcess, SetKind,
    SetTree, SortKey,
};

/// Combine per-shard results according to `plan`.
///
/// Empty inputs are skipped, so a shard that holds nothing for this query contributes
/// nothing rather than an empty row.
pub fn merge_results(plan: &Plan, parts: Vec<QueryResult>) -> QueryResult {
    let columns = parts
        .iter()
        .find(|p| !p.columns.is_empty())
        .map(|p| p.columns.clone())
        .unwrap_or_default();

    match plan {
        Plan::SingleShard => parts.into_iter().next().unwrap_or_default(),

        Plan::Concat { limit } => {
            let mut rows: Vec<Vec<Value>> = parts.into_iter().flat_map(|p| p.rows).collect();
            if let Some(n) = limit {
                rows.truncate(*n);
            }
            QueryResult { columns, rows }
        }

        Plan::Merge { keys, limit } => {
            // Each shard already sorted its own rows, so a full sort of the concatenation
            // gives the same order as a k-way merge and is simpler to be sure of. The
            // per-shard LIMIT still does the work that matters — it is what stops every
            // shard shipping its whole table.
            let mut rows: Vec<Vec<Value>> = parts.into_iter().flat_map(|p| p.rows).collect();
            rows.sort_by(|a, b| compare_rows(a, b, keys));
            if let Some(n) = limit {
                rows.truncate(*n);
            }
            QueryResult { columns, rows }
        }

        Plan::Aggregate { combine } => {
            let values: Vec<Value> = parts
                .into_iter()
                .filter_map(|p| p.rows.into_iter().next())
                .filter_map(|mut r| {
                    if r.is_empty() {
                        None
                    } else {
                        Some(r.remove(0))
                    }
                })
                .collect();
            QueryResult {
                columns,
                rows: vec![vec![combine_values(*combine, values)]],
            }
        }

        Plan::Grouped(g) => merge_grouped(g, parts),

        Plan::PostProcess(p) => merge_post_process(p, columns, parts),

        // Set operations and scalar-subquery substitution are handled by the coordinator before
        // any per-shard merge (see `ShardManager::eval_set_op` / the ScalarSubqueries branch).
        Plan::SetOp(_) => unreachable!("set operations are combined by eval_set_op, not merge"),
        Plan::ScalarSubqueries => {
            unreachable!("scalar subqueries are substituted before merge")
        }
    }
}

/// Concatenate every shard's rows, then optionally dedup, sort, skip `offset`, and truncate to
/// `limit` — the coordinator half of `DISTINCT`, `UNION` / `UNION ALL`, and `OFFSET`.
fn merge_post_process(
    p: &PostProcess,
    columns: Vec<String>,
    parts: Vec<QueryResult>,
) -> QueryResult {
    let mut rows: Vec<Vec<Value>> = parts.into_iter().flat_map(|p| p.rows).collect();

    if p.distinct {
        dedup_rows(&mut rows);
    }
    finalize_rows(&mut rows, &p.order_by, p.offset, p.limit);
    QueryResult { columns, rows }
}

/// Sort (if there are keys), skip `offset`, then truncate to `limit` — the tail every
/// coordinator-side plan shares.
pub fn finalize_rows(
    rows: &mut Vec<Vec<Value>>,
    order_by: &[SortKey],
    offset: usize,
    limit: Option<usize>,
) {
    if !order_by.is_empty() {
        rows.sort_by(|a, b| compare_rows(a, b, order_by));
    }
    if offset > 0 {
        *rows = if offset < rows.len() {
            rows.split_off(offset)
        } else {
            Vec::new()
        };
    }
    if let Some(n) = limit {
        rows.truncate(n);
    }
}

/// Collapse rows equal under SQLite value comparison (so `Integer(1)` and `Real(1.0)`, and two
/// NULLs, are one row), keeping first appearance. `BTreeSet` navigates by `GroupKey`'s `Ord`.
fn dedup_rows(rows: &mut Vec<Vec<Value>>) {
    let mut seen: BTreeSet<GroupKey> = BTreeSet::new();
    rows.retain(|row| seen.insert(GroupKey(row.clone())));
}

/// Evaluate a set-operation tree over branch results (each already a full fan-out). Rows are cloned
/// out of the branch results and combined with SQLite's `UNION`/`INTERSECT`/`EXCEPT` semantics.
pub fn evaluate_set_tree(tree: &SetTree, branches: &[QueryResult]) -> Vec<Vec<Value>> {
    match tree {
        SetTree::Leaf(i) => branches.get(*i).map(|b| b.rows.clone()).unwrap_or_default(),
        SetTree::Op {
            op,
            all,
            left,
            right,
        } => {
            let l = evaluate_set_tree(left, branches);
            let r = evaluate_set_tree(right, branches);
            apply_set_op(*op, *all, l, r)
        }
    }
}

/// Multiplicities of each distinct row (by value equality).
fn row_counts(rows: &[Vec<Value>]) -> BTreeMap<GroupKey, usize> {
    let mut counts: BTreeMap<GroupKey, usize> = BTreeMap::new();
    for row in rows {
        *counts.entry(GroupKey(row.clone())).or_insert(0) += 1;
    }
    counts
}

/// Combine two branch results under one set operator, honouring `ALL` (multiset) vs distinct
/// semantics exactly as SQLite does.
fn apply_set_op(
    op: SetKind,
    all: bool,
    mut left: Vec<Vec<Value>>,
    right: Vec<Vec<Value>>,
) -> Vec<Vec<Value>> {
    match op {
        SetKind::Union => {
            left.extend(right);
            if !all {
                dedup_rows(&mut left);
            }
            left
        }
        SetKind::Intersect => {
            let mut budget = row_counts(&right);
            let mut seen: BTreeSet<GroupKey> = BTreeSet::new();
            let mut out = Vec::new();
            for row in left {
                let key = GroupKey(row.clone());
                if all {
                    // Keep min(countL, countR) copies: consume the right budget per occurrence.
                    if let Some(remaining) = budget.get_mut(&key)
                        && *remaining > 0
                    {
                        *remaining -= 1;
                        out.push(row);
                    }
                } else if budget.contains_key(&key) && seen.insert(key) {
                    // Distinct rows present in both sides.
                    out.push(row);
                }
            }
            out
        }
        SetKind::Except => {
            let mut budget = row_counts(&right);
            let mut seen: BTreeSet<GroupKey> = BTreeSet::new();
            let mut out = Vec::new();
            for row in left {
                let key = GroupKey(row.clone());
                if all {
                    // Keep max(0, countL - countR) copies: consume the right budget first.
                    if let Some(remaining) = budget.get_mut(&key)
                        && *remaining > 0
                    {
                        *remaining -= 1;
                        continue;
                    }
                    out.push(row);
                } else if !budget.contains_key(&key) && seen.insert(key) {
                    // Distinct rows in the left not present in the right.
                    out.push(row);
                }
            }
            out
        }
    }
}

/// The coordinator half of two-phase aggregation: bucket every shard's partial rows by their
/// group key, then fold each group into one final row per the recipe in `g.outputs`.
fn merge_grouped(g: &Grouped, parts: Vec<QueryResult>) -> QueryResult {
    // Group by the key columns. The key is ordered by `compare_values`, so `Integer(1)` and
    // `Real(1.0)` land in the same group (as SQLite's GROUP BY would), and all NULLs group
    // together. Partial rows are one-per-group-per-shard, so this holds at most (groups × shards)
    // rows — no more than the plain Concat/Merge plans already materialise.
    let mut groups: BTreeMap<GroupKey, Vec<Vec<Value>>> = BTreeMap::new();
    for part in parts {
        for row in part.rows {
            let key: Vec<Value> = g
                .group_cols
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                .collect();
            groups.entry(GroupKey(key)).or_default().push(row);
        }
    }

    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(groups.len());
    for (key, partials) in &groups {
        // Compute the full output row — visible columns plus any hidden columns HAVING needs.
        let mut out: Vec<Value> = Vec::with_capacity(g.outputs.len());
        for col in &g.outputs {
            out.push(match col {
                OutputCol::Group(i) => key.0.get(*i).cloned().unwrap_or(Value::Null),
                OutputCol::Combine { kind, col } => {
                    let vals = partials
                        .iter()
                        .map(|r| r.get(*col).cloned().unwrap_or(Value::Null))
                        .collect();
                    combine_values(*kind, vals)
                }
                OutputCol::Avg { sum_col, count_col } => avg_of(partials, *sum_col, *count_col),
            });
        }
        // HAVING filters complete groups; NULL (unknown) excludes the group, as SQLite does.
        if let Some(having) = &g.having
            && eval_having(having, &out) != Some(true)
        {
            continue;
        }
        out.truncate(g.visible_count);
        rows.push(out);
    }

    finalize_rows(&mut rows, &g.order_by, g.offset, g.limit);

    QueryResult {
        columns: g.output_names.clone(),
        rows,
    }
}

/// Global `AVG` for one group: sum the partial `SUM`s, sum the partial `COUNT`s, divide. NULL
/// when the total count is zero — matching SQLite's `AVG` of an all-NULL group.
fn avg_of(partials: &[Vec<Value>], sum_col: usize, count_col: usize) -> Value {
    let mut total_sum = 0.0f64;
    let mut total_count: i64 = 0;
    for row in partials {
        match row.get(sum_col) {
            Some(Value::Integer(i)) => total_sum += *i as f64,
            Some(Value::Real(f)) => total_sum += *f,
            _ => {} // NULL partial sum (an all-NULL shard group) contributes nothing.
        }
        if let Some(Value::Integer(c)) = row.get(count_col) {
            total_count += *c;
        }
    }
    if total_count == 0 {
        Value::Null
    } else {
        Value::Real(total_sum / total_count as f64)
    }
}

/// Evaluate a `HAVING` predicate over a group's computed output row, in SQL's three-valued logic:
/// `Some(true)` / `Some(false)` for known outcomes, `None` for NULL (unknown). A group is kept only
/// on `Some(true)`, so an unknown (NULL) predicate excludes it — exactly as SQLite does.
fn eval_having(expr: &HavingExpr, row: &[Value]) -> Option<bool> {
    use std::cmp::Ordering;
    match expr {
        HavingExpr::And(a, b) => match (eval_having(a, row), eval_having(b, row)) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        },
        HavingExpr::Or(a, b) => match (eval_having(a, row), eval_having(b, row)) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        },
        HavingExpr::Not(a) => eval_having(a, row).map(|b| !b),
        HavingExpr::IsNull { value, negated } => {
            let is_null = matches!(having_value(value, row), Value::Null);
            Some(is_null ^ *negated)
        }
        HavingExpr::Compare { left, op, right } => {
            let l = having_value(left, row);
            let r = having_value(right, row);
            // A comparison with NULL is unknown.
            if matches!(l, Value::Null) || matches!(r, Value::Null) {
                return None;
            }
            let ord = compare_values(&l, &r);
            Some(match op {
                CompareOp::Eq => ord == Ordering::Equal,
                CompareOp::Ne => ord != Ordering::Equal,
                CompareOp::Lt => ord == Ordering::Less,
                CompareOp::Le => ord != Ordering::Greater,
                CompareOp::Gt => ord == Ordering::Greater,
                CompareOp::Ge => ord != Ordering::Less,
            })
        }
    }
}

fn having_value(value: &HavingValue, row: &[Value]) -> Value {
    match value {
        HavingValue::Column(i) => row.get(*i).cloned().unwrap_or(Value::Null),
        HavingValue::Literal(v) => v.clone(),
    }
}

/// A group key ordered — and therefore made equal — by SQLite's value comparison, so shards that
/// represent the same group differently (`Integer(1)` vs `Real(1.0)`, or via NULLs) still merge.
/// `BTreeMap` uses only `Ord`, so equality here means "same group".
struct GroupKey(Vec<Value>);

impl PartialEq for GroupKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}
impl Eq for GroupKey {}
impl PartialOrd for GroupKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for GroupKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for (a, b) in self.0.iter().zip(other.0.iter()) {
            let ord = compare_values(a, b);
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        self.0.len().cmp(&other.0.len())
    }
}

fn compare_rows(a: &[Value], b: &[Value], keys: &[SortKey]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for k in keys {
        let (x, y) = match (a.get(k.column), b.get(k.column)) {
            (Some(x), Some(y)) => (x, y),
            _ => continue,
        };

        // NULL ordering is explicit rather than folded into the value comparison, because
        // SQLite's default differs by direction and a merge that got it wrong would order
        // rows differently from the same query on a single shard.
        let ord = match (matches!(x, Value::Null), matches!(y, Value::Null)) {
            (true, true) => Ordering::Equal,
            (true, false) => {
                if k.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if k.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let c = compare_values(x, y);
                if k.descending { c.reverse() } else { c }
            }
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// SQLite's storage-class ordering: NULL < numbers < text < blob.
fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    fn rank(v: &Value) -> u8 {
        match v {
            Value::Null => 0,
            Value::Integer(_) | Value::Real(_) => 1,
            Value::Text(_) => 2,
            Value::Blob(_) => 3,
        }
    }
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Real(x), Value::Real(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Value::Integer(x), Value::Real(y)) => {
            (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal)
        }
        (Value::Real(x), Value::Integer(y)) => {
            x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal)
        }
        (Value::Text(x), Value::Text(y)) => x.cmp(y),
        (Value::Blob(x), Value::Blob(y)) => x.cmp(y),
        _ => rank(a).cmp(&rank(b)),
    }
}

fn combine_values(combine: Combine, values: Vec<Value>) -> Value {
    let present: Vec<Value> = values
        .into_iter()
        .filter(|v| !matches!(v, Value::Null))
        .collect();
    if present.is_empty() {
        // MIN/MAX of nothing is NULL, and so is SUM — but COUNT of nothing is 0, and COUNT
        // never yields NULL per shard, so an all-NULL set can only have come from SUM/MIN/MAX.
        return Value::Null;
    }

    match combine {
        Combine::Sum => {
            // Stay in integers when every part is an integer, so COUNT and integer SUM do
            // not come back as floats.
            if present.iter().all(|v| matches!(v, Value::Integer(_))) {
                let total: i64 = present
                    .iter()
                    .map(|v| match v {
                        Value::Integer(i) => *i,
                        _ => 0,
                    })
                    .sum();
                Value::Integer(total)
            } else {
                let total: f64 = present
                    .iter()
                    .map(|v| match v {
                        Value::Integer(i) => *i as f64,
                        Value::Real(f) => *f,
                        _ => 0.0,
                    })
                    .sum();
                Value::Real(total)
            }
        }
        Combine::Min => present
            .into_iter()
            .min_by(compare_values)
            .unwrap_or(Value::Null),
        Combine::Max => present
            .into_iter()
            .max_by(compare_values)
            .unwrap_or(Value::Null),
    }
}
