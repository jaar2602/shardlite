//! Combining per-shard results into one.

use crate::storage::exec::{QueryResult, Value};

use super::plan::{Combine, Plan, SortKey};

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
