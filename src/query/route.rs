//! Routing a single client statement to the shard(s) that hold its rows.
//!
//! Shards are chosen by hashing a row's shard-key value (see [`ShardKeys`] and
//! [`crate::shard::shard_of`]). A client issuing plain SQL should not have to compute that by hand —
//! otherwise every write with no explicit shard lands on shard 0 and the cluster behaves like a
//! single database. This module reads the shard-key value out of an `INSERT` / `UPDATE` / `DELETE` /
//! `SELECT` and says which shard(s) to run it on.
//!
//! Only a statement that pins the shard key to a literal routes to one shard. A write that does not
//! pin it is spread across **every** shard, so no row is missed; a read that does not is left to the
//! caller's fan-out. A statement that would be *wrong* to route — an `INSERT` into a table with a
//! declared shard key whose value is missing or non-literal — is refused rather than sent somewhere
//! plausible. A table with no declared shard key is passed through unchanged (the caller's default).
//!
//! The routing-key bytes match the conventions already used with [`crate::shard::ShardManager`]:
//! text is its UTF-8 bytes, an integer its little-endian bytes. Insert and lookup therefore agree on
//! the shard for the same value — the property the round-trip test pins.

use std::collections::BTreeMap;

use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, ColumnOption, Delete, Expr, FromTable, ObjectName, Query,
    SetExpr, Statement as SqlStatement, TableConstraint, TableFactor, TableObject, TableWithJoins,
    UnaryOperator, Value as SqlValue, ValueWithSpan,
};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

use crate::query::ShardKeys;
use crate::shard::{ModuloV1, Routing};
use crate::storage::exec::Value;

/// Where a client statement should run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// Exactly one shard; run the original SQL there.
    One(u32),
    /// A multi-row `INSERT` whose rows span shards: run each rewritten `INSERT` on its shard.
    Split(Vec<(u32, String)>),
    /// Every shard — a write with no shard-key predicate, so it must touch them all.
    All,
    /// Cannot be routed safely; refuse with this reason.
    Refuse(String),
    /// No routing opinion; the caller uses its default (fan out a read, or the current shard).
    Passthrough,
}

/// The `(table, column)` a `CREATE TABLE` declares as a single-column primary key, if any — so the
/// primary key can be adopted as the shard key automatically. A composite primary key, or a table
/// with none, returns `None` (nothing to route on without a choice). Anything but a `CREATE TABLE`
/// returns `None`.
pub fn primary_key_of(sql: &str) -> Option<(String, String)> {
    let mut stmts = Parser::parse_sql(&SQLiteDialect {}, sql).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let SqlStatement::CreateTable(ct) = stmts.pop().unwrap() else {
        return None;
    };
    let table = last_ident(&ct.name);

    // Column-level `id ... PRIMARY KEY`. More than one so-marked column is not a single key.
    let mut column_pks = ct.columns.iter().filter(|c| {
        c.options
            .iter()
            .any(|o| matches!(o.option, ColumnOption::PrimaryKey(_)))
    });
    if let Some(col) = column_pks.next() {
        return match column_pks.next() {
            Some(_) => None,
            None => Some((table, col.name.value.clone())),
        };
    }

    // Table-level `PRIMARY KEY (col)` — single column only.
    for c in &ct.constraints {
        if let TableConstraint::PrimaryKey(pk) = c {
            return match pk.columns.as_slice() {
                [only] => match &only.column.expr {
                    Expr::Identifier(id) => Some((table, id.value.clone())),
                    _ => None,
                },
                _ => None,
            };
        }
    }
    None
}

/// Decide where a single statement should run, given the declared shard keys.
pub fn route_statement(sql: &str, keys: &ShardKeys, shard_count: u32) -> Route {
    let routing = Routing::ModuloV1(
        ModuloV1::new(shard_count).expect("route_statement requires at least one shard"),
    );
    route_statement_with(sql, keys, routing)
}

/// Decide where a statement should run using the committed routing scheme.
///
/// Kept separate from [`route_statement`] so existing modulo callers retain their API while a
/// dynamic cluster can advance `LinearV1` without teaching clients about shard IDs.
pub fn route_statement_with(sql: &str, keys: &ShardKeys, routing: Routing) -> Route {
    let mut stmts = match Parser::parse_sql(&SQLiteDialect {}, sql) {
        Ok(s) => s,
        // Not our job to report parse errors — let the normal execution path surface them.
        Err(_) => return Route::Passthrough,
    };
    if stmts.len() != 1 {
        return Route::Passthrough;
    }
    match stmts.pop().unwrap() {
        SqlStatement::Insert(insert) => route_insert(insert, keys, routing),
        SqlStatement::Update(u) => {
            // An UPDATE ... FROM joins other tables; the target's key no longer proves all matched
            // rows are co-located, so leave it to the default path.
            let table = if u.from.is_some() {
                None
            } else {
                table_of(&u.table)
            };
            if let Some(key) = updated_shard_key(&u, table.as_deref(), keys) {
                return Route::Refuse(format!(
                    "the shard key column `{key}` cannot be updated; changing it would move a row \
                     between SQLite files outside one transaction"
                ));
            }
            route_point(table, u.selection.as_ref(), keys, routing, Route::All)
        }
        SqlStatement::Delete(d) => route_point(
            delete_table(&d),
            d.selection.as_ref(),
            keys,
            routing,
            Route::All,
        ),
        SqlStatement::Query(q) => {
            let (table, selection) = select_target(&q);
            route_point(table, selection, keys, routing, Route::Passthrough)
        }
        _ => Route::Passthrough,
    }
}

/// The declared shard key changed by this statement, if any.
///
/// This guard is also used by explicitly-sharded write APIs: naming a physical shard must not make
/// a row-key move possible outside the router.
pub fn shard_key_update(sql: &str, keys: &ShardKeys) -> Option<String> {
    let mut statements = Parser::parse_sql(&SQLiteDialect {}, sql).ok()?;
    if statements.len() != 1 {
        return None;
    }
    let SqlStatement::Update(update) = statements.pop()? else {
        return None;
    };
    let table = table_of(&update.table);
    updated_shard_key(&update, table.as_deref(), keys)
}

fn updated_shard_key(
    update: &sqlparser::ast::Update,
    table: Option<&str>,
    keys: &ShardKeys,
) -> Option<String> {
    let key = shard_key_column(table?, keys)?;
    update
        .assignments
        .iter()
        .any(|assignment| {
            assignment_target_columns(&assignment.target)
                .iter()
                .any(|column| column.eq_ignore_ascii_case(&key))
        })
        .then_some(key)
}

fn assignment_target_columns(target: &AssignmentTarget) -> Vec<String> {
    match target {
        AssignmentTarget::ColumnName(column) => vec![last_ident(column)],
        AssignmentTarget::Tuple(columns) => columns.iter().map(last_ident).collect(),
    }
}

/// Route an `INSERT ... VALUES` by the shard-key value of each row.
fn route_insert(insert: sqlparser::ast::Insert, keys: &ShardKeys, routing: Routing) -> Route {
    let TableObject::TableName(name) = &insert.table else {
        return Route::Passthrough;
    };
    let table = last_ident(name);
    let Some(key_col) = shard_key_column(&table, keys) else {
        // No declared shard key: nothing to route on, use the caller's default.
        return Route::Passthrough;
    };

    if insert.columns.is_empty() {
        return Route::Refuse(format!(
            "INSERT into `{table}` must list its columns so the shard key `{key_col}` can be \
             located; an unlisted INSERT cannot be routed"
        ));
    }
    let Some(col_idx) = insert
        .columns
        .iter()
        .position(|c| last_ident(c).eq_ignore_ascii_case(&key_col))
    else {
        return Route::Refuse(format!(
            "the shard key column `{key_col}` of `{table}` is not in the INSERT column list, so \
             the row cannot be routed"
        ));
    };

    let Some(source) = &insert.source else {
        return Route::Refuse(format!(
            "only INSERT ... VALUES can be routed by the shard key `{key_col}`; this form has no \
             VALUES"
        ));
    };
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Route::Refuse(
            "only INSERT ... VALUES can be routed by shard key; INSERT ... SELECT is not"
                .to_string(),
        );
    };

    // Group row indices by the shard their key value routes to.
    let mut by_shard: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
    for (i, row) in values.rows.iter().enumerate() {
        let Some(expr) = row.content.get(col_idx) else {
            return Route::Refuse(format!(
                "an INSERT row has no value for the shard key `{key_col}`"
            ));
        };
        let Some(shard) = shard_for_expr(expr, routing) else {
            return Route::Refuse(format!(
                "the shard key `{key_col}` must be a literal text or integer to route; got `{expr}`"
            ));
        };
        by_shard.entry(shard).or_default().push(i);
    }

    match by_shard.len() {
        0 => Route::Refuse("INSERT ... VALUES has no rows".to_string()),
        // Every row routes to the same shard — send the statement unchanged.
        1 => Route::One(*by_shard.keys().next().unwrap()),
        // Rows span shards: one INSERT per shard, carrying only that shard's rows.
        _ => Route::Split(
            by_shard
                .into_iter()
                .map(|(shard, idxs)| {
                    let mut one = insert.clone();
                    if let Some(src) = one.source.as_mut()
                        && let SetExpr::Values(v) = src.body.as_mut()
                    {
                        v.rows = idxs.iter().map(|&i| values.rows[i].clone()).collect();
                    }
                    (shard, SqlStatement::Insert(one).to_string())
                })
                .collect(),
        ),
    }
}

/// Route an `UPDATE` / `DELETE` / `SELECT` by a `shard_key = <literal>` predicate in its `WHERE`.
/// With no such predicate the statement is unpinned — `when_unpinned` decides what that means (every
/// shard for a write, the caller's fan-out for a read).
fn route_point(
    table: Option<String>,
    selection: Option<&Expr>,
    keys: &ShardKeys,
    routing: Routing,
    when_unpinned: Route,
) -> Route {
    let Some(table) = table else {
        return Route::Passthrough;
    };
    let Some(key_col) = shard_key_column(&table, keys) else {
        return Route::Passthrough;
    };
    match selection.and_then(|w| pin_value(w, &key_col)) {
        Some(val) => match route_key_bytes(&val) {
            Some(bytes) => Route::One(routing.route(&bytes).0),
            // A pinned but non-routable literal type (real / NULL): fall back safely.
            None => when_unpinned,
        },
        None => when_unpinned,
    }
}

/// The literal a `WHERE` pins `key_col` to, if any — found in a top-level `=` or `AND`-chain. An
/// `OR` is deliberately not descended: it can match rows on more than one shard.
fn pin_value(expr: &Expr, key_col: &str) -> Option<Value> {
    match expr {
        Expr::Nested(inner) => pin_value(inner, key_col),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => pin_value(left, key_col).or_else(|| pin_value(right, key_col)),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if is_column(left, key_col) {
                literal_of(right)
            } else if is_column(right, key_col) {
                literal_of(left)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn is_column(e: &Expr, key_col: &str) -> bool {
    match e {
        Expr::Identifier(id) => id.value.eq_ignore_ascii_case(key_col),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .is_some_and(|p| p.value.eq_ignore_ascii_case(key_col)),
        _ => false,
    }
}

/// A literal value usable as a routing key, or `None` for a non-literal / unroutable expression.
fn literal_of(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            SqlValue::Number(n, _) => n
                .parse::<i64>()
                .map(Value::Integer)
                .ok()
                .or_else(|| n.parse::<f64>().map(Value::Real).ok()),
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                Some(Value::Text(s.clone()))
            }
            SqlValue::Boolean(b) => Some(Value::Integer(i64::from(*b))),
            _ => None,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match literal_of(expr)? {
            Value::Integer(n) => Some(Value::Integer(-n)),
            Value::Real(f) => Some(Value::Real(-f)),
            _ => None,
        },
        _ => None,
    }
}

/// The routing-key bytes for a value — text as UTF-8, integer as little-endian, matching the
/// conventions used directly with [`crate::shard::ShardManager::route`]. A real or NULL key is not
/// routable (returns `None`).
fn route_key_bytes(v: &Value) -> Option<Vec<u8>> {
    match v {
        Value::Text(s) => Some(s.as_bytes().to_vec()),
        Value::Integer(n) => Some(n.to_le_bytes().to_vec()),
        _ => None,
    }
}

fn shard_for_expr(expr: &Expr, routing: Routing) -> Option<u32> {
    let bytes = route_key_bytes(&literal_of(expr)?)?;
    Some(routing.route(&bytes).0)
}

/// The shard-key column declared for `table`, if any. Declarations are keyed by lowercased table
/// name, so the lookup lowercases too.
fn shard_key_column(table: &str, keys: &ShardKeys) -> Option<String> {
    keys.get(&table.to_ascii_lowercase()).cloned()
}

/// The last identifier of a possibly-qualified name (`schema.t` → `t`), unquoted.
fn last_ident(name: &ObjectName) -> String {
    name.0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.clone())
        .unwrap_or_default()
}

/// The single table of a `FROM` item, or `None` if it is a join or a non-table source.
fn table_of(twj: &TableWithJoins) -> Option<String> {
    if !twj.joins.is_empty() {
        return None;
    }
    match &twj.relation {
        TableFactor::Table { name, .. } => Some(last_ident(name)),
        _ => None,
    }
}

fn delete_table(d: &Delete) -> Option<String> {
    let (FromTable::WithFromKeyword(twjs) | FromTable::WithoutKeyword(twjs)) = &d.from;
    match twjs.as_slice() {
        [one] => table_of(one),
        _ => None,
    }
}

fn select_target(q: &Query) -> (Option<String>, Option<&Expr>) {
    let SetExpr::Select(select) = q.body.as_ref() else {
        return (None, None);
    };
    match select.from.as_slice() {
        [one] => (table_of(one), select.selection.as_ref()),
        _ => (None, None),
    }
}
