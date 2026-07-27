//! Strict mode: refuse the statements shardlite would otherwise decide for you.
//!
//! # What strict is, and what it is not
//!
//! Alignment removes the *divergences* — the places where one shard and many shards disagree. It
//! does not remove the **guesses**: the places where shardlite picks something the SQL did not
//! specify, and picks it consistently. `LIMIT` with no `ORDER BY` is the clearest example. The
//! rows returned are now the same at every shard count, but only because shardlite chose an order
//! on the user's behalf.
//!
//! Strict mode is where a developer opts out of being guessed for. One rule:
//!
//! > **Refuse any statement where shardlite would otherwise choose on the user's behalf.**
//!
//! # Why this does not break the one-behaviour rule
//!
//! The alignment contract constrains *answers*, not *acceptance*. Strict only ever shrinks the
//! accepted set — a statement accepted under strict returns byte-identical results to the same
//! statement under the default. It is asserted that way in the alignment suite, and it is why
//! strict is recorded once for the database rather than being a per-connection switch: acceptance
//! may vary between databases, never within one.

use sqlparser::ast::{Query, SetExpr, Statement as SqlStatement};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

use crate::query::ShardKeys;
use crate::shard::classes::TableClasses;

/// Check `sql` against the strict rules. `Ok(())` when nothing was left to shardlite's discretion.
pub fn check(sql: &str, keys: &ShardKeys, classes: &TableClasses) -> Result<(), String> {
    let Ok(mut statements) = Parser::parse_sql(&SQLiteDialect {}, sql) else {
        return Ok(());
    };
    if statements.len() != 1 {
        return Ok(());
    }
    match statements.pop().unwrap() {
        SqlStatement::CreateTable(create) => {
            let table = last_ident(&create.name.to_string());
            if !classes.contains_key(&table) {
                return Err(format!(
                    "strict mode: `{table}` has no declared table class, so shardlite would \
                     default it to sharded. Declare it explicitly as sharded or global before \
                     creating it, so the constraints it may carry are a decision rather than a \
                     default."
                ));
            }
            Ok(())
        }
        SqlStatement::Query(query) => check_query(&query, keys),
        _ => Ok(()),
    }
}

fn check_query(query: &Query, keys: &ShardKeys) -> Result<(), String> {
    let has_limit = query.limit_clause.is_some();
    if !has_limit {
        // Without a LIMIT, an unspecified order changes only the order rows arrive in, not which
        // rows they are. Strict is about choices that change the answer.
        return Ok(());
    }

    let Some(order) = &query.order_by else {
        return Err(
            "strict mode: LIMIT without ORDER BY does not say which rows to keep, so shardlite \
             orders by the shard key to make the answer stable. Add an ORDER BY so the choice is \
             yours."
                .to_string(),
        );
    };

    // With a LIMIT, a non-total ORDER BY does not say which of two tied rows survives — shardlite
    // breaks the tie itself. Requiring the shard key in the sort makes it total, because rows
    // sharing a shard key value are co-located and ordered by that shard alone.
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(());
    };
    let Some(table) = single_table(select) else {
        return Ok(());
    };
    let Some(shard_key) = keys.get(&table.to_ascii_lowercase()) else {
        return Ok(());
    };
    let sorted_by_key = order.to_string().to_ascii_lowercase();
    if !mentions(&sorted_by_key, shard_key) {
        return Err(format!(
            "strict mode: ORDER BY with a LIMIT must be a total order, or which of two tied rows \
             survives the limit is shardlite's choice rather than yours. Add the shard key \
             `{shard_key}` as the last ORDER BY term."
        ));
    }
    Ok(())
}

/// Whether a rendered `ORDER BY` names `column` as a whole word.
fn mentions(rendered: &str, column: &str) -> bool {
    rendered
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .any(|word| word == column.to_ascii_lowercase())
}

fn single_table(select: &sqlparser::ast::Select) -> Option<String> {
    match select.from.as_slice() {
        [only] if only.joins.is_empty() => match &only.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => Some(last_ident(&name.to_string())),
            _ => None,
        },
        _ => None,
    }
}

/// Strict refuses a plan that falls back to coordinator materialisation.
///
/// Such a query is *correct* — but it pulls its sources to one machine, so it is fine on a laptop
/// with forty rows and falls over in production. That is a choice shardlite made silently, and it
/// is the class of problem no functional test catches.
pub fn check_plan(plan: &crate::query::Plan) -> Result<(), String> {
    match plan {
        crate::query::Plan::Central(_) => Err(
            "strict mode: this query cannot be pushed down to the shards, so shardlite would \
             materialise its sources on the coordinator and run it there. That is correct but \
             holds the whole result set in memory on one machine. Restructure the query, or run \
             without strict mode if the cost is acceptable."
                .to_string(),
        ),
        _ => Ok(()),
    }
}

fn last_ident(name: &str) -> String {
    name.rsplit('.')
        .next()
        .unwrap_or(name)
        .trim_matches('"')
        .to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::classes::TableClass;

    fn keys() -> ShardKeys {
        let mut k = ShardKeys::new();
        k.insert("t".to_string(), "id".to_string());
        k
    }

    fn classes() -> TableClasses {
        let mut c = TableClasses::new();
        c.insert("t".to_string(), TableClass::Sharded);
        c
    }

    #[test]
    fn limit_without_order_by_is_refused() {
        let err = check("SELECT id FROM t LIMIT 3", &keys(), &classes()).unwrap_err();
        assert!(err.contains("LIMIT without ORDER BY"), "{err}");
    }

    #[test]
    fn limit_with_a_total_order_is_accepted() {
        assert!(check("SELECT id FROM t ORDER BY id LIMIT 3", &keys(), &classes()).is_ok());
        assert!(
            check(
                "SELECT id FROM t ORDER BY grp, id LIMIT 3",
                &keys(),
                &classes()
            )
            .is_ok()
        );
    }

    #[test]
    fn limit_with_a_tied_order_is_refused() {
        let err = check("SELECT id FROM t ORDER BY grp LIMIT 3", &keys(), &classes()).unwrap_err();
        assert!(err.contains("total order"), "{err}");
    }

    #[test]
    fn an_unlimited_query_needs_no_order() {
        // Without a LIMIT, order affects presentation, not which rows are returned.
        assert!(check("SELECT id FROM t", &keys(), &classes()).is_ok());
        assert!(check("SELECT id FROM t ORDER BY grp", &keys(), &classes()).is_ok());
    }

    #[test]
    fn create_table_without_a_declared_class_is_refused() {
        let err = check(
            "CREATE TABLE fresh (id INTEGER PRIMARY KEY)",
            &keys(),
            &classes(),
        )
        .unwrap_err();
        assert!(err.contains("no declared table class"), "{err}");
    }

    #[test]
    fn create_table_with_a_declared_class_is_accepted() {
        assert!(
            check(
                "CREATE TABLE t (id INTEGER PRIMARY KEY)",
                &keys(),
                &classes()
            )
            .is_ok()
        );
    }

    #[test]
    fn central_execution_is_refused() {
        let plan = crate::query::plan("SELECT z.k FROM (SELECT k FROM t) z").unwrap();
        assert!(check_plan(&plan).is_err());
    }
}
