//! The `CREATE TABLE` constraint gate.
//!
//! # Why a constraint is judged at DDL time
//!
//! A `UNIQUE` index on a column that is not the shard key cannot be enforced across shards: two
//! rows with the same value hash to different files, and neither file can see the other's
//! conflict. The tempting response is to document that and move on. Measurement shows why that is
//! not enough — the same two values collide on one shard at three shards and split at four, so the
//! constraint appears to hold or not hold depending on where they happen to hash. A guarantee that
//! is data-dependent is worse than one that is absent, because nothing about the schema reveals it.
//!
//! So the decision is taken once, at `CREATE TABLE`, against the rules for *many* shards —
//! regardless of how many shards exist right now. A schema that passes the gate means the same
//! thing at one shard and at a hundred, and growing the cluster can never change what a constraint
//! does. This is the rule Citus applies in `create_distributed_table`, for the same reason.
//!
//! The refusal always names the two ways out: put the shard key in the constraint, or declare the
//! table `global`.

use sqlparser::ast::{ColumnOption, Expr, IndexColumn, Statement as SqlStatement, TableConstraint};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

use crate::shard::classes::{TableClass, TableClasses, class_of};

/// Check a `CREATE TABLE` against the rules that must hold at any shard count.
///
/// `Ok(())` for anything that is not a `CREATE TABLE`, for a `global` table (which keeps full
/// SQLite semantics because it lives in one file), and for a sharded table whose constraints are
/// all shard-local. Otherwise an explanation of which constraint cannot survive sharding.
pub fn check_create_table(sql: &str, classes: &TableClasses) -> Result<(), String> {
    let Ok(mut statements) = Parser::parse_sql(&SQLiteDialect {}, sql) else {
        // Not our job to report parse errors — SQLite will.
        return Ok(());
    };
    if statements.len() != 1 {
        return Ok(());
    }
    let SqlStatement::CreateTable(create) = statements.pop().unwrap() else {
        return Ok(());
    };

    let table = last_ident(&create.name.to_string());
    if class_of(classes, &table) == TableClass::Global {
        return Ok(());
    }

    // The shard key is the single-column PRIMARY KEY, matching what the router adopts. Without
    // one, the table has no shard key, so no constraint can include it.
    let shard_key = single_column_primary_key(&create);

    for column in &create.columns {
        let name = column.name.value.to_ascii_lowercase();
        for option in &column.options {
            match &option.option {
                ColumnOption::Unique(_) => {
                    if shard_key.as_deref() != Some(name.as_str()) {
                        return Err(unique_message(
                            &table,
                            std::slice::from_ref(&name),
                            shard_key.as_deref(),
                        ));
                    }
                }
                ColumnOption::ForeignKey { .. } => {
                    return Err(foreign_key_message(&table));
                }
                _ => {}
            }
        }
    }

    for constraint in &create.constraints {
        match constraint {
            TableConstraint::Unique(unique) => {
                let cols: Vec<String> = unique.columns.iter().filter_map(column_name).collect();
                let covered = shard_key
                    .as_deref()
                    .is_some_and(|key| cols.iter().any(|c| c == key));
                if !covered {
                    return Err(unique_message(&table, &cols, shard_key.as_deref()));
                }
            }
            TableConstraint::PrimaryKey(_) => {}
            TableConstraint::ForeignKey { .. } => {
                return Err(foreign_key_message(&table));
            }
            _ => {}
        }
    }

    Ok(())
}

/// The single-column `PRIMARY KEY`, lowercased — the column the router adopts as the shard key.
/// A composite primary key yields `None`: no one column routes the row.
fn single_column_primary_key(create: &sqlparser::ast::CreateTable) -> Option<String> {
    for column in &create.columns {
        for option in &column.options {
            if matches!(&option.option, ColumnOption::PrimaryKey(_)) {
                return Some(column.name.value.to_ascii_lowercase());
            }
        }
    }
    for constraint in &create.constraints {
        if let TableConstraint::PrimaryKey(pk) = constraint
            && let [only] = pk.columns.as_slice()
        {
            return column_name(only);
        }
    }
    None
}

/// The column an index term names, lowercased. An expression index (`UNIQUE (lower(x))`) has no
/// single column, and yields `None` — which the caller treats as not covering the shard key.
fn column_name(index_column: &IndexColumn) -> Option<String> {
    match &index_column.column.expr {
        Expr::Identifier(id) => Some(id.value.to_ascii_lowercase()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.to_ascii_lowercase()),
        _ => None,
    }
}

fn unique_message(table: &str, columns: &[String], shard_key: Option<&str>) -> String {
    let cols = columns.join(", ");
    let key = match shard_key {
        Some(k) => format!("the shard key of `{table}` is `{k}`"),
        None => {
            format!("`{table}` has no shard key, because its PRIMARY KEY is not a single column")
        }
    };
    format!(
        "UNIQUE on ({cols}) cannot be enforced on a sharded table: rows with the same value hash \
         to different shards, so no single SQLite file sees both and the constraint would hold or \
         not hold depending on where the values landed. {key}. Either include the shard key in the \
         constraint, or declare `{table}` global so all its rows live on one shard."
    )
}

fn foreign_key_message(table: &str) -> String {
    format!(
        "a FOREIGN KEY cannot be enforced on a sharded table: the check runs on the shard holding \
         the referencing row, which does not hold the referenced row, so a valid reference is \
         rejected. Declare `{table}` and the table it references global so both live on one shard, \
         or drop the constraint and enforce it in the application."
    )
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

    fn classes() -> TableClasses {
        TableClasses::new()
    }

    #[test]
    fn a_shard_local_schema_passes() {
        for sql in [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE t (id INTEGER PRIMARY KEY UNIQUE, name TEXT)",
            "CREATE TABLE t (id INTEGER PRIMARY KEY, amt INTEGER CHECK (amt >= 0))",
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL DEFAULT 'x')",
        ] {
            assert!(check_create_table(sql, &classes()).is_ok(), "{sql}");
        }
    }

    #[test]
    fn unique_off_the_shard_key_is_refused() {
        let err = check_create_table(
            "CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT UNIQUE)",
            &classes(),
        )
        .unwrap_err();
        assert!(err.contains("UNIQUE"), "{err}");
        assert!(err.contains("global"), "the remedy must be named: {err}");
    }

    #[test]
    fn unique_including_the_shard_key_is_allowed() {
        assert!(
            check_create_table(
                "CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT, UNIQUE (id, email))",
                &classes(),
            )
            .is_ok()
        );
    }

    #[test]
    fn foreign_keys_are_refused_on_a_sharded_table() {
        for sql in [
            "CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES par(id))",
            "CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES par(id))",
        ] {
            assert!(check_create_table(sql, &classes()).is_err(), "{sql}");
        }
    }

    #[test]
    fn a_global_table_keeps_full_sqlite_semantics() {
        let mut classes = classes();
        classes.insert("u".to_string(), TableClass::Global);
        assert!(
            check_create_table(
                "CREATE TABLE u (id INTEGER PRIMARY KEY, email TEXT UNIQUE)",
                &classes,
            )
            .is_ok()
        );
        classes.insert("ch".to_string(), TableClass::Global);
        assert!(
            check_create_table(
                "CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES par(id))",
                &classes,
            )
            .is_ok()
        );
    }

    #[test]
    fn a_composite_primary_key_has_no_shard_key_to_include() {
        let err = check_create_table(
            "CREATE TABLE cp (a INTEGER, b INTEGER, email TEXT UNIQUE, PRIMARY KEY (a, b))",
            &classes(),
        )
        .unwrap_err();
        assert!(err.contains("no shard key"), "{err}");
    }

    #[test]
    fn non_create_statements_are_not_gated() {
        for sql in ["SELECT 1", "INSERT INTO t (id) VALUES (1)", "DROP TABLE t"] {
            assert!(check_create_table(sql, &classes()).is_ok(), "{sql}");
        }
    }
}
