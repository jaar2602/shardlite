//! How a table is distributed, and what that fixes about its semantics.
//!
//! # Why a table needs a declared class
//!
//! Some guarantees cannot span independent SQLite files. A `UNIQUE` index on a column that is not
//! the shard key is the clearest case: two rows with the same value hash to different shards, land
//! in different files, and neither file can see the other's conflict. Worse, whether the constraint
//! *appears* to hold is data-dependent — the same two values collide on one shard at N=3 and split
//! at N=4, so the constraint holds or does not hold depending on where they hash.
//!
//! Attaching the guarantee to a **declaration** instead of to the shard count is what removes that.
//! A table's class is fixed at `CREATE TABLE` and means the same thing at every shard count, so
//! growing the cluster can never change what a constraint does.
//!
//! # Why `Reference` is not here yet
//!
//! The plan describes a third class, replicated to every shard so a join against it stays
//! co-located. It is deliberately absent: non-co-located joins are currently refused at *every*
//! shard count, so they are a capability gap rather than an alignment gap, and nothing in the
//! measured divergence list needs them. It belongs with the work that widens what shardlite can
//! answer, not with the work that makes one shard and many shards agree.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// How a table's rows are distributed across shards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum TableClass {
    /// Rows are routed by the table's shard key. Constraints are shard-local at every shard
    /// count, including one — which is why a constraint that needs to be global is refused at
    /// `CREATE TABLE` rather than silently weakening on the first split.
    #[default]
    Sharded,
    /// Every row lives on shard 0. Keeps full SQLite semantics permanently: `UNIQUE`, `FOREIGN
    /// KEY`, `AUTOINCREMENT` and a consistent snapshot behave identically at one shard and at a
    /// hundred, because there is only ever one file involved.
    Global,
}

impl TableClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sharded => "sharded",
            Self::Global => "global",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "sharded" => Some(Self::Sharded),
            "global" => Some(Self::Global),
            _ => None,
        }
    }
}

/// Declared classes by lowercased table name. A table with no entry is [`TableClass::Sharded`].
pub type TableClasses = HashMap<String, TableClass>;

/// The class declared for `table`, defaulting to sharded.
pub fn class_of(classes: &TableClasses, table: &str) -> TableClass {
    classes
        .get(&table.to_ascii_lowercase())
        .copied()
        .unwrap_or_default()
}

fn path(dir: &Path) -> PathBuf {
    dir.join("table_classes.txt")
}

/// Load declared classes from `table_classes.txt` (whitespace-separated `table class` lines,
/// `#` comments). A missing or malformed file yields no declarations, so every table is sharded —
/// the conservative default, where constraints are shard-local and the DDL gate applies.
pub fn load(dir: &Path) -> TableClasses {
    let mut classes = TableClasses::new();
    if let Ok(content) = std::fs::read_to_string(path(dir)) {
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((table, class)) = line.split_once(char::is_whitespace)
                && let Some(class) = TableClass::parse(class)
            {
                classes.insert(table.trim().to_ascii_lowercase(), class);
            }
        }
    }
    classes
}

/// Write classes atomically (temp file then rename), so a crash cannot leave a torn file.
pub fn persist(dir: &Path, classes: &TableClasses) -> crate::Result<()> {
    let mut out = String::from("# shardlite table classes: <table> <sharded|global>\n");
    let mut entries: Vec<_> = classes.iter().collect();
    entries.sort();
    for (table, class) in entries {
        out.push_str(table);
        out.push('\t');
        out.push_str(class.as_str());
        out.push('\n');
    }
    let file = path(dir);
    let tmp = file.with_extension("txt.tmp");
    std::fs::write(&tmp, out.as_bytes())
        .map_err(|e| crate::Error::Protocol(format!("writing table classes: {e}")))?;
    std::fs::rename(&tmp, &file)
        .map_err(|e| crate::Error::Protocol(format!("replacing table classes: {e}")))?;
    Ok(())
}
