//! Detecting a replica that has silently diverged.
//!
//! # The failure this exists to catch
//!
//! Replication here is physical: a follower writes raw pages it never interprets. If the
//! capturing VFS mis-reads a frame, or `apply` writes them in the wrong order, the follower
//! ends up holding *valid pages that are the wrong pages*. `PRAGMA integrity_check` passes,
//! because the file is structurally fine. Reads succeed and return wrong rows.
//!
//! Nothing else in this project would notice. The VFS is the highest-risk code here, and
//! this is its only detector.
//!
//! # Why the hash is logical, never the file
//!
//! Hashing pages or bytes looks obviously right and is wrong. Two nodes holding identical
//! data legitimately differ in freelist layout, in vacuum history, and in where a checkpoint
//! happened to leave things. A byte comparison would report divergence constantly and be
//! ignored within a week — which is worse than having no detector, because it would train
//! everyone to dismiss the one real alarm.
//!
//! So this reads **rows**: the schema, then every table's contents in a defined order. What
//! it compares is what a user would call the data.
//!
//! # Encoding is length-prefixed and type-tagged
//!
//! Concatenating values into a hash invites collisions between different data that flattens
//! the same way — `("ab", "c")` and `("a", "bc")` — and between values that print alike but
//! are not equal, like the integer 1 and the text "1". Every value carries a type tag and
//! every variable-length field its length, so distinct content cannot hash alike.

use rusqlite::Connection;

use crate::error::{Error, Result};

/// A content hash. Comparing two of these compares the data two nodes hold.
pub type ContentHash = [u8; 32];

/// Render a hash for logs and messages.
pub fn hex(h: &ContentHash) -> String {
    h.iter().map(|b| format!("{b:02x}")).collect()
}

// Type tags. Distinct so that values which render alike cannot hash alike.
const T_NULL: u8 = 0;
const T_INT: u8 = 1;
const T_REAL: u8 = 2;
const T_TEXT: u8 = 3;
const T_BLOB: u8 = 4;

fn write_bytes(h: &mut blake3::Hasher, tag: u8, bytes: &[u8]) {
    h.update(&[tag]);
    h.update(&(bytes.len() as u64).to_le_bytes());
    h.update(bytes);
}

fn write_value(h: &mut blake3::Hasher, v: &rusqlite::types::ValueRef<'_>) {
    use rusqlite::types::ValueRef;
    match v {
        ValueRef::Null => h.update(&[T_NULL]),
        ValueRef::Integer(i) => {
            h.update(&[T_INT]);
            h.update(&i.to_le_bytes())
        }
        // Hashed by bit pattern, not by its decimal rendering: two floats that print the
        // same are not necessarily the same number.
        ValueRef::Real(f) => {
            h.update(&[T_REAL]);
            h.update(&f.to_bits().to_le_bytes())
        }
        ValueRef::Text(t) => {
            write_bytes(h, T_TEXT, t);
            h
        }
        ValueRef::Blob(b) => {
            write_bytes(h, T_BLOB, b);
            h
        }
    };
}

/// Tables to hash, in a fixed order.
///
/// `sqlite_%` is excluded: those are SQLite's own bookkeeping — sequences, stat tables,
/// internal indexes — and they legitimately differ between nodes holding identical user data.
/// Including them would produce exactly the false alarms this is built to avoid.
fn user_tables(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn
        .prepare(
            "SELECT name FROM sqlite_schema \
             WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
             ORDER BY name",
        )
        .map_err(Error::Sqlite)?;
    let names = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(Error::Sqlite)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Error::Sqlite)?;
    Ok(names)
}

/// Whether a table has a rowid, which decides how its rows can be ordered.
fn has_rowid(conn: &Connection, table: &str) -> bool {
    conn.query_row(
        &format!("SELECT rowid FROM \"{table}\" LIMIT 1"),
        [],
        |_| Ok(()),
    )
    .is_ok()
        || conn
            .query_row(&format!("SELECT count(*) FROM \"{table}\""), [], |r| {
                r.get::<_, i64>(0)
            })
            .map(|n| n == 0)
            .unwrap_or(false)
}

/// Hash the logical contents of a database.
///
/// Two nodes holding the same data produce the same hash regardless of how their files came
/// to be — different vacuum history, different checkpoint timing, different freelist.
pub fn content_hash(conn: &Connection) -> Result<ContentHash> {
    let mut h = blake3::Hasher::new();
    h.update(b"shardlite-content-v1");

    // The schema itself is part of the data: a replica with the right rows and the wrong
    // schema has still diverged.
    {
        let mut stmt = conn
            .prepare(
                "SELECT type, name, tbl_name, COALESCE(sql, '') FROM sqlite_schema \
                 WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
            )
            .map_err(Error::Sqlite)?;
        let mut rows = stmt.query([]).map_err(Error::Sqlite)?;
        while let Some(row) = rows.next().map_err(Error::Sqlite)? {
            for i in 0..4 {
                let v: String = row.get(i).map_err(Error::Sqlite)?;
                write_bytes(&mut h, T_TEXT, v.as_bytes());
            }
        }
    }

    for table in user_tables(conn)? {
        write_bytes(&mut h, T_TEXT, table.as_bytes());

        // Ordering has to be defined or two nodes with identical rows could hash differently
        // purely because SQLite returned them in a different order.
        let order = if has_rowid(conn, &table) {
            "rowid".to_string()
        } else {
            // A WITHOUT ROWID table has no such handle, so order by everything. Slower, and
            // the only ordering guaranteed to be total.
            let mut cols = Vec::new();
            let mut stmt = conn
                .prepare(&format!("PRAGMA table_info(\"{table}\")"))
                .map_err(Error::Sqlite)?;
            let mut rows = stmt.query([]).map_err(Error::Sqlite)?;
            while let Some(row) = rows.next().map_err(Error::Sqlite)? {
                let name: String = row.get(1).map_err(Error::Sqlite)?;
                cols.push(format!("\"{name}\""));
            }
            cols.join(", ")
        };

        let mut stmt = conn
            .prepare(&format!("SELECT * FROM \"{table}\" ORDER BY {order}"))
            .map_err(Error::Sqlite)?;
        let width = stmt.column_count();
        let mut rows = stmt.query([]).map_err(Error::Sqlite)?;
        let mut seen: u64 = 0;
        while let Some(row) = rows.next().map_err(Error::Sqlite)? {
            for i in 0..width {
                let v = row.get_ref(i).map_err(Error::Sqlite)?;
                write_value(&mut h, &v);
            }
            seen += 1;
        }
        // Row count folded in, so a table that lost its last row cannot hash like one that
        // never had it.
        h.update(&seen.to_le_bytes());
    }

    Ok(*h.finalize().as_bytes())
}

/// Hash a database file directly.
///
/// **The caller must ensure nothing is writing it.** For a shard being followed that means
/// pausing the pull loop and waiting for it to come to rest — a follower mid-apply is
/// rewriting pages, and a hash taken across that describes a state that never existed.
pub fn hash_file(path: &std::path::Path) -> Result<ContentHash> {
    let conn = Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| Error::Open {
        path: path.display().to_string(),
        source: e,
    })?;
    content_hash(&conn)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db(setup: &[&str]) -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        for s in setup {
            conn.execute_batch(s).unwrap();
        }
        conn
    }

    #[test]
    fn identical_data_hashes_the_same() {
        let a = db(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'one'), (2, 'two')",
        ]);
        let b = db(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'one'), (2, 'two')",
        ]);
        assert_eq!(content_hash(&a).unwrap(), content_hash(&b).unwrap());
    }

    #[test]
    fn a_single_changed_row_diverges() {
        let a = db(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'one')",
        ]);
        let b = db(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'onf')",
        ]);
        assert_ne!(content_hash(&a).unwrap(), content_hash(&b).unwrap());
    }

    #[test]
    fn a_missing_row_diverges() {
        // The replication bug this is really for: a frame that never arrived.
        let a = db(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY)",
            "INSERT INTO t VALUES (1), (2), (3)",
        ]);
        let b = db(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY)",
            "INSERT INTO t VALUES (1), (2)",
        ]);
        assert_ne!(content_hash(&a).unwrap(), content_hash(&b).unwrap());
    }

    #[test]
    fn vacuum_does_not_look_like_divergence() {
        // The reason this hashes rows and not the file. VACUUM rewrites page layout entirely
        // while changing nothing a user would call data; a byte comparison would scream.
        let conn = db(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            "DELETE FROM t WHERE id = 2",
        ]);
        let before = content_hash(&conn).unwrap();
        conn.execute_batch("VACUUM").unwrap();
        assert_eq!(
            content_hash(&conn).unwrap(),
            before,
            "VACUUM changed the layout, not the data"
        );
    }

    #[test]
    fn a_freelist_from_deleted_rows_does_not_look_like_divergence() {
        // One node that deleted rows and one that never inserted them hold the same data and
        // very different files.
        let churned = db(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'keep')",
            "INSERT INTO t VALUES (2, 'gone'), (3, 'gone')",
            "DELETE FROM t WHERE v = 'gone'",
        ]);
        let clean = db(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'keep')",
        ]);
        assert_eq!(
            content_hash(&churned).unwrap(),
            content_hash(&clean).unwrap()
        );
    }

    #[test]
    fn values_that_render_alike_do_not_hash_alike() {
        // Without type tags, the integer 1 and the text '1' flatten identically.
        let ints = db(&["CREATE TABLE t (v)", "INSERT INTO t VALUES (1)"]);
        let texts = db(&["CREATE TABLE t (v)", "INSERT INTO t VALUES ('1')"]);
        assert_ne!(content_hash(&ints).unwrap(), content_hash(&texts).unwrap());
    }

    #[test]
    fn a_shifted_field_boundary_does_not_collide() {
        // Without length prefixes, ('ab','c') and ('a','bc') flatten to the same bytes.
        let a = db(&[
            "CREATE TABLE t (x TEXT, y TEXT)",
            "INSERT INTO t VALUES ('ab','c')",
        ]);
        let b = db(&[
            "CREATE TABLE t (x TEXT, y TEXT)",
            "INSERT INTO t VALUES ('a','bc')",
        ]);
        assert_ne!(content_hash(&a).unwrap(), content_hash(&b).unwrap());
    }

    #[test]
    fn a_schema_difference_diverges_even_with_the_same_rows() {
        // A replica with the right rows and the wrong schema has still diverged.
        let a = db(&["CREATE TABLE t (id INTEGER PRIMARY KEY)"]);
        let b = db(&["CREATE TABLE t (id INTEGER PRIMARY KEY, extra TEXT)"]);
        assert_ne!(content_hash(&a).unwrap(), content_hash(&b).unwrap());
    }

    #[test]
    fn row_order_on_disk_does_not_affect_the_hash() {
        // Two nodes that inserted the same rows in different orders hold the same data.
        let a = db(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1,'a'); INSERT INTO t VALUES (2,'b');",
        ]);
        let b = db(&[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (2,'b'); INSERT INTO t VALUES (1,'a');",
        ]);
        assert_eq!(content_hash(&a).unwrap(), content_hash(&b).unwrap());
    }

    #[test]
    fn every_column_type_is_covered() {
        let a = db(&[
            "CREATE TABLE t (a, b, c, d, e)",
            "INSERT INTO t VALUES (NULL, 7, 1.5, 'text', x'00ff')",
        ]);
        let b = db(&[
            "CREATE TABLE t (a, b, c, d, e)",
            "INSERT INTO t VALUES (NULL, 7, 1.5, 'text', x'00ff')",
        ]);
        assert_eq!(content_hash(&a).unwrap(), content_hash(&b).unwrap());

        let changed = db(&[
            "CREATE TABLE t (a, b, c, d, e)",
            "INSERT INTO t VALUES (NULL, 7, 1.5, 'text', x'00fe')",
        ]);
        assert_ne!(content_hash(&a).unwrap(), content_hash(&changed).unwrap());
    }

    #[test]
    fn a_without_rowid_table_is_hashed_too() {
        let a = db(&[
            "CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID",
            "INSERT INTO t VALUES ('a','1'), ('b','2')",
        ]);
        let b = db(&[
            "CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID",
            "INSERT INTO t VALUES ('b','2'), ('a','1')",
        ]);
        assert_eq!(content_hash(&a).unwrap(), content_hash(&b).unwrap());

        let c = db(&[
            "CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID",
            "INSERT INTO t VALUES ('a','1'), ('b','3')",
        ]);
        assert_ne!(content_hash(&a).unwrap(), content_hash(&c).unwrap());
    }

    #[test]
    fn an_empty_database_hashes_consistently() {
        assert_eq!(
            content_hash(&db(&[])).unwrap(),
            content_hash(&db(&[])).unwrap()
        );
        // And is not the same as one holding an empty table.
        assert_ne!(
            content_hash(&db(&[])).unwrap(),
            content_hash(&db(&["CREATE TABLE t (a)"])).unwrap()
        );
    }
}
