//! Opening connections, and the ordering invariant between them.
//!
//! A read-only connection cannot create the `-wal` and `-shm` sidecar files — only a
//! writer can. So the writer must open a given database before any reader does. That is
//! enforced structurally: [`open_reader`] takes a [`WriterOpened`] token that only
//! [`open_writer`] can produce.
//!
//! The invariant holds *per database on every open*, not once per process. When shard
//! connections are pooled and cold shards are closed, reopening a shard must run the
//! writer first again.

use std::path::Path;

use rusqlite::{Connection, OpenFlags};

use crate::config::{PragmaProfile, Role};
use crate::error::{Error, Result};
use crate::storage::pragma;

/// Proof that a writer has opened this database and materialized its WAL sidecars.
///
/// Not transferable between databases: it carries the path it was issued for.
#[derive(Debug, Clone)]
pub struct WriterOpened {
    path: String,
}

impl WriterOpened {
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// Open the single read-write connection for a database, creating it if absent.
///
/// Returns a [`WriterOpened`] token required by [`open_reader`].
pub fn open_writer(path: &Path, p: &PragmaProfile) -> Result<(Connection, WriterOpened)> {
    assert_eq!(
        p.role,
        Role::Writer,
        "open_writer requires a writer profile"
    );
    let path_str = path.display().to_string();

    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;

    let conn = Connection::open_with_flags(path, flags).map_err(|source| Error::Open {
        path: path_str.clone(),
        source,
    })?;

    pragma::apply(&conn, p, &path_str)?;
    pragma::verify(&conn, p)?;
    materialize_wal(&conn)?;

    Ok((conn, WriterOpened { path: path_str }))
}

/// Open a read-only connection.
///
/// Requires a [`WriterOpened`] token for the same database, because a read-only
/// connection cannot create the WAL sidecars it needs.
pub fn open_reader(path: &Path, p: &PragmaProfile, opened: &WriterOpened) -> Result<Connection> {
    assert_eq!(
        p.role,
        Role::Reader,
        "open_reader requires a reader profile"
    );
    let path_str = path.display().to_string();
    assert_eq!(
        path_str, opened.path,
        "WriterOpened token is for a different database"
    );

    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;

    let conn = Connection::open_with_flags(path, flags).map_err(|source| Error::Open {
        path: path_str.clone(),
        source,
    })?;

    pragma::apply(&conn, p, &path_str)?;
    pragma::verify(&conn, p)?;

    Ok(conn)
}

/// Force the `-wal` and `-shm` files into existence.
///
/// This is required, not defensive. Measured on SQLite 3.51.3 and confirmed on 3.53.2:
/// after `journal_mode = WAL` alone, neither sidecar exists on disk; they appear only on
/// first write-lock acquisition. An empty `IMMEDIATE` transaction is the cheapest way to
/// trigger that.
///
/// Without it a read-only connection has nothing to attach to, which is exactly the
/// failure the [`WriterOpened`] token exists to prevent. Covered by the
/// `wal_sidecars_exist_after_writer_open` integration test — do not remove either.
fn materialize_wal(conn: &Connection) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE; COMMIT;")?;
    Ok(())
}
