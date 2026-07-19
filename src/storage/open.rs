//! Opening connections, and the ordering invariant between them.
//!
//! A read-only connection cannot **create** a database file, so something must create it
//! before a reader can open it. Across the shard fleets that ordering is established
//! dynamically by `WriterFleet::ensure_open`, because the writer that created a shard lives
//! on another thread and may evict it at any time.
//!
//! The invariant holds *per database on every open*, not once per process: when cold shards
//! are closed by the LRU, a shard that is reopened must be created by a writer first.

use std::path::Path;

use rusqlite::{Connection, OpenFlags};

use crate::config::{PragmaProfile, Role};
use crate::error::{Error, Result};
use crate::storage::pragma;

/// Open the single read-write connection for a database, creating it if absent.
pub fn open_writer(path: &Path, p: &PragmaProfile) -> Result<Connection> {
    open_writer_vfs(path, p, None)
}

/// As [`open_writer`], but routed through a named VFS.
///
/// Used to put a database behind the WAL-capture VFS. The capture is a pass-through, so the
/// resulting file is an ordinary SQLite database — nothing about durability or format
/// changes, only that committed frames are also teed to a capture buffer.
pub fn open_writer_vfs(path: &Path, p: &PragmaProfile, vfs: Option<&str>) -> Result<Connection> {
    assert_eq!(
        p.role,
        Role::Writer,
        "open_writer requires a writer profile"
    );
    let path_str = path.display().to_string();

    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;

    let conn = match vfs {
        Some(name) => Connection::open_with_flags_and_vfs(path, flags, name),
        None => Connection::open_with_flags(path, flags),
    }
    .map_err(|source| Error::Open {
        path: path_str.clone(),
        source,
    })?;

    pragma::apply(&conn, p, &path_str)?;
    pragma::verify(&conn, p)?;
    materialize_wal(&conn)?;

    Ok(conn)
}

/// Open a read-only connection to a database that is already known to exist.
///
/// A read-only connection cannot create a database file, so callers must ensure the file
/// exists first — `WriterFleet::ensure_open` is what does that in the fleets.
pub fn open_reader_existing(path: &Path, p: &PragmaProfile) -> Result<Connection> {
    assert_eq!(
        p.role,
        Role::Reader,
        "open_reader_existing requires a reader profile"
    );
    let path_str = path.display().to_string();
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
