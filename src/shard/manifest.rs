//! The on-disk manifest describing a shardlite data directory.
//!
//! Its job is to make **immutable choices enforceable**. `shard_count` cannot change after
//! data exists — changing it re-routes every key, which is indistinguishable from losing
//! the data — so it is recorded at creation and any later disagreement is refused at open
//! rather than discovered as missing rows.
//!
//! Format is a deliberately boring line-based text file: readable by a human with `cat`,
//! diagnosable without tooling, and needing no serialization dependency.
//!
//! ```text
//! shardlite-manifest 1
//! shard_count=64
//! sqlite_version=3.53.2
//! created_unix=1784500000
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

pub const FILE_NAME: &str = "shardlite.manifest";

/// Bumped only when the layout of the data directory changes incompatibly.
pub const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub format_version: u32,
    pub shard_count: u32,
    /// The SQLite that created this directory.
    ///
    /// Recorded rather than enforced: the file format is stable across versions, so a local
    /// upgrade is fine. It matters for *replication*, which is byte-level and requires every
    /// node to link an identical SQLite — a cluster join should compare these.
    pub sqlite_version: String,
    pub created_unix: u64,
}

impl Manifest {
    pub fn path(dir: &Path) -> PathBuf {
        dir.join(FILE_NAME)
    }

    /// Load the manifest for `dir`, creating it if the directory is new.
    ///
    /// Refuses if an existing manifest disagrees with `shard_count`, naming both values —
    /// the operator needs to know what the data actually is, not just that something is off.
    pub fn open_or_create(dir: &Path, shard_count: u32) -> Result<Self> {
        let path = Self::path(dir);

        if path.exists() {
            let found = Self::read(&path)?;

            if found.format_version != FORMAT_VERSION {
                return Err(Error::Manifest(format!(
                    "{} was written by shardlite format version {}, but this build speaks \
                     version {}. Refusing to open.",
                    path.display(),
                    found.format_version,
                    FORMAT_VERSION
                )));
            }

            if found.shard_count != shard_count {
                return Err(Error::Manifest(format!(
                    "{} holds data for {} shards, but this process was configured for {}. \
                     Shard count is fixed at creation because changing it re-routes every \
                     key — existing rows would become unreachable. Open with \
                     shard_count = {} , or migrate the data to a new directory.",
                    path.display(),
                    found.shard_count,
                    shard_count,
                    found.shard_count
                )));
            }

            return Ok(found);
        }

        std::fs::create_dir_all(dir).map_err(|e| {
            Error::Manifest(format!("creating data directory {}: {e}", dir.display()))
        })?;

        let manifest = Self {
            format_version: FORMAT_VERSION,
            shard_count,
            sqlite_version: rusqlite::version().to_string(),
            created_unix: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        };
        manifest.write(&path)?;
        Ok(manifest)
    }

    /// Read without creating. Useful for inspecting a directory.
    pub fn read(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::Manifest(format!("reading {}: {e}", path.display())))?;

        let mut lines = text.lines();
        let header = lines
            .next()
            .ok_or_else(|| Error::Manifest(format!("{} is empty", path.display())))?;

        let format_version = header
            .strip_prefix("shardlite-manifest ")
            .and_then(|v| v.trim().parse::<u32>().ok())
            .ok_or_else(|| {
                Error::Manifest(format!(
                    "{} does not start with a `shardlite-manifest <version>` header; \
                     is this a shardlite data directory?",
                    path.display()
                ))
            })?;

        let fields: HashMap<&str, &str> = lines
            .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
            .filter_map(|l| l.split_once('='))
            .map(|(k, v)| (k.trim(), v.trim()))
            .collect();

        let need = |key: &str| -> Result<&str> {
            fields
                .get(key)
                .copied()
                .ok_or_else(|| Error::Manifest(format!("{} is missing `{key}`", path.display())))
        };

        let shard_count = need("shard_count")?.parse::<u32>().map_err(|e| {
            Error::Manifest(format!(
                "{}: shard_count is not a number: {e}",
                path.display()
            ))
        })?;

        Ok(Self {
            format_version,
            shard_count,
            sqlite_version: need("sqlite_version").unwrap_or("unknown").to_string(),
            created_unix: fields
                .get("created_unix")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
        })
    }

    fn write(&self, path: &Path) -> Result<()> {
        let body = format!(
            "shardlite-manifest {}\n\
             # Written at creation. shard_count is immutable — see src/shard/manifest.rs\n\
             shard_count={}\n\
             sqlite_version={}\n\
             created_unix={}\n",
            self.format_version, self.shard_count, self.sqlite_version, self.created_unix
        );
        std::fs::write(path, body)
            .map_err(|e| Error::Manifest(format!("writing {}: {e}", path.display())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn creates_then_reopens_with_the_same_count() {
        let dir = TempDir::new().unwrap();
        let a = Manifest::open_or_create(dir.path(), 64).unwrap();
        assert_eq!(a.shard_count, 64);
        assert_eq!(a.format_version, FORMAT_VERSION);
        assert!(!a.sqlite_version.is_empty());

        let b = Manifest::open_or_create(dir.path(), 64).unwrap();
        assert_eq!(a, b, "reopening must not rewrite the manifest");
    }

    #[test]
    fn refuses_a_different_shard_count() {
        // The whole reason the manifest exists.
        let dir = TempDir::new().unwrap();
        Manifest::open_or_create(dir.path(), 64).unwrap();

        let err = Manifest::open_or_create(dir.path(), 32).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("64"),
            "error must name the on-disk value: {msg}"
        );
        assert!(
            msg.contains("32"),
            "error must name the requested value: {msg}"
        );
        assert!(
            msg.contains("re-routes every key"),
            "error must explain why this is refused: {msg}"
        );
    }

    #[test]
    fn refuses_an_unknown_format_version() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            Manifest::path(dir.path()),
            "shardlite-manifest 999\nshard_count=64\nsqlite_version=3.53.2\ncreated_unix=1\n",
        )
        .unwrap();
        let err = Manifest::open_or_create(dir.path(), 64).unwrap_err();
        assert!(err.to_string().contains("999"));
    }

    #[test]
    fn rejects_a_file_that_is_not_a_manifest() {
        let dir = TempDir::new().unwrap();
        std::fs::write(Manifest::path(dir.path()), "hello\n").unwrap();
        let err = Manifest::open_or_create(dir.path(), 64).unwrap_err();
        assert!(
            err.to_string().contains("shardlite data directory"),
            "should say what is wrong: {err}"
        );
    }

    #[test]
    fn is_human_readable() {
        let dir = TempDir::new().unwrap();
        Manifest::open_or_create(dir.path(), 16).unwrap();
        let text = std::fs::read_to_string(Manifest::path(dir.path())).unwrap();
        assert!(text.starts_with("shardlite-manifest 1\n"));
        assert!(text.contains("shard_count=16"));
    }
}
