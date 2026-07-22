//! Durable file writes for the console's small state files (users, connections).
//!
//! A half-written users or connections file is a lockout or, worse, a silent loss of a stored
//! credential. So every write lands atomically: write a sibling temp file, fsync it, then rename
//! over the target. A crash leaves either the old file or the new one, never a torn one — the
//! same discipline meshdb's term store uses.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(dir).map_err(|e| format!("creating {}: {e}", dir.display()))?;
    let tmp = path.with_extension("tmp");
    {
        let mut options = OpenOptions::new();
        options.create(true).truncate(true).write(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut f = options
            .open(&tmp)
            .map_err(|e| format!("creating {}: {e}", tmp.display()))?;
        f.write_all(bytes)
            .map_err(|e| format!("writing {}: {e}", tmp.display()))?;
        f.sync_all()
            .map_err(|e| format!("syncing {}: {e}", tmp.display()))?;
    }
    std::fs::rename(&tmp, path)
        .map_err(|e| format!("renaming {} -> {}: {e}", tmp.display(), path.display()))?;
    Ok(())
}
