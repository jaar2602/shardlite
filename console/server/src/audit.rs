//! Durable, append-only audit events for security and data-changing console actions.
//!
//! SQL text, parameters, passwords, and cluster credentials are intentionally never accepted by
//! this API. An event records who acted, the operation class and target, and whether it succeeded.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Unix milliseconds.
    pub t: u64,
    pub actor: Option<String>,
    pub action: String,
    pub target: String,
    pub outcome: String,
}

#[derive(Clone)]
pub struct Audit {
    path: PathBuf,
    file: Arc<Mutex<File>>,
}

impl Audit {
    pub fn open(path: &Path) -> Result<Self, String> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)
                .map_err(|e| format!("creating audit directory {}: {e}", dir.display()))?;
        }
        let mut options = OpenOptions::new();
        options.create(true).append(true).read(true);
        #[cfg(unix)]
        options.mode(0o600);
        let file = options
            .open(path)
            .map_err(|e| format!("opening audit log {}: {e}", path.display()))?;
        Ok(Self {
            path: path.to_path_buf(),
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn record(&self, actor: Option<&str>, action: &str, target: &str, outcome: &str) {
        let event = Event {
            t: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            actor: actor.map(str::to_string),
            action: action.to_string(),
            target: target.to_string(),
            outcome: outcome.to_string(),
        };
        let Ok(line) = serde_json::to_vec(&event) else {
            return;
        };
        let mut file = self.file.lock().unwrap();
        if file.write_all(&line).is_ok() && file.write_all(b"\n").is_ok() {
            let _ = file.sync_data();
        }
    }

    pub fn recent(&self, limit: usize) -> Result<Vec<Event>, String> {
        let file = File::open(&self.path)
            .map_err(|e| format!("reading audit log {}: {e}", self.path.display()))?;
        let mut events: Vec<Event> = BufReader::new(file)
            .lines()
            .map_while(Result::ok)
            .filter_map(|line| serde_json::from_str(&line).ok())
            .collect();
        let keep = limit.clamp(1, 1000);
        if events.len() > keep {
            events.drain(..events.len() - keep);
        }
        events.reverse();
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_are_durable_bounded_and_newest_first() {
        let path = std::env::temp_dir().join(format!(
            "shardlite-console-audit-{}.jsonl",
            rand::random::<u64>()
        ));
        let audit = Audit::open(&path).unwrap();
        audit.record(Some("alice"), "query.execute", "prod", "ok");
        audit.record(None, "login", "unknown", "denied");
        let events = audit.recent(1).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, "login");
        assert_eq!(events[0].outcome, "denied");
        std::fs::remove_file(path).ok();
    }
}
