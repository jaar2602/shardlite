//! Saved analytical artifacts: **reports** (a named, shareable query with a visualization) and
//! **dashboards** (an arrangement of report tiles). These promote the SQL workbench's per-browser
//! saved queries into first-class, server-persisted, shareable objects. Neither holds a secret, so —
//! unlike connections — nothing here is sealed; they are plain JSON registries (`reports.json`,
//! `dashboards.json`). The same store backs both the UI and the assistant's report/dashboard tools.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::store::write_atomic;

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

const VIZ: &[&str] = &["table", "bar", "line", "number"];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Report {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// The connection this report queries (a connection name), if pinned.
    #[serde(default)]
    pub connection: Option<String>,
    pub sql: String,
    #[serde(default)]
    pub viz: String,
    pub created_by: String,
    pub created_at: u64,
    pub updated_at: u64,
}

/// A tile places one report on a dashboard grid.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tile {
    pub report_id: String,
    #[serde(default)]
    pub x: u32,
    #[serde(default)]
    pub y: u32,
    #[serde(default = "one")]
    pub w: u32,
    #[serde(default = "one")]
    pub h: u32,
    /// Override the report's own visualization on this dashboard, if set.
    #[serde(default)]
    pub viz: Option<String>,
}

fn one() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dashboard {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub tiles: Vec<Tile>,
    pub created_by: String,
    pub created_at: u64,
    pub updated_at: u64,
}

// --- Reports store ---

#[derive(Debug, Default, Serialize, Deserialize)]
struct ReportFile {
    next_id: u64,
    reports: BTreeMap<String, Report>,
}

pub struct Reports {
    path: PathBuf,
    inner: RwLock<ReportFile>,
}

impl Reports {
    pub fn open(path: &Path) -> Result<Self, String> {
        let inner = if path.exists() {
            serde_json::from_slice(&std::fs::read(path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?
        } else {
            ReportFile::default()
        };
        Ok(Self {
            path: path.to_path_buf(),
            inner: RwLock::new(inner),
        })
    }

    pub fn list(&self) -> Vec<Report> {
        self.inner.read().unwrap().reports.values().cloned().collect()
    }

    pub fn get(&self, id: &str) -> Option<Report> {
        self.inner.read().unwrap().reports.get(id).cloned()
    }

    pub fn create(
        &self,
        name: &str,
        description: &str,
        connection: Option<String>,
        sql: &str,
        viz: &str,
        by: &str,
    ) -> Result<Report, String> {
        validate(name, sql, viz)?;
        let mut f = self.inner.write().unwrap();
        f.next_id += 1;
        let now = unix_millis();
        let report = Report {
            id: format!("r{}", f.next_id),
            name: name.trim().to_string(),
            description: description.to_string(),
            connection,
            sql: sql.to_string(),
            viz: normalize_viz(viz),
            created_by: by.to_string(),
            created_at: now,
            updated_at: now,
        };
        f.reports.insert(report.id.clone(), report.clone());
        persist(&self.path, &*f)?;
        Ok(report)
    }

    pub fn update(
        &self,
        id: &str,
        name: &str,
        description: &str,
        connection: Option<String>,
        sql: &str,
        viz: &str,
    ) -> Result<Report, String> {
        validate(name, sql, viz)?;
        let mut f = self.inner.write().unwrap();
        let report = f.reports.get_mut(id).ok_or("no such report")?;
        report.name = name.trim().to_string();
        report.description = description.to_string();
        report.connection = connection;
        report.sql = sql.to_string();
        report.viz = normalize_viz(viz);
        report.updated_at = unix_millis();
        let updated = report.clone();
        persist(&self.path, &*f)?;
        Ok(updated)
    }

    pub fn delete(&self, id: &str) -> Result<(), String> {
        let mut f = self.inner.write().unwrap();
        if f.reports.remove(id).is_none() {
            return Err("no such report".into());
        }
        persist(&self.path, &*f)
    }
}

fn validate(name: &str, sql: &str, viz: &str) -> Result<(), String> {
    if name.trim().is_empty() {
        return Err("a report needs a name".into());
    }
    if sql.trim().is_empty() {
        return Err("a report needs a SQL query".into());
    }
    if !viz.is_empty() && !VIZ.contains(&viz) {
        return Err(format!("viz must be one of {VIZ:?}"));
    }
    Ok(())
}

fn normalize_viz(viz: &str) -> String {
    if VIZ.contains(&viz) {
        viz.to_string()
    } else {
        "table".to_string()
    }
}

fn persist(path: &Path, f: &ReportFile) -> Result<(), String> {
    write_atomic(path, &serde_json::to_vec_pretty(f).map_err(|e| e.to_string())?)
}

// --- Dashboards store ---

#[derive(Debug, Default, Serialize, Deserialize)]
struct DashboardFile {
    next_id: u64,
    dashboards: BTreeMap<String, Dashboard>,
}

pub struct Dashboards {
    path: PathBuf,
    inner: RwLock<DashboardFile>,
}

impl Dashboards {
    pub fn open(path: &Path) -> Result<Self, String> {
        let inner = if path.exists() {
            serde_json::from_slice(&std::fs::read(path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?
        } else {
            DashboardFile::default()
        };
        Ok(Self {
            path: path.to_path_buf(),
            inner: RwLock::new(inner),
        })
    }

    pub fn list(&self) -> Vec<Dashboard> {
        self.inner
            .read()
            .unwrap()
            .dashboards
            .values()
            .cloned()
            .collect()
    }

    pub fn get(&self, id: &str) -> Option<Dashboard> {
        self.inner.read().unwrap().dashboards.get(id).cloned()
    }

    pub fn create(
        &self,
        name: &str,
        description: &str,
        tiles: Vec<Tile>,
        by: &str,
    ) -> Result<Dashboard, String> {
        if name.trim().is_empty() {
            return Err("a dashboard needs a name".into());
        }
        let mut f = self.inner.write().unwrap();
        f.next_id += 1;
        let now = unix_millis();
        let dashboard = Dashboard {
            id: format!("d{}", f.next_id),
            name: name.trim().to_string(),
            description: description.to_string(),
            tiles,
            created_by: by.to_string(),
            created_at: now,
            updated_at: now,
        };
        f.dashboards.insert(dashboard.id.clone(), dashboard.clone());
        persist_dashboards(&self.path, &*f)?;
        Ok(dashboard)
    }

    pub fn update(
        &self,
        id: &str,
        name: &str,
        description: &str,
        tiles: Vec<Tile>,
    ) -> Result<Dashboard, String> {
        if name.trim().is_empty() {
            return Err("a dashboard needs a name".into());
        }
        let mut f = self.inner.write().unwrap();
        let dashboard = f.dashboards.get_mut(id).ok_or("no such dashboard")?;
        dashboard.name = name.trim().to_string();
        dashboard.description = description.to_string();
        dashboard.tiles = tiles;
        dashboard.updated_at = unix_millis();
        let updated = dashboard.clone();
        persist_dashboards(&self.path, &*f)?;
        Ok(updated)
    }

    pub fn delete(&self, id: &str) -> Result<(), String> {
        let mut f = self.inner.write().unwrap();
        if f.dashboards.remove(id).is_none() {
            return Err("no such dashboard".into());
        }
        persist_dashboards(&self.path, &*f)
    }
}

fn persist_dashboards(path: &Path, f: &DashboardFile) -> Result<(), String> {
    write_atomic(path, &serde_json::to_vec_pretty(f).map_err(|e| e.to_string())?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "shardlite-console-{name}-{}-{}.json",
            std::process::id(),
            unix_millis()
        ))
    }

    #[test]
    fn reports_crud_round_trips_and_validates() {
        let path = temp("reports");
        let store = Reports::open(&path).unwrap();
        assert!(store.list().is_empty());

        // Validation.
        assert!(store.create("", "", None, "SELECT 1", "table", "u").is_err());
        assert!(store.create("r", "", None, "  ", "table", "u").is_err());
        assert!(store.create("r", "", None, "SELECT 1", "pie", "u").is_err());

        let r = store
            .create(
                "Revenue",
                "monthly",
                Some("local".into()),
                "SELECT sum(x) FROM t",
                "number",
                "alice",
            )
            .unwrap();
        assert_eq!(r.id, "r1");
        assert_eq!(r.viz, "number");
        assert_eq!(r.created_by, "alice");

        let r2 = store
            .update("r1", "Revenue v2", "", None, "SELECT count(*) FROM t", "table")
            .unwrap();
        assert_eq!(r2.name, "Revenue v2");
        assert_eq!(r2.connection, None);
        assert!(r2.updated_at >= r2.created_at);

        // Persisted and reloadable; ids do not collide with a second report.
        let r3 = store.create("Other", "", None, "SELECT 2", "", "bob").unwrap();
        assert_eq!(r3.id, "r2");
        let reopened = Reports::open(&path).unwrap();
        assert_eq!(reopened.list().len(), 2);
        assert_eq!(reopened.get("r1").unwrap().name, "Revenue v2");

        store.delete("r1").unwrap();
        assert!(store.get("r1").is_none());
        assert!(store.delete("missing").is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn dashboards_hold_report_tiles() {
        let path = temp("dashboards");
        let store = Dashboards::open(&path).unwrap();
        let d = store
            .create(
                "Ops",
                "",
                vec![Tile {
                    report_id: "r1".into(),
                    x: 0,
                    y: 0,
                    w: 2,
                    h: 1,
                    viz: None,
                }],
                "alice",
            )
            .unwrap();
        assert_eq!(d.id, "d1");
        assert_eq!(d.tiles.len(), 1);
        assert_eq!(d.tiles[0].report_id, "r1");

        let d2 = store.update("d1", "Ops board", "the ops board", vec![]).unwrap();
        assert!(d2.tiles.is_empty());
        assert_eq!(d2.name, "Ops board");

        store.delete("d1").unwrap();
        assert!(store.delete("d1").is_err());
        let _ = std::fs::remove_file(&path);
    }
}
