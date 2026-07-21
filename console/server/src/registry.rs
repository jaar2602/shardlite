//! The saved meshdb connections — the console's equivalent of a database client's connection
//! list. Each profile names a cluster's HTTP `/v1` edge and the meshdb credential to use against
//! it. The credential is a real secret, so it is **sealed at rest** (scoping decision 3): the
//! file stores ciphertext, and only the console master passphrase can open it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::crypto::Sealer;

/// What the API returns about a connection — never the secret.
#[derive(Debug, Clone, Serialize)]
pub struct ConnectionInfo {
    pub name: String,
    pub url: String,
    pub meshdb_user: Option<String>,
}

/// What the proxy needs to actually reach a cluster.
#[derive(Debug, Clone)]
pub struct Resolved {
    pub url: String,
    pub meshdb_user: Option<String>,
    pub meshdb_secret: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    name: String,
    /// Base URL of the cluster's HTTP edge, e.g. `http://10.0.0.5:4680`. No trailing slash.
    url: String,
    /// meshdb username, if the cluster requires auth. Not a secret.
    meshdb_user: Option<String>,
    /// The meshdb secret, sealed. `None` when the cluster runs without auth.
    sealed_secret: Option<String>,
}

pub struct Registry {
    path: PathBuf,
    sealer: Sealer,
    map: RwLock<HashMap<String, Record>>,
}

#[derive(Debug)]
pub enum RegistryError {
    Exists,
    NotFound,
    /// The stored secret would not decrypt — wrong master passphrase, or a tampered file.
    Unsealable,
    Io(String),
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryError::Exists => write!(f, "a connection with that name already exists"),
            RegistryError::NotFound => write!(f, "no such connection"),
            RegistryError::Unsealable => write!(
                f,
                "the stored secret could not be decrypted — wrong master passphrase, or the \
                 connections file has been tampered with"
            ),
            RegistryError::Io(e) => write!(f, "{e}"),
        }
    }
}

impl Registry {
    pub fn open(path: &Path, sealer: Sealer) -> Result<Self, RegistryError> {
        let map = if path.exists() {
            let bytes = std::fs::read(path).map_err(|e| RegistryError::Io(e.to_string()))?;
            let records: Vec<Record> =
                serde_json::from_slice(&bytes).map_err(|e| RegistryError::Io(e.to_string()))?;
            records.into_iter().map(|r| (r.name.clone(), r)).collect()
        } else {
            HashMap::new()
        };
        Ok(Registry {
            path: path.to_path_buf(),
            sealer,
            map: RwLock::new(map),
        })
    }

    fn persist(&self, map: &HashMap<String, Record>) -> Result<(), RegistryError> {
        let mut records: Vec<&Record> = map.values().collect();
        records.sort_by(|a, b| a.name.cmp(&b.name));
        let json =
            serde_json::to_vec_pretty(&records).map_err(|e| RegistryError::Io(e.to_string()))?;
        crate::store::write_atomic(&self.path, &json).map_err(RegistryError::Io)
    }

    /// Add or replace a connection. Admin-gated at the API layer. The secret is sealed before it
    /// touches disk. A trailing slash on `url` is trimmed so proxy path-joining is unambiguous.
    pub fn put(
        &self,
        name: &str,
        url: &str,
        meshdb_user: Option<String>,
        meshdb_secret: Option<String>,
        replace: bool,
    ) -> Result<(), RegistryError> {
        let mut map = self.map.write().unwrap();
        if !replace && map.contains_key(name) {
            return Err(RegistryError::Exists);
        }
        let sealed_secret = meshdb_secret.map(|s| self.sealer.seal(s.as_bytes()));
        map.insert(
            name.to_string(),
            Record {
                name: name.to_string(),
                url: url.trim_end_matches('/').to_string(),
                meshdb_user,
                sealed_secret,
            },
        );
        self.persist(&map)
    }

    pub fn delete(&self, name: &str) -> Result<(), RegistryError> {
        let mut map = self.map.write().unwrap();
        if map.remove(name).is_none() {
            return Err(RegistryError::NotFound);
        }
        self.persist(&map)
    }

    pub fn list(&self) -> Vec<ConnectionInfo> {
        let map = self.map.read().unwrap();
        let mut out: Vec<_> = map
            .values()
            .map(|r| ConnectionInfo {
                name: r.name.clone(),
                url: r.url.clone(),
                meshdb_user: r.meshdb_user.clone(),
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    pub fn names(&self) -> Vec<String> {
        let map = self.map.read().unwrap();
        let mut out: Vec<String> = map.keys().cloned().collect();
        out.sort();
        out
    }

    /// Open a connection's secret for the proxy. `Unsealable` means the master passphrase does
    /// not match what sealed this record — surfaced, never silently treated as "no auth".
    pub fn resolve(&self, name: &str) -> Result<Resolved, RegistryError> {
        let map = self.map.read().unwrap();
        let record = map.get(name).ok_or(RegistryError::NotFound)?;
        let meshdb_secret = match &record.sealed_secret {
            None => None,
            Some(sealed) => {
                let bytes = self.sealer.open(sealed).ok_or(RegistryError::Unsealable)?;
                Some(String::from_utf8(bytes).map_err(|_| RegistryError::Unsealable)?)
            }
        };
        Ok(Resolved {
            url: record.url.clone(),
            meshdb_user: record.meshdb_user.clone(),
            meshdb_secret,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> PathBuf {
        let mut p = std::env::temp_dir();
        let n: u64 = rand::random();
        p.push(format!("meshdb-console-conns-{n}.json"));
        p
    }

    #[test]
    fn a_connection_round_trips_and_hides_its_secret() {
        let path = tmp();
        let reg = Registry::open(&path, Sealer::from_passphrase("master")).unwrap();
        reg.put(
            "prod",
            "http://10.0.0.5:4680/",
            Some("app".into()),
            Some("s3cret".into()),
            false,
        )
        .unwrap();

        // list never exposes the secret...
        let listed = reg.list();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].url, "http://10.0.0.5:4680"); // trailing slash trimmed

        // ...but resolve opens it for the proxy.
        let r = reg.resolve("prod").unwrap();
        assert_eq!(r.meshdb_secret.as_deref(), Some("s3cret"));

        // and the on-disk file must not contain the plaintext.
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(!raw.contains("s3cret"));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn a_wrong_master_passphrase_fails_loudly_not_silently() {
        let path = tmp();
        {
            let reg = Registry::open(&path, Sealer::from_passphrase("right")).unwrap();
            reg.put("c", "http://h:1", Some("u".into()), Some("x".into()), false)
                .unwrap();
        }
        let reg = Registry::open(&path, Sealer::from_passphrase("wrong")).unwrap();
        assert!(matches!(reg.resolve("c"), Err(RegistryError::Unsealable)));
        std::fs::remove_file(&path).ok();
    }
}
