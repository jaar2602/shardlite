//! Saved **S3 connections**: named, reusable archival targets (bucket + endpoint + credentials)
//! persisted in the console so a target survives page reloads and node restarts. The secret access
//! key is **sealed** with the same [`Sealer`](crate::crypto::Sealer) as every other secret; the rest
//! is plain. One saved connection is *activated* onto a cluster (pushed to `/v1/s3/config`) to become
//! its snapshot target — the UI labels whichever one matches the cluster's live S3 status.
//! Persisted to `s3conns.json`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::crypto::Sealer;
use crate::store::write_atomic;

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Non-secret view returned by the API (never the secret key).
#[derive(Debug, Clone, Serialize)]
pub struct S3ConnView {
    pub id: String,
    pub name: String,
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
    pub prefix: String,
    pub access_key: String,
    /// Whether a secret key is stored (so the UI shows configured/not without revealing it).
    pub has_secret: bool,
    pub created_by: String,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Stored {
    id: String,
    name: String,
    #[serde(default)]
    bucket: String,
    #[serde(default)]
    endpoint: String,
    #[serde(default)]
    region: String,
    #[serde(default)]
    prefix: String,
    #[serde(default)]
    access_key: String,
    #[serde(default)]
    sealed_secret: Option<String>,
    created_by: String,
    created_at: u64,
    updated_at: u64,
}

/// Decrypted credentials, for pushing a saved connection onto a cluster's `/v1/s3/config`.
#[derive(Debug, Clone)]
pub struct ResolvedS3 {
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
    pub prefix: String,
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct S3File {
    next_id: u64,
    conns: BTreeMap<String, Stored>,
}

pub struct S3Connections {
    path: PathBuf,
    sealer: Sealer,
    inner: RwLock<S3File>,
}

impl S3Connections {
    pub fn open(path: &Path, sealer: Sealer) -> Result<Self, String> {
        let inner = if path.exists() {
            serde_json::from_slice(&std::fs::read(path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?
        } else {
            S3File::default()
        };
        Ok(Self {
            path: path.to_path_buf(),
            sealer,
            inner: RwLock::new(inner),
        })
    }

    pub fn list(&self) -> Vec<S3ConnView> {
        self.inner.read().unwrap().conns.values().map(view).collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create(
        &self,
        name: &str,
        bucket: &str,
        endpoint: &str,
        region: &str,
        prefix: &str,
        access_key: &str,
        secret_key: Option<String>,
        by: &str,
    ) -> Result<S3ConnView, String> {
        validate(name, bucket)?;
        let mut f = self.inner.write().unwrap();
        f.next_id += 1;
        let now = unix_millis();
        let stored = Stored {
            id: format!("s{}", f.next_id),
            name: name.trim().to_string(),
            bucket: bucket.trim().to_string(),
            endpoint: endpoint.trim().to_string(),
            region: region.trim().to_string(),
            prefix: prefix.trim().to_string(),
            access_key: access_key.trim().to_string(),
            sealed_secret: self.seal(secret_key),
            created_by: by.to_string(),
            created_at: now,
            updated_at: now,
        };
        let v = view(&stored);
        f.conns.insert(stored.id.clone(), stored);
        persist(&self.path, &f)?;
        Ok(v)
    }

    /// Update a saved connection. `secret_key` follows preserve-on-omit: `None` keeps the stored
    /// secret, `Some("")` clears it, `Some(k)` seals a new one.
    #[allow(clippy::too_many_arguments)]
    pub fn update(
        &self,
        id: &str,
        name: &str,
        bucket: &str,
        endpoint: &str,
        region: &str,
        prefix: &str,
        access_key: &str,
        secret_key: Option<String>,
    ) -> Result<S3ConnView, String> {
        validate(name, bucket)?;
        let mut f = self.inner.write().unwrap();
        let c = f.conns.get_mut(id).ok_or("no such S3 connection")?;
        c.name = name.trim().to_string();
        c.bucket = bucket.trim().to_string();
        c.endpoint = endpoint.trim().to_string();
        c.region = region.trim().to_string();
        c.prefix = prefix.trim().to_string();
        c.access_key = access_key.trim().to_string();
        match secret_key {
            Some(k) if k.is_empty() => c.sealed_secret = None,
            Some(k) => c.sealed_secret = Some(self.sealer.seal(k.as_bytes())),
            None => {}
        }
        c.updated_at = unix_millis();
        let v = view(c);
        persist(&self.path, &f)?;
        Ok(v)
    }

    pub fn delete(&self, id: &str) -> Result<(), String> {
        let mut f = self.inner.write().unwrap();
        if f.conns.remove(id).is_none() {
            return Err("no such S3 connection".into());
        }
        persist(&self.path, &f)
    }

    /// Decrypt a saved connection for activation. `None` if it doesn't exist or has no secret.
    pub fn resolved(&self, id: &str) -> Option<ResolvedS3> {
        let f = self.inner.read().unwrap();
        let c = f.conns.get(id)?;
        let secret_key = c
            .sealed_secret
            .as_ref()
            .and_then(|s| self.sealer.open(s))
            .map(|b| String::from_utf8_lossy(&b).into_owned())?;
        Some(ResolvedS3 {
            bucket: c.bucket.clone(),
            endpoint: c.endpoint.clone(),
            region: c.region.clone(),
            prefix: c.prefix.clone(),
            access_key: c.access_key.clone(),
            secret_key,
        })
    }

    fn seal(&self, secret: Option<String>) -> Option<String> {
        secret
            .filter(|s| !s.is_empty())
            .map(|s| self.sealer.seal(s.as_bytes()))
    }
}

fn view(s: &Stored) -> S3ConnView {
    S3ConnView {
        id: s.id.clone(),
        name: s.name.clone(),
        bucket: s.bucket.clone(),
        endpoint: s.endpoint.clone(),
        region: s.region.clone(),
        prefix: s.prefix.clone(),
        access_key: s.access_key.clone(),
        has_secret: s.sealed_secret.is_some(),
        created_by: s.created_by.clone(),
        updated_at: s.updated_at,
    }
}

fn validate(name: &str, bucket: &str) -> Result<(), String> {
    if name.trim().is_empty() {
        return Err("an S3 connection needs a name".into());
    }
    if bucket.trim().is_empty() {
        return Err("an S3 connection needs a bucket".into());
    }
    Ok(())
}

fn persist(path: &Path, f: &S3File) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(f).map_err(|e| e.to_string())?;
    write_atomic(path, &bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn crud_seals_the_secret_and_preserves_on_omit() {
        let path = std::env::temp_dir().join(format!(
            "shardlite-console-s3conns-{}-{}.json",
            std::process::id(),
            unix_millis(),
        ));
        let _cleanup = Cleanup(path.clone());
        let store = S3Connections::open(&path, Sealer::from_passphrase("k")).unwrap();

        let v = store
            .create("prod", "my-bucket", "https://minio.local", "us-east-1", "shardlite/", "AK", Some("SECRET".into()), "admin")
            .unwrap();
        assert_eq!(v.id, "s1");
        assert!(v.has_secret);

        // The plaintext secret is never on disk.
        let on_disk = std::fs::read_to_string(&path).unwrap();
        assert!(!on_disk.contains("SECRET"), "the secret must be sealed on disk");

        // resolved() decrypts it.
        let r = store.resolved("s1").unwrap();
        assert_eq!(r.secret_key, "SECRET");
        assert_eq!(r.bucket, "my-bucket");

        // Update with no secret preserves it; renaming works.
        store
            .update("s1", "prod-eu", "my-bucket", "https://minio.local", "eu-west-1", "shardlite/", "AK", None)
            .unwrap();
        let r = store.resolved("s1").unwrap();
        assert_eq!(r.secret_key, "SECRET");
        assert_eq!(r.region, "eu-west-1");

        // Reopen reads it back through the seal.
        let reopened = S3Connections::open(&path, Sealer::from_passphrase("k")).unwrap();
        assert_eq!(reopened.resolved("s1").unwrap().secret_key, "SECRET");
        assert_eq!(reopened.list().len(), 1);

        // Delete.
        store.delete("s1").unwrap();
        assert!(store.list().is_empty());
        assert!(store.delete("s1").is_err());
    }
}
