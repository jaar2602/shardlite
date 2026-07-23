//! The saved meshdb connections — the console's equivalent of a database client's connection
//! list. Each profile names a cluster's HTTP `/v1` edge and the meshdb credential to use against
//! it. The credential is a real secret, so it is **sealed at rest** (scoping decision 3): the
//! file stores ciphertext, and only the console master passphrase can open it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use rustls_pki_types::{pem::PemObject, CertificateDer};
use serde::{Deserialize, Serialize};

use crate::crypto::Sealer;

/// S3 replication settings for a connection — the non-secret half. The bucket a cluster archives
/// its shards to for HA. The secret key is sealed separately (see [`Record::s3_sealed_secret_key`]).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct S3Settings {
    pub bucket: String,
    pub region: String,
    /// e.g. `https://s3.us-east-1.amazonaws.com` or a MinIO endpoint. Empty = derive from region.
    pub endpoint: String,
    pub access_key: String,
    /// Key prefix under which shard snapshots and change-logs are stored.
    pub prefix: String,
    /// Whether replication to S3 is turned on for this cluster.
    pub enabled: bool,
}

/// What the API returns about a connection — never a secret.
#[derive(Debug, Clone, Serialize)]
pub struct ConnectionInfo {
    pub name: String,
    pub url: String,
    pub seeds: Vec<String>,
    pub meshdb_user: Option<String>,
    pub enabled: bool,
    pub timeout_ms: u64,
    pub allow_insecure_http: bool,
    pub custom_ca_pem: Option<String>,
    /// S3 replication config (non-secret); `None` if never configured.
    pub s3: Option<S3Settings>,
}

/// What the proxy needs to actually reach a cluster.
#[derive(Debug, Clone)]
pub struct Resolved {
    pub url: String,
    pub meshdb_user: Option<String>,
    pub meshdb_secret: Option<String>,
    pub timeout_ms: u64,
    pub custom_ca_pem: Option<String>,
    /// S3 replication config, decrypted, ready to push to the nodes via the `apply-s3` action
    /// (`POST /api/connections/<n>/apply-s3` → each node's `POST /v1/s3/config`).
    pub s3: Option<S3Settings>,
    pub s3_secret_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    name: String,
    /// Base URL of the cluster's HTTP edge, e.g. `http://10.0.0.5:4680`. No trailing slash.
    url: String,
    /// HTTP origins used to observe individual members. Empty in legacy records, where `url`
    /// remains the single seed.
    #[serde(default)]
    seeds: Vec<String>,
    /// meshdb username, if the cluster requires auth. Not a secret.
    meshdb_user: Option<String>,
    /// The meshdb secret, sealed. `None` when the cluster runs without auth.
    sealed_secret: Option<String>,
    #[serde(default = "default_enabled")]
    enabled: bool,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
    #[serde(default = "default_allow_insecure_http")]
    allow_insecure_http: bool,
    /// Optional private CA certificates. Certificates are public material, so these are stored
    /// as PEM rather than sealed like the cluster credential.
    #[serde(default)]
    custom_ca_pem: Option<String>,
    /// S3 replication settings (non-secret). Absent in records created before S3 support.
    #[serde(default)]
    s3: Option<S3Settings>,
    /// The S3 secret key, sealed like the cluster credential. `None` when S3 is unconfigured.
    #[serde(default)]
    s3_sealed_secret_key: Option<String>,
}

fn default_enabled() -> bool {
    true
}

fn default_timeout_ms() -> u64 {
    60_000
}

// Existing v1 profiles used HTTP and had no explicit acknowledgement bit. Preserve those on
// migration; every newly-created profile defaults to false at the API layer.
fn default_allow_insecure_http() -> bool {
    true
}

pub struct Registry {
    path: PathBuf,
    sealer: Sealer,
    map: RwLock<HashMap<String, Record>>,
    /// Last seed that produced a successful collector observation. Ephemeral by design: every
    /// restart rediscovers a reachable edge instead of pinning stale routing state to disk.
    preferred: RwLock<HashMap<String, String>>,
}

#[derive(Debug)]
pub enum RegistryError {
    Exists,
    NotFound,
    /// The stored secret would not decrypt — wrong master passphrase, or a tampered file.
    Unsealable,
    Disabled,
    Invalid(String),
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
            RegistryError::Disabled => write!(f, "this connection is disabled"),
            RegistryError::Invalid(e) => write!(f, "{e}"),
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
            preferred: RwLock::new(HashMap::new()),
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
    #[cfg(test)]
    fn put(
        &self,
        name: &str,
        url: &str,
        meshdb_user: Option<String>,
        meshdb_secret: Option<String>,
        replace: bool,
    ) -> Result<(), RegistryError> {
        self.put_config(
            name,
            url,
            meshdb_user,
            meshdb_secret,
            replace,
            true,
            default_timeout_ms(),
            true,
            None,
        )
    }

    #[cfg(test)]
    pub fn put_config(
        &self,
        name: &str,
        url: &str,
        meshdb_user: Option<String>,
        meshdb_secret: Option<String>,
        replace: bool,
        enabled: bool,
        timeout_ms: u64,
        allow_insecure_http: bool,
        custom_ca_pem: Option<String>,
    ) -> Result<(), RegistryError> {
        self.put_config_seeds(
            name,
            vec![url.to_string()],
            meshdb_user,
            meshdb_secret,
            replace,
            enabled,
            timeout_ms,
            allow_insecure_http,
            custom_ca_pem,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put_config_seeds(
        &self,
        name: &str,
        seeds: Vec<String>,
        meshdb_user: Option<String>,
        meshdb_secret: Option<String>,
        replace: bool,
        enabled: bool,
        timeout_ms: u64,
        allow_insecure_http: bool,
        custom_ca_pem: Option<String>,
        s3: Option<S3Settings>,
        s3_secret: Option<String>,
    ) -> Result<(), RegistryError> {
        validate_name(name)?;
        if seeds.is_empty() || seeds.len() > 32 {
            return Err(RegistryError::Invalid(
                "a connection requires between 1 and 32 database endpoints".into(),
            ));
        }
        let mut normalized = Vec::with_capacity(seeds.len());
        for seed in seeds {
            let seed = normalize_url(&seed, allow_insecure_http)?;
            if !normalized.contains(&seed) {
                normalized.push(seed);
            }
        }
        let custom_ca_pem = normalize_custom_ca(&normalized, custom_ca_pem)?;
        if !(1_000..=300_000).contains(&timeout_ms) {
            return Err(RegistryError::Invalid(
                "timeout_ms must be between 1000 and 300000".into(),
            ));
        }
        let mut map = self.map.write().unwrap();
        if !replace && map.contains_key(name) {
            return Err(RegistryError::Exists);
        }
        let sealed_secret = match meshdb_secret {
            Some(secret) => Some(self.sealer.seal(secret.as_bytes())),
            None if replace => map
                .get(name)
                .and_then(|record| record.sealed_secret.clone()),
            None => None,
        };
        // Same preserve-on-omit pattern as the cluster credential: a blank S3 secret on edit keeps
        // the stored one rather than clearing it.
        let s3_sealed_secret_key = match s3_secret {
            Some(secret) => Some(self.sealer.seal(secret.as_bytes())),
            None if replace => map
                .get(name)
                .and_then(|record| record.s3_sealed_secret_key.clone()),
            None => None,
        };
        map.insert(
            name.to_string(),
            Record {
                name: name.to_string(),
                url: normalized[0].clone(),
                seeds: normalized.clone(),
                meshdb_user,
                sealed_secret,
                enabled,
                timeout_ms,
                allow_insecure_http,
                custom_ca_pem,
                s3,
                s3_sealed_secret_key,
            },
        );
        self.persist(&map)?;
        self.preferred
            .write()
            .unwrap()
            .retain(|connection, seed| connection != name || normalized.contains(seed));
        Ok(())
    }

    pub fn delete(&self, name: &str) -> Result<(), RegistryError> {
        let mut map = self.map.write().unwrap();
        if map.remove(name).is_none() {
            return Err(RegistryError::NotFound);
        }
        self.persist(&map)?;
        self.preferred.write().unwrap().remove(name);
        Ok(())
    }

    pub fn list(&self) -> Vec<ConnectionInfo> {
        let map = self.map.read().unwrap();
        let mut out: Vec<_> = map
            .values()
            .map(|r| ConnectionInfo {
                name: r.name.clone(),
                url: record_seeds(r)[0].clone(),
                seeds: record_seeds(r),
                meshdb_user: r.meshdb_user.clone(),
                enabled: r.enabled,
                timeout_ms: r.timeout_ms,
                allow_insecure_http: r.allow_insecure_http,
                custom_ca_pem: r.custom_ca_pem.clone(),
                s3: r.s3.clone(),
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    pub fn names(&self) -> Vec<String> {
        let map = self.map.read().unwrap();
        let mut out: Vec<String> = map
            .values()
            .filter(|record| record.enabled)
            .map(|record| record.name.clone())
            .collect();
        out.sort();
        out
    }

    /// Open a connection's secret for the proxy. `Unsealable` means the master passphrase does
    /// not match what sealed this record — surfaced, never silently treated as "no auth".
    pub fn resolve(&self, name: &str) -> Result<Resolved, RegistryError> {
        let resolved = self.resolve_any(name)?;
        let enabled = self
            .map
            .read()
            .unwrap()
            .get(name)
            .is_some_and(|record| record.enabled);
        if !enabled {
            return Err(RegistryError::Disabled);
        }
        Ok(resolved)
    }

    /// Resolve a profile even when disabled, for an explicit administrator connection test.
    pub fn resolve_any(&self, name: &str) -> Result<Resolved, RegistryError> {
        let map = self.map.read().unwrap();
        let record = map.get(name).ok_or(RegistryError::NotFound)?;
        let seeds = record_seeds(record);
        let preferred = self.preferred.read().unwrap().get(name).cloned();
        let url = preferred
            .filter(|seed| seeds.contains(seed))
            .unwrap_or_else(|| seeds[0].clone());
        resolve_record(record, url, &self.sealer)
    }

    /// Resolve every configured seed for the bounded collector.
    pub fn resolve_seeds(&self, name: &str) -> Result<Vec<Resolved>, RegistryError> {
        let map = self.map.read().unwrap();
        let record = map.get(name).ok_or(RegistryError::NotFound)?;
        if !record.enabled {
            return Err(RegistryError::Disabled);
        }
        record_seeds(record)
            .into_iter()
            .map(|seed| resolve_record(record, seed, &self.sealer))
            .collect()
    }

    pub fn resolve_all_any(&self, name: &str) -> Result<Vec<Resolved>, RegistryError> {
        let map = self.map.read().unwrap();
        let record = map.get(name).ok_or(RegistryError::NotFound)?;
        record_seeds(record)
            .into_iter()
            .map(|seed| resolve_record(record, seed, &self.sealer))
            .collect()
    }

    /// Resolve an unregistered endpoint with an existing profile's credentials and transport
    /// policy. Used only for read-only node verification; it does not add the endpoint.
    pub fn resolve_candidate(&self, name: &str, raw_url: &str) -> Result<Resolved, RegistryError> {
        let map = self.map.read().unwrap();
        let record = map.get(name).ok_or(RegistryError::NotFound)?;
        let url = normalize_url(raw_url, record.allow_insecure_http)?;
        resolve_record(record, url, &self.sealer)
    }

    pub fn mark_preferred(&self, name: &str, seed: &str) {
        let known = self
            .map
            .read()
            .unwrap()
            .get(name)
            .is_some_and(|record| record_seeds(record).iter().any(|url| url == seed));
        if known {
            self.preferred
                .write()
                .unwrap()
                .insert(name.to_string(), seed.to_string());
        }
    }

    pub fn preferred_seed(&self, name: &str) -> Option<String> {
        self.preferred.read().unwrap().get(name).cloned()
    }

    /// Atomically decrypt every saved cluster secret with the current key and reseal it with a
    /// new key. Nothing is written until all existing ciphertext has authenticated successfully.
    pub fn rotate_key(&mut self, new_sealer: Sealer) -> Result<(), RegistryError> {
        let mut updated = self.map.read().unwrap().clone();
        for record in updated.values_mut() {
            if let Some(sealed) = &record.sealed_secret {
                let plaintext = self.sealer.open(sealed).ok_or(RegistryError::Unsealable)?;
                record.sealed_secret = Some(new_sealer.seal(&plaintext));
            }
            if let Some(sealed) = &record.s3_sealed_secret_key {
                let plaintext = self.sealer.open(sealed).ok_or(RegistryError::Unsealable)?;
                record.s3_sealed_secret_key = Some(new_sealer.seal(&plaintext));
            }
        }
        self.persist(&updated)?;
        *self.map.write().unwrap() = updated;
        self.sealer = new_sealer;
        Ok(())
    }
}

fn resolve_record(
    record: &Record,
    url: String,
    sealer: &Sealer,
) -> Result<Resolved, RegistryError> {
    let meshdb_secret = match &record.sealed_secret {
        None => None,
        Some(sealed) => {
            let bytes = sealer.open(sealed).ok_or(RegistryError::Unsealable)?;
            Some(String::from_utf8(bytes).map_err(|_| RegistryError::Unsealable)?)
        }
    };
    let s3_secret_key = match &record.s3_sealed_secret_key {
        None => None,
        Some(sealed) => {
            let bytes = sealer.open(sealed).ok_or(RegistryError::Unsealable)?;
            Some(String::from_utf8(bytes).map_err(|_| RegistryError::Unsealable)?)
        }
    };
    Ok(Resolved {
        url,
        meshdb_user: record.meshdb_user.clone(),
        meshdb_secret,
        timeout_ms: record.timeout_ms,
        custom_ca_pem: record.custom_ca_pem.clone(),
        s3: record.s3.clone(),
        s3_secret_key,
    })
}

fn record_seeds(record: &Record) -> Vec<String> {
    if record.seeds.is_empty() {
        vec![record.url.clone()]
    } else {
        record.seeds.clone()
    }
}

fn validate_name(name: &str) -> Result<(), RegistryError> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return Err(RegistryError::Invalid(
            "name must be 1-64 ASCII letters, numbers, dots, dashes, or underscores".into(),
        ));
    }
    Ok(())
}

fn normalize_url(raw: &str, allow_insecure_http: bool) -> Result<String, RegistryError> {
    let mut url = url::Url::parse(raw)
        .map_err(|_| RegistryError::Invalid("URL must be an absolute http or https URL".into()))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(RegistryError::Invalid(
            "URL must be an absolute http or https URL".into(),
        ));
    }
    if url.scheme() == "http" && !allow_insecure_http {
        return Err(RegistryError::Invalid(
            "plaintext HTTP requires allow_insecure_http=true; use HTTPS in production".into(),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(RegistryError::Invalid(
            "URL must not contain embedded credentials".into(),
        ));
    }
    if url.query().is_some() || url.fragment().is_some() || !matches!(url.path(), "" | "/") {
        return Err(RegistryError::Invalid(
            "URL must be an origin without a path, query, or fragment".into(),
        ));
    }
    url.set_path("");
    Ok(url.as_str().trim_end_matches('/').to_string())
}

fn normalize_custom_ca(
    seeds: &[String],
    custom_ca_pem: Option<String>,
) -> Result<Option<String>, RegistryError> {
    let Some(pem) = custom_ca_pem.filter(|pem| !pem.trim().is_empty()) else {
        return Ok(None);
    };
    if seeds.iter().any(|url| !url.starts_with("https://")) {
        return Err(RegistryError::Invalid(
            "a custom CA can only be used with an HTTPS connection".into(),
        ));
    }
    let mut count = 0;
    let mut roots = rustls::RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(pem.as_bytes()) {
        let certificate = certificate.map_err(|_| {
            RegistryError::Invalid("custom CA must contain valid PEM certificates".into())
        })?;
        roots.add(certificate).map_err(|_| {
            RegistryError::Invalid("custom CA contains an invalid certificate".into())
        })?;
        count += 1;
    }
    if count == 0 {
        return Err(RegistryError::Invalid(
            "custom CA must contain at least one PEM certificate".into(),
        ));
    }
    Ok(Some(pem))
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
        assert!(listed[0].enabled);
        assert_eq!(listed[0].timeout_ms, 60_000);

        // ...but resolve opens it for the proxy.
        let r = reg.resolve("prod").unwrap();
        assert_eq!(r.meshdb_secret.as_deref(), Some("s3cret"));

        // and the on-disk file must not contain the plaintext.
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(!raw.contains("s3cret"));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn s3_config_round_trips_and_seals_its_key() {
        let path = tmp();
        let reg = Registry::open(&path, Sealer::from_passphrase("master")).unwrap();
        reg.put_config_seeds(
            "prod",
            vec!["http://10.0.0.5:4680".into()],
            None,
            None,
            false,
            true,
            60_000,
            true,
            None,
            Some(S3Settings {
                bucket: "meshdb-backup".into(),
                region: "us-east-1".into(),
                endpoint: String::new(),
                access_key: "AKIAEXAMPLE".into(),
                prefix: "cluster-a".into(),
                enabled: true,
            }),
            Some("s3-secret-key-material".into()),
        )
        .unwrap();

        // list surfaces the non-secret S3 settings, never the key.
        let listed = reg.list();
        let s3 = listed[0].s3.as_ref().unwrap();
        assert_eq!(s3.bucket, "meshdb-backup");
        assert_eq!(s3.access_key, "AKIAEXAMPLE");
        assert!(s3.enabled);

        // resolve decrypts the secret key for the proxy.
        let r = reg.resolve("prod").unwrap();
        assert_eq!(r.s3_secret_key.as_deref(), Some("s3-secret-key-material"));
        assert_eq!(r.s3.as_ref().unwrap().bucket, "meshdb-backup");

        // the sealed key is never written to disk in plaintext.
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(!raw.contains("s3-secret-key-material"));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn editing_a_connection_preserves_the_s3_key_when_omitted() {
        let path = tmp();
        let reg = Registry::open(&path, Sealer::from_passphrase("master")).unwrap();
        let s3 = S3Settings {
            bucket: "b".into(),
            enabled: true,
            ..Default::default()
        };
        reg.put_config_seeds(
            "c",
            vec!["http://h:1".into()],
            None,
            None,
            false,
            true,
            60_000,
            true,
            None,
            Some(s3.clone()),
            Some("key1".into()),
        )
        .unwrap();
        // Edit (replace) without re-supplying the secret → the stored key survives.
        reg.put_config_seeds(
            "c",
            vec!["http://h:1".into()],
            None,
            None,
            true,
            true,
            60_000,
            true,
            None,
            Some(s3),
            None,
        )
        .unwrap();
        let r = reg.resolve("c").unwrap();
        assert_eq!(
            r.s3_secret_key.as_deref(),
            Some("key1"),
            "the S3 key must survive an edit that omits it"
        );
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

    #[test]
    fn invalid_origins_and_unsafe_names_are_rejected() {
        let path = tmp();
        let reg = Registry::open(&path, Sealer::from_passphrase("master")).unwrap();
        assert!(matches!(
            reg.put("bad/name", "http://host:1", None, None, false),
            Err(RegistryError::Invalid(_))
        ));
        assert!(matches!(
            reg.put("ok", "file:///tmp/db", None, None, false),
            Err(RegistryError::Invalid(_))
        ));
        assert!(matches!(
            reg.put("ok", "http://user:pw@host:1", None, None, false),
            Err(RegistryError::Invalid(_))
        ));
        assert!(matches!(
            reg.put_config(
                "ok",
                "http://host:1",
                None,
                None,
                false,
                true,
                60_000,
                false,
                None,
            ),
            Err(RegistryError::Invalid(_))
        ));
    }

    #[test]
    fn editing_preserves_an_omitted_secret_and_disabled_profiles_do_not_resolve() {
        let path = tmp();
        let reg = Registry::open(&path, Sealer::from_passphrase("master")).unwrap();
        reg.put(
            "prod",
            "http://host:1",
            Some("app".into()),
            Some("secret".into()),
            false,
        )
        .unwrap();
        reg.put_config(
            "prod",
            "https://host:2",
            Some("app".into()),
            None,
            true,
            false,
            10_000,
            true,
            None,
        )
        .unwrap();
        let resolved = reg.resolve_any("prod").unwrap();
        assert_eq!(resolved.meshdb_secret.as_deref(), Some("secret"));
        assert_eq!(resolved.timeout_ms, 10_000);
        assert!(matches!(reg.resolve("prod"), Err(RegistryError::Disabled)));
        assert!(reg.names().is_empty());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn key_rotation_reseals_all_secrets_atomically() {
        let path = tmp();
        let mut reg = Registry::open(&path, Sealer::from_passphrase("old")).unwrap();
        reg.put(
            "prod",
            "http://host:1",
            Some("app".into()),
            Some("secret".into()),
            false,
        )
        .unwrap();
        let before = std::fs::read_to_string(&path).unwrap();

        reg.rotate_key(Sealer::from_passphrase("new")).unwrap();
        let after = std::fs::read_to_string(&path).unwrap();
        assert_ne!(before, after);
        assert_eq!(
            reg.resolve("prod").unwrap().meshdb_secret.as_deref(),
            Some("secret")
        );
        drop(reg);
        assert!(matches!(
            Registry::open(&path, Sealer::from_passphrase("old"))
                .unwrap()
                .resolve("prod"),
            Err(RegistryError::Unsealable)
        ));
        assert_eq!(
            Registry::open(&path, Sealer::from_passphrase("new"))
                .unwrap()
                .resolve("prod")
                .unwrap()
                .meshdb_secret
                .as_deref(),
            Some("secret")
        );
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn custom_ca_requires_https_and_a_valid_certificate() {
        let path = tmp();
        let reg = Registry::open(&path, Sealer::from_passphrase("master")).unwrap();
        let malformed_der = "-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n";
        assert!(matches!(
            reg.put_config(
                "prod",
                "https://host:443",
                None,
                None,
                false,
                true,
                60_000,
                false,
                Some(malformed_der.into()),
            ),
            Err(RegistryError::Invalid(_))
        ));
        assert!(matches!(
            reg.put_config(
                "prod",
                "http://host:80",
                None,
                None,
                false,
                true,
                60_000,
                true,
                Some(malformed_der.into()),
            ),
            Err(RegistryError::Invalid(_))
        ));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn multi_seed_profiles_deduplicate_and_prefer_a_successful_seed() {
        let path = tmp();
        let reg = Registry::open(&path, Sealer::from_passphrase("master")).unwrap();
        reg.put_config_seeds(
            "prod",
            vec![
                "https://node-1:4680/".into(),
                "https://node-2:4680".into(),
                "https://node-1:4680".into(),
            ],
            None,
            None,
            false,
            true,
            5_000,
            false,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(reg.list()[0].seeds.len(), 2);
        assert_eq!(reg.resolve_seeds("prod").unwrap().len(), 2);
        assert_eq!(reg.resolve("prod").unwrap().url, "https://node-1:4680");
        reg.mark_preferred("prod", "https://node-2:4680");
        assert_eq!(reg.resolve("prod").unwrap().url, "https://node-2:4680");
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn candidate_resolution_reuses_credentials_without_registering_the_endpoint() {
        let path = tmp();
        let reg = Registry::open(&path, Sealer::from_passphrase("master")).unwrap();
        reg.put_config(
            "prod",
            "https://node-1:4680",
            Some("app".into()),
            Some("secret".into()),
            false,
            true,
            8_000,
            false,
            None,
        )
        .unwrap();
        let candidate = reg
            .resolve_candidate("prod", "https://node-2:4680/")
            .unwrap();
        assert_eq!(candidate.url, "https://node-2:4680");
        assert_eq!(candidate.meshdb_user.as_deref(), Some("app"));
        assert_eq!(candidate.meshdb_secret.as_deref(), Some("secret"));
        assert_eq!(reg.list()[0].seeds, vec!["https://node-1:4680"]);
        std::fs::remove_file(&path).ok();
    }
}
