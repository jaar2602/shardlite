//! Who may connect, and what they may do once they have.
//!
//! # The mechanism: challenge–response, because there is no TLS
//!
//! This protocol runs over plain TCP. A password sent in `Hello` would cross the wire in
//! clear, and every network hop would learn it. Instead the server sends a fresh random
//! nonce and the client answers with `blake3::keyed_hash(key, nonce)` — proof it holds the
//! key, without the key ever leaving either machine. A recorded handshake is useless against
//! any other connection, because the nonce is never reused.
//!
//! # What this does and does not protect
//!
//! Stated plainly, because security features that overstate themselves are worse than none:
//! this stops **unauthorized access** — an attacker who can reach the port cannot read or
//! write data, join the cluster, or subscribe to the replication stream. It does **not**
//! encrypt anything: an attacker who can capture traffic still sees queries and rows, and an
//! active man-in-the-middle can hijack a connection after its handshake. On a hostile
//! network, run this inside a tunnel (WireGuard, an SSH tunnel, a service mesh) — that is a
//! deliberate scope line, not an oversight, because pulling a TLS stack into a 0.5 GB
//! footprint is a decision the operator should make, not this crate.
//!
//! # Roles: clients and cluster members are different species
//!
//! `Read` < `Write` < `Admin` order themselves — each includes the previous. `Cluster` is
//! deliberately **not** on that ladder. The cluster verbs — votes, heartbeats, subscription,
//! snapshots, forwarded requests — would let any holder disrupt elections or copy entire
//! shards wholesale. An administrator's credentials being stolen should not hand an attacker
//! the replication stream, and a peer node's credentials should not run ad-hoc queries.
//!
//! # The server never stores the secret
//!
//! The server keeps `blake3(secret)`, not the secret. A stolen server config still
//! authenticates to *this* cluster — that is unavoidable in any shared-key MAC scheme — but
//! it does not reveal the secret itself, which people reuse elsewhere despite every warning.

use std::collections::BTreeMap;

use crate::error::{Error, Result};

use super::protocol::Request;

/// What an authenticated principal may do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Role {
    /// Queries only.
    Read,
    /// Reads plus single-shard writes.
    Write,
    /// Everything a client can do, including cluster-wide DDL.
    Admin,
    /// Node-to-node verbs: election, replication, snapshots, forwarded requests. Not on the
    /// client ladder — see the module docs for why.
    Cluster,
}

/// What a request demands of the connection making it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Requirement {
    /// The handshake itself; nothing to demand yet.
    Handshake,
    Read,
    Write,
    Admin,
    Cluster,
}

/// The demand each request makes. Exhaustive on purpose: a new request variant must decide
/// its requirement here or fail to compile, rather than defaulting to something.
pub fn required(req: &Request) -> Requirement {
    match req {
        Request::Hello { .. } | Request::Auth { .. } | Request::JoinWithToken { .. } => Requirement::Handshake,

        Request::Query { .. }
        | Request::QueryAll { .. }
        | Request::Route { .. }
        | Request::Info => Requirement::Read,

        Request::Execute { .. } | Request::Transaction { .. } => Requirement::Write,

        // A routed statement is whatever its verb is: DDL reshapes every shard (an operator
        // action), a write needs Write, everything else is a read.
        Request::Run { statement } => match crate::db::first_keyword(&statement.sql).as_str() {
            "CREATE" | "DROP" | "ALTER" => Requirement::Admin,
            "INSERT" | "UPDATE" | "DELETE" | "REPLACE" => Requirement::Write,
            _ => Requirement::Read,
        },

        // Cluster-wide DDL reshapes every shard; that is an operator action, not a client one.
        Request::ExecuteAll { .. } | Request::SchemaApply { .. } => Requirement::Admin,

        // User management is the most privileged client action there is.
        Request::CreateUser { .. } | Request::DropUser { .. } | Request::ListUsers => {
            Requirement::Admin
        }

        // Subscription and snapshots hand out entire shards; votes and heartbeats steer the
        // cluster; Direct is how peers forward work they have already authorized.
        Request::Subscribe { .. }
        | Request::SnapshotBegin { .. }
        | Request::SnapshotRead { .. }
        | Request::SnapshotEnd { .. }
        | Request::SplitImageInfo { .. }
        | Request::SplitImageRead { .. }
        | Request::Vote(_)
        | Request::Beat(_)
        | Request::CatalogGet
        | Request::CatalogPrepare { .. }
        | Request::CatalogCommit { .. }
        | Request::CatalogCommand(_)
        | Request::CatalogInstall { .. }
        // A batched fan-out is coordinator-to-owner, on the cluster's own authority. Reading a
        // shard's schema version is the same shape and changes nothing.
        | Request::ShardBatch { .. }
        | Request::SchemaVersions { .. }
        | Request::Direct(_) => Requirement::Cluster,
        Request::RoutedDirect { .. } => Requirement::Cluster,
    }
}

impl Role {
    /// Whether this role satisfies a requirement.
    pub fn permits(self, need: Requirement) -> bool {
        match need {
            Requirement::Handshake => true,
            Requirement::Read => matches!(self, Role::Read | Role::Write | Role::Admin),
            Requirement::Write => matches!(self, Role::Write | Role::Admin),
            Requirement::Admin => matches!(self, Role::Admin),
            // Strictly the cluster role. An admin is a person; peers are machines. Neither
            // should be able to impersonate the other with the same credentials.
            Requirement::Cluster => matches!(self, Role::Cluster),
        }
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Role::Read => "read",
            Role::Write => "write",
            Role::Admin => "admin",
            Role::Cluster => "cluster",
        };
        f.write_str(s)
    }
}

impl std::str::FromStr for Role {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "read" => Ok(Role::Read),
            "write" => Ok(Role::Write),
            "admin" => Ok(Role::Admin),
            "cluster" => Ok(Role::Cluster),
            other => Err(Error::Protocol(format!(
                "unknown role '{other}'; expected read, write, admin or cluster"
            ))),
        }
    }
}

/// A key derived from a secret. This — not the secret — is what the server stores and what
/// the MAC is keyed with.
pub type Key = [u8; 32];

pub fn derive_key(secret: &str) -> Key {
    *blake3::hash(secret.as_bytes()).as_bytes()
}

/// The client's answer to a challenge.
pub fn prove(key: &Key, nonce: &[u8; 32]) -> [u8; 32] {
    *blake3::keyed_hash(key, nonce).as_bytes()
}

/// A fresh 32-byte nonce from the operating system's entropy pool.
///
/// Fails closed: a nonce that cannot be produced refuses the connection rather than falling
/// back to something guessable — a predictable nonce turns the whole handshake into a
/// replayable password.
pub fn nonce() -> Result<[u8; 32]> {
    use std::io::Read;
    // `read_exact`, never `fs::read`: urandom has no end-of-file, so a whole-file read
    // would block forever filling memory.
    let mut out = [0u8; 32];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut out))
        .map_err(|e| Error::Protocol(format!("reading /dev/urandom: {e}")))?;
    Ok(out)
}

#[derive(Debug, Clone, Copy)]
struct User {
    key: Key,
    role: Role,
}

const USERS_HEADER: &str = "shardlite-users-v1";

/// The users a server accepts, and — when opened from a file — the file it persists to.
///
/// Mutable at runtime behind an `RwLock`: an admin can create and drop users while the server
/// runs, and every change is written back durably so it survives a restart. Empty means
/// authentication is not configured and the server is open, which it announces loudly rather
/// than quietly being.
#[derive(Debug)]
pub struct AuthConfig {
    users: std::sync::RwLock<BTreeMap<String, User>>,
    /// Where mutations are persisted. `None` for an in-memory config (tests, or a server
    /// whose users are fixed in code).
    path: Option<std::path::PathBuf>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            users: std::sync::RwLock::new(BTreeMap::new()),
            path: None,
        }
    }
}

impl AuthConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open a users file, creating an empty one if it does not exist, and persist every later
    /// change back to it. This is how an operator provisions users outside of code.
    pub fn open(path: &std::path::Path) -> Result<Self> {
        let users = if path.exists() {
            Self::read(path)?
        } else {
            BTreeMap::new()
        };
        Ok(Self {
            users: std::sync::RwLock::new(users),
            path: Some(path.to_path_buf()),
        })
    }

    /// Register `name` with `secret` and `role`, in memory. The builder form, for tests and
    /// code-defined users; does not touch any file.
    pub fn user(self, name: &str, secret: &str, role: Role) -> Self {
        self.users.write().expect("users lock").insert(
            name.to_string(),
            User {
                key: derive_key(secret),
                role,
            },
        );
        self
    }

    /// Create a user from an already-derived key, persisting the change.
    ///
    /// Takes the key, not the secret: the key is what the wire carries and what the file
    /// stores, so the plaintext secret never reaches the server at all.
    pub fn create(&self, name: &str, key: Key, role: Role) -> Result<()> {
        if name.is_empty() || name.contains(char::is_whitespace) {
            return Err(Error::Protocol(
                "a user name must be non-empty and contain no whitespace".into(),
            ));
        }
        self.users
            .write()
            .expect("users lock")
            .insert(name.to_string(), User { key, role });
        self.save()
    }

    /// Remove a user, persisting the change. Returns whether the user existed.
    pub fn drop_user(&self, name: &str) -> Result<bool> {
        let existed = self
            .users
            .write()
            .expect("users lock")
            .remove(name)
            .is_some();
        if existed {
            self.save()?;
        }
        Ok(existed)
    }

    /// Every user's name and role, sorted. Never returns keys — those do not leave the store.
    pub fn list(&self) -> Vec<(String, Role)> {
        self.users
            .read()
            .expect("users lock")
            .iter()
            .map(|(n, u)| (n.clone(), u.role))
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.users.read().expect("users lock").is_empty()
    }

    /// Check a proof against a challenge. `None` means refused — and deliberately does not
    /// say whether the name or the proof was wrong, so the handshake cannot be used to
    /// enumerate valid names.
    pub fn verify(&self, name: &str, nonce: &[u8; 32], proof: &[u8; 32]) -> Option<Role> {
        let users = self.users.read().expect("users lock");
        let user = users.get(name)?;
        // Compared as `blake3::Hash`es, whose equality is constant-time — a byte-slice
        // compare would leak how many leading bytes matched through timing.
        let expected = blake3::keyed_hash(&user.key, nonce);
        if expected == blake3::Hash::from_bytes(*proof) {
            Some(user.role)
        } else {
            None
        }
    }

    /// Write the store durably: temp file, fsync, rename. A user database half-written by a
    /// crash would lock people out or, worse, admit them; the same care the term store takes.
    fn save(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let users = self.users.read().expect("users lock");
        let mut body = format!(
            "{USERS_HEADER}
"
        );
        for (name, u) in users.iter() {
            // `user <name> <role> <hex-key>` — the key, never the secret.
            body.push_str(&format!(
                "user {name} {} {}
",
                u.role,
                u.key.iter().map(|b| format!("{b:02x}")).collect::<String>()
            ));
        }

        let tmp = path.with_extension("tmp");
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp)
                .map_err(|e| Error::Manifest(format!("creating {}: {e}", tmp.display())))?;
            f.write_all(body.as_bytes())
                .map_err(|e| Error::Manifest(format!("writing {}: {e}", tmp.display())))?;
            f.sync_all()
                .map_err(|e| Error::Manifest(format!("fsyncing {}: {e}", tmp.display())))?;
        }
        std::fs::rename(&tmp, path)
            .map_err(|e| Error::Manifest(format!("renaming into {}: {e}", path.display())))?;
        if let Some(dir) = path.parent()
            && let Ok(d) = std::fs::File::open(dir)
        {
            let _ = d.sync_all();
        }
        Ok(())
    }

    fn read(path: &std::path::Path) -> Result<BTreeMap<String, User>> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::Manifest(format!("reading {}: {e}", path.display())))?;
        let mut lines = text.lines();
        if lines.next() != Some(USERS_HEADER) {
            return Err(Error::Manifest(format!(
                "{} is not a shardlite users file (missing the `{USERS_HEADER}` header)",
                path.display()
            )));
        }
        let mut users = BTreeMap::new();
        for (n, line) in lines.enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            // `user <name> <role> <hex-key>`. A malformed line is refused, not skipped: a
            // silently dropped user is a lockout that looks like a typo.
            if parts.len() != 4 || parts[0] != "user" {
                return Err(Error::Manifest(format!(
                    "{}: line {} is malformed",
                    path.display(),
                    n + 2
                )));
            }
            let role: Role = parts[2].parse()?;
            let key = parse_key(parts[3]).ok_or_else(|| {
                Error::Manifest(format!("{}: line {} has a bad key", path.display(), n + 2))
            })?;
            users.insert(parts[1].to_string(), User { key, role });
        }
        Ok(users)
    }
}

/// Parse a 64-char hex string into a 32-byte key.
fn parse_key(hex: &str) -> Option<Key> {
    if hex.len() != 64 {
        return None;
    }
    let mut key = [0u8; 32];
    for (i, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_correct_proof_authenticates_with_its_role() {
        let auth = AuthConfig::new().user("app", "s3cret", Role::Write);
        let n = nonce().unwrap();
        let proof = prove(&derive_key("s3cret"), &n);
        assert_eq!(auth.verify("app", &n, &proof), Some(Role::Write));
    }

    #[test]
    fn a_wrong_secret_or_unknown_name_is_refused_identically() {
        // The refusal must not distinguish the two, or the handshake enumerates names.
        let auth = AuthConfig::new().user("app", "s3cret", Role::Write);
        let n = nonce().unwrap();
        let bad = prove(&derive_key("wrong"), &n);
        assert_eq!(auth.verify("app", &n, &bad), None);
        assert_eq!(auth.verify("nobody", &n, &bad), None);
    }

    #[test]
    fn a_proof_is_bound_to_its_nonce() {
        // A captured handshake replayed against a new connection must fail — the nonce is
        // fresh per connection, so the old proof proves nothing.
        let auth = AuthConfig::new().user("app", "s3cret", Role::Write);
        let n1 = nonce().unwrap();
        let n2 = nonce().unwrap();
        assert_ne!(n1, n2, "nonces must be fresh");
        let proof_for_n1 = prove(&derive_key("s3cret"), &n1);
        assert_eq!(auth.verify("app", &n2, &proof_for_n1), None);
    }

    #[test]
    fn the_role_ladder_is_ordered_and_cluster_is_off_it() {
        use Requirement as N;
        assert!(Role::Read.permits(N::Read) && !Role::Read.permits(N::Write));
        assert!(Role::Write.permits(N::Read) && Role::Write.permits(N::Write));
        assert!(!Role::Write.permits(N::Admin));
        assert!(Role::Admin.permits(N::Read) && Role::Admin.permits(N::Admin));

        // The line that matters: an administrator cannot subscribe to the replication
        // stream, and a peer node cannot run ad-hoc queries. Stolen credentials on either
        // side stay on their side of the line.
        assert!(!Role::Admin.permits(N::Cluster));
        assert!(Role::Cluster.permits(N::Cluster));
        assert!(!Role::Cluster.permits(N::Read));
    }
}
