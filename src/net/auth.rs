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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
        Request::Hello { .. } | Request::Auth { .. } => Requirement::Handshake,

        Request::Query { .. }
        | Request::QueryAll { .. }
        | Request::Route { .. }
        | Request::Info => Requirement::Read,

        Request::Execute { .. } => Requirement::Write,

        // Cluster-wide DDL reshapes every shard; that is an operator action, not a client one.
        Request::ExecuteAll { .. } | Request::SchemaApply { .. } => Requirement::Admin,

        // Subscription and snapshots hand out entire shards; votes and heartbeats steer the
        // cluster; Direct is how peers forward work they have already authorized.
        Request::Subscribe { .. }
        | Request::SnapshotBegin { .. }
        | Request::SnapshotRead { .. }
        | Request::SnapshotEnd { .. }
        | Request::Vote(_)
        | Request::Beat(_)
        | Request::Direct(_) => Requirement::Cluster,
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

#[derive(Debug, Clone)]
struct User {
    key: Key,
    role: Role,
}

/// The users a server accepts. Empty means authentication is not configured and the server
/// is open — which it announces loudly at startup rather than quietly being.
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    users: BTreeMap<String, User>,
}

impl AuthConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `name` with `secret` and `role`. The secret is hashed immediately and never
    /// stored.
    pub fn user(mut self, name: &str, secret: &str, role: Role) -> Self {
        self.users.insert(
            name.to_string(),
            User {
                key: derive_key(secret),
                role,
            },
        );
        self
    }

    pub fn is_empty(&self) -> bool {
        self.users.is_empty()
    }

    /// Check a proof against a challenge. `None` means refused — and deliberately does not
    /// say whether the name or the proof was wrong, so the handshake cannot be used to
    /// enumerate valid names.
    pub fn verify(&self, name: &str, nonce: &[u8; 32], proof: &[u8; 32]) -> Option<Role> {
        let user = self.users.get(name)?;
        // Compared as `blake3::Hash`es, whose equality is constant-time — a byte-slice
        // compare would leak how many leading bytes matched through timing.
        let expected = blake3::keyed_hash(&user.key, nonce);
        if expected == blake3::Hash::from_bytes(*proof) {
            Some(user.role)
        } else {
            None
        }
    }
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
