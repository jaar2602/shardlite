//! The console's own user store — its identity layer, entirely separate from the meshdb
//! credentials it stores per connection (this is scoping decision 1: multi-user from the start).
//!
//! Four roles. Console policy is checked before any request reaches a stored meshdb credential;
//! meshdb then applies its own role as a second, independent boundary. Old persisted `user`
//! records deserialize as `Developer` for compatibility with the v1 console.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::crypto::{hash_password, verify_password};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Viewer,
    #[serde(alias = "user")]
    Developer,
    Operator,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    Observe,
    Query,
    Write,
    Operate,
    ManageConnections,
    ManageConsoleUsers,
    ManageMeshUsers,
    ReadAudit,
}

impl Role {
    pub fn permits(self, permission: Permission) -> bool {
        use Permission as P;
        use Role as R;
        matches!(
            (self, permission),
            (R::Admin, _)
                | (R::Viewer, P::Observe | P::Query)
                | (R::Developer, P::Observe | P::Query | P::Write)
                | (R::Operator, P::Observe | P::Query | P::Operate)
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    name: String,
    role: Role,
    /// Argon2 PHC string. Never the plaintext.
    pw_hash: String,
}

/// A persisted set of console users. Every mutation is written back to `path` before it is
/// acknowledged, so a crash cannot lose a just-created account or leave a half-written file.
pub struct Users {
    path: PathBuf,
    map: RwLock<HashMap<String, Record>>,
}

#[derive(Debug)]
pub enum UserError {
    Exists,
    NotFound,
    /// Refusing to delete the last admin would otherwise lock everyone out of administration.
    LastAdmin,
    Io(String),
}

impl std::fmt::Display for UserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UserError::Exists => write!(f, "a console user with that name already exists"),
            UserError::NotFound => write!(f, "no such console user"),
            UserError::LastAdmin => write!(f, "cannot remove the last admin"),
            UserError::Io(e) => write!(f, "{e}"),
        }
    }
}

impl Users {
    /// Load the store from `path` (creating an empty one if absent). If the store has no admin,
    /// bootstrap one from `bootstrap` — how the very first login exists before anyone can create
    /// users. Passing `None` with an empty store is allowed but leaves no way to log in.
    pub fn open(path: &Path, bootstrap: Option<(&str, &str)>) -> Result<Self, UserError> {
        let map = if path.exists() {
            let bytes = std::fs::read(path).map_err(|e| UserError::Io(e.to_string()))?;
            let records: Vec<Record> =
                serde_json::from_slice(&bytes).map_err(|e| UserError::Io(e.to_string()))?;
            records.into_iter().map(|r| (r.name.clone(), r)).collect()
        } else {
            HashMap::new()
        };
        let users = Users {
            path: path.to_path_buf(),
            map: RwLock::new(map),
        };
        let has_admin = users
            .map
            .read()
            .unwrap()
            .values()
            .any(|r| r.role == Role::Admin);
        if !has_admin {
            if let Some((name, password)) = bootstrap {
                users.insert(name, password, Role::Admin)?;
            }
        }
        Ok(users)
    }

    fn persist(&self, map: &HashMap<String, Record>) -> Result<(), UserError> {
        let mut records: Vec<&Record> = map.values().collect();
        records.sort_by(|a, b| a.name.cmp(&b.name));
        let json = serde_json::to_vec_pretty(&records).map_err(|e| UserError::Io(e.to_string()))?;
        crate::store::write_atomic(&self.path, &json).map_err(UserError::Io)
    }

    fn insert(&self, name: &str, password: &str, role: Role) -> Result<(), UserError> {
        let mut map = self.map.write().unwrap();
        if map.contains_key(name) {
            return Err(UserError::Exists);
        }
        map.insert(
            name.to_string(),
            Record {
                name: name.to_string(),
                role,
                pw_hash: hash_password(password),
            },
        );
        self.persist(&map)
    }

    /// Create a console user. Admin-gated at the API layer.
    pub fn create(&self, name: &str, password: &str, role: Role) -> Result<(), UserError> {
        self.insert(name, password, role)
    }

    /// Remove a console user, refusing to remove the last remaining admin.
    pub fn delete(&self, name: &str) -> Result<(), UserError> {
        let mut map = self.map.write().unwrap();
        let record = map.get(name).ok_or(UserError::NotFound)?;
        if record.role == Role::Admin {
            let admins = map.values().filter(|r| r.role == Role::Admin).count();
            if admins <= 1 {
                return Err(UserError::LastAdmin);
            }
        }
        map.remove(name);
        self.persist(&map)
    }

    /// Verify a login, returning the user's role on success.
    pub fn verify(&self, name: &str, password: &str) -> Option<Role> {
        let map = self.map.read().unwrap();
        let record = map.get(name)?;
        if verify_password(password, &record.pw_hash) {
            Some(record.role)
        } else {
            None
        }
    }

    /// Names and roles, never hashes. Sorted for a stable UI.
    pub fn list(&self) -> Vec<(String, Role)> {
        let map = self.map.read().unwrap();
        let mut out: Vec<_> = map.values().map(|r| (r.name.clone(), r.role)).collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> PathBuf {
        let mut p = std::env::temp_dir();
        let n: u64 = rand::random();
        p.push(format!("meshdb-console-users-{n}.json"));
        p
    }

    #[test]
    fn bootstrap_creates_an_admin_who_can_log_in() {
        let path = tmp();
        let users = Users::open(&path, Some(("admin", "pw"))).unwrap();
        assert_eq!(users.verify("admin", "pw"), Some(Role::Admin));
        assert_eq!(users.verify("admin", "wrong"), None);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn users_persist_across_reopen() {
        let path = tmp();
        {
            let users = Users::open(&path, Some(("admin", "pw"))).unwrap();
            users.create("bob", "s3cret", Role::Developer).unwrap();
        }
        let reopened = Users::open(&path, None).unwrap();
        assert_eq!(reopened.verify("bob", "s3cret"), Some(Role::Developer));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn the_last_admin_cannot_be_removed() {
        let path = tmp();
        let users = Users::open(&path, Some(("admin", "pw"))).unwrap();
        assert!(matches!(users.delete("admin"), Err(UserError::LastAdmin)));
        users.create("admin2", "pw", Role::Admin).unwrap();
        assert!(users.delete("admin").is_ok()); // now safe, another admin remains
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn roles_enforce_the_console_permission_ladder() {
        assert!(Role::Viewer.permits(Permission::Observe));
        assert!(Role::Viewer.permits(Permission::Query));
        assert!(!Role::Viewer.permits(Permission::Write));
        assert!(Role::Developer.permits(Permission::Write));
        assert!(!Role::Developer.permits(Permission::Operate));
        assert!(Role::Operator.permits(Permission::Operate));
        assert!(!Role::Operator.permits(Permission::ManageConnections));
        assert!(Role::Admin.permits(Permission::ManageMeshUsers));
    }

    #[test]
    fn the_legacy_user_role_loads_as_developer() {
        let role: Role = serde_json::from_str("\"user\"").unwrap();
        assert_eq!(role, Role::Developer);
    }
}
