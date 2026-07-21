//! Console sessions. A successful login mints a random opaque token held in memory and handed
//! back as an `HttpOnly` cookie; every later request is identified by looking the token up.
//!
//! Sessions are in-memory by design: a console restart logs everyone out, which for an operator
//! tool is a feature, not a loss (no session survives a redeploy, nothing to invalidate on disk).
//! The token is 256 bits from the OS CSPRNG, so it cannot be guessed and carries no data an
//! attacker could forge — all authority lives server-side in this map.

use std::collections::HashMap;
use std::sync::RwLock;

use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use base64::Engine;
use rand::rngs::OsRng;
use rand::RngCore;

use crate::users::Role;

pub const COOKIE_NAME: &str = "mc_session";

#[derive(Clone)]
pub struct Session {
    pub user: String,
    pub role: Role,
}

#[derive(Default)]
pub struct Sessions {
    map: RwLock<HashMap<String, Session>>,
}

impl Sessions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mint a session token for a logged-in user.
    pub fn create(&self, user: String, role: Role) -> String {
        let mut raw = [0u8; 32];
        OsRng.fill_bytes(&mut raw);
        let token = B64URL.encode(raw);
        self.map
            .write()
            .unwrap()
            .insert(token.clone(), Session { user, role });
        token
    }

    pub fn lookup(&self, token: &str) -> Option<Session> {
        self.map.read().unwrap().get(token).cloned()
    }

    pub fn remove(&self, token: &str) {
        self.map.write().unwrap().remove(token);
    }
}

/// The `Set-Cookie` value for a fresh session. `HttpOnly` keeps it out of JavaScript (so an XSS
/// cannot read it); `SameSite=Strict` blocks it from being sent on cross-site requests (CSRF
/// defence); `Path=/` scopes it to the whole console. `Secure` is *not* set here because the
/// console may run over plaintext on localhost during development — put it behind TLS in
/// production, where a fronting proxy adds `Secure`.
pub fn set_cookie(token: &str) -> String {
    format!("{COOKIE_NAME}={token}; HttpOnly; SameSite=Strict; Path=/")
}

/// The `Set-Cookie` value that clears the session cookie on logout.
pub fn clear_cookie() -> String {
    format!("{COOKIE_NAME}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0")
}

/// Pull the session token out of a `Cookie:` header value, if present.
pub fn token_from_cookie_header(header: &str) -> Option<String> {
    for part in header.split(';') {
        let part = part.trim();
        if let Some(value) = part.strip_prefix(&format!("{COOKIE_NAME}=")) {
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_session_is_found_by_its_token_and_gone_after_removal() {
        let s = Sessions::new();
        let token = s.create("admin".into(), Role::Admin);
        assert_eq!(s.lookup(&token).unwrap().user, "admin");
        s.remove(&token);
        assert!(s.lookup(&token).is_none());
    }

    #[test]
    fn tokens_are_unique() {
        let s = Sessions::new();
        let a = s.create("x".into(), Role::User);
        let b = s.create("x".into(), Role::User);
        assert_ne!(a, b);
    }

    #[test]
    fn the_token_is_parsed_out_of_a_realistic_cookie_header() {
        let h = format!("other=1; {COOKIE_NAME}=abc123; last=2");
        assert_eq!(token_from_cookie_header(&h), Some("abc123".to_string()));
        assert_eq!(token_from_cookie_header("nothing=here"), None);
        assert_eq!(token_from_cookie_header(&format!("{COOKIE_NAME}=")), None);
    }
}
