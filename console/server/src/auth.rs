//! Console sessions. A successful login mints a random opaque token held in memory and handed
//! back as an `HttpOnly` cookie; every later request is identified by looking the token up.
//!
//! Sessions are in-memory by design: a console restart logs everyone out, which for an operator
//! tool is a feature, not a loss (no session survives a redeploy, nothing to invalidate on disk).
//! The token is 256 bits from the OS CSPRNG, so it cannot be guessed and carries no data an
//! attacker could forge — all authority lives server-side in this map.

use std::collections::HashMap;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

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
    pub csrf: String,
}

pub struct Sessions {
    map: RwLock<HashMap<String, Entry>>,
    idle_ttl: Duration,
    absolute_ttl: Duration,
}

struct Entry {
    session: Session,
    created: Instant,
    last_seen: Instant,
}

impl Sessions {
    pub fn new() -> Self {
        Self::with_ttl(
            Duration::from_secs(30 * 60),
            Duration::from_secs(12 * 60 * 60),
        )
    }

    pub fn with_ttl(idle_ttl: Duration, absolute_ttl: Duration) -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            idle_ttl,
            absolute_ttl,
        }
    }

    /// Mint a session token for a logged-in user.
    pub fn create(&self, user: String, role: Role) -> (String, String) {
        let mut raw = [0u8; 32];
        OsRng.fill_bytes(&mut raw);
        let token = B64URL.encode(raw);
        OsRng.fill_bytes(&mut raw);
        let csrf = B64URL.encode(raw);
        let now = Instant::now();
        self.map.write().unwrap().insert(
            token.clone(),
            Entry {
                session: Session {
                    user,
                    role,
                    csrf: csrf.clone(),
                },
                created: now,
                last_seen: now,
            },
        );
        (token, csrf)
    }

    pub fn lookup(&self, token: &str) -> Option<Session> {
        let now = Instant::now();
        let mut map = self.map.write().unwrap();
        let expired = map.get(token).is_some_and(|entry| {
            now.duration_since(entry.last_seen) >= self.idle_ttl
                || now.duration_since(entry.created) >= self.absolute_ttl
        });
        if expired {
            map.remove(token);
            return None;
        }
        let entry = map.get_mut(token)?;
        entry.last_seen = now;
        Some(entry.session.clone())
    }

    pub fn remove(&self, token: &str) {
        self.map.write().unwrap().remove(token);
    }

    pub fn remove_user(&self, user: &str) {
        self.map
            .write()
            .unwrap()
            .retain(|_, entry| entry.session.user != user);
    }
}

impl Default for Sessions {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy)]
struct Attempt {
    failures: u32,
    window_started: Instant,
    blocked_until: Option<Instant>,
}

pub struct LoginLimiter {
    attempts: Mutex<HashMap<String, Attempt>>,
    max_failures: u32,
    window: Duration,
    block_for: Duration,
}

impl LoginLimiter {
    pub fn new() -> Self {
        Self::with_policy(5, Duration::from_secs(5 * 60), Duration::from_secs(5 * 60))
    }

    fn with_policy(max_failures: u32, window: Duration, block_for: Duration) -> Self {
        Self {
            attempts: Mutex::new(HashMap::new()),
            max_failures,
            window,
            block_for,
        }
    }

    /// Seconds until this identity may try again, or `None` when a login attempt is allowed.
    pub fn retry_after(&self, identity: &str) -> Option<u64> {
        let now = Instant::now();
        let mut attempts = self.attempts.lock().unwrap();
        attempts.retain(|_, attempt| {
            now.duration_since(attempt.window_started) < self.window + self.block_for
        });
        let attempt = attempts.get(identity)?;
        let until = attempt.blocked_until?;
        if until > now {
            return Some(until.duration_since(now).as_secs().max(1));
        }
        attempts.remove(identity);
        None
    }

    pub fn failed(&self, identity: &str) {
        let now = Instant::now();
        let mut attempts = self.attempts.lock().unwrap();
        let attempt = attempts.entry(identity.to_string()).or_insert(Attempt {
            failures: 0,
            window_started: now,
            blocked_until: None,
        });
        if now.duration_since(attempt.window_started) >= self.window {
            *attempt = Attempt {
                failures: 0,
                window_started: now,
                blocked_until: None,
            };
        }
        attempt.failures += 1;
        if attempt.failures >= self.max_failures {
            attempt.blocked_until = Some(now + self.block_for);
        }
    }

    pub fn succeeded(&self, identity: &str) {
        self.attempts.lock().unwrap().remove(identity);
    }
}

impl Default for LoginLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// The `Set-Cookie` value for a fresh session. `HttpOnly` keeps it out of JavaScript (so an XSS
/// cannot read it); `SameSite=Strict` blocks it from being sent on cross-site requests (CSRF
/// defence); `Path=/` scopes it to the whole console. `Secure` is configurable because local
/// development may use plaintext; production deployments must enable it when the browser-facing
/// endpoint uses HTTPS.
pub fn set_cookie(token: &str, secure: bool) -> String {
    let secure = if secure { "; Secure" } else { "" };
    format!("{COOKIE_NAME}={token}; HttpOnly; SameSite=Strict; Path=/{secure}")
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
        let (token, _) = s.create("admin".into(), Role::Admin);
        assert_eq!(s.lookup(&token).unwrap().user, "admin");
        s.remove(&token);
        assert!(s.lookup(&token).is_none());
    }

    #[test]
    fn tokens_are_unique() {
        let s = Sessions::new();
        let (a, _) = s.create("x".into(), Role::Viewer);
        let (b, _) = s.create("x".into(), Role::Viewer);
        assert_ne!(a, b);
    }

    #[test]
    fn the_token_is_parsed_out_of_a_realistic_cookie_header() {
        let h = format!("other=1; {COOKIE_NAME}=abc123; last=2");
        assert_eq!(token_from_cookie_header(&h), Some("abc123".to_string()));
        assert_eq!(token_from_cookie_header("nothing=here"), None);
        assert_eq!(token_from_cookie_header(&format!("{COOKIE_NAME}=")), None);
    }

    #[test]
    fn sessions_expire_and_user_revocation_removes_every_token() {
        let sessions = Sessions::with_ttl(Duration::ZERO, Duration::from_secs(1));
        let (expired, _) = sessions.create("old".into(), Role::Viewer);
        assert!(sessions.lookup(&expired).is_none());

        let sessions = Sessions::new();
        let (a, _) = sessions.create("alice".into(), Role::Viewer);
        let (b, _) = sessions.create("alice".into(), Role::Developer);
        sessions.remove_user("alice");
        assert!(sessions.lookup(&a).is_none());
        assert!(sessions.lookup(&b).is_none());
    }

    #[test]
    fn repeated_login_failures_are_throttled_and_success_resets_them() {
        let limiter =
            LoginLimiter::with_policy(2, Duration::from_secs(60), Duration::from_secs(60));
        limiter.failed("alice@host");
        assert_eq!(limiter.retry_after("alice@host"), None);
        limiter.failed("alice@host");
        assert!(limiter.retry_after("alice@host").is_some());
        limiter.succeeded("alice@host");
        assert_eq!(limiter.retry_after("alice@host"), None);
    }
}
