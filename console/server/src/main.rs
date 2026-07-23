//! meshdb console — a standalone web app for managing and observing meshdb clusters.
//!
//! It is deliberately separate from the database: its own binary, its own login, its own state.
//! The browser talks only to this backend (same origin, so no CORS), and this backend talks to
//! clusters over their stable HTTP `/v1` edge. See `docs/console-plan.md`.

mod ai;
mod api;
mod assets;
mod assistant;
mod audit;
mod auth;
mod crypto;
mod database;
mod metrics;
mod operations;
mod proxy;
mod registry;
mod respond;
mod store;
mod users;

use std::sync::Arc;

use api::{AppState, StreamSlots};
use audit::Audit;
use auth::{LoginLimiter, Sessions};
use crypto::Sealer;
use metrics::Metrics;
use operations::Operations;
use registry::Registry;
use users::Users;

fn main() {
    if let Err(e) = run() {
        eprintln!("meshdb-console: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().collect();
    let listen = flag(&args, "--listen").unwrap_or_else(|| "127.0.0.1:7100".into());
    let data = flag(&args, "--data").unwrap_or_else(|| "console-data".into());
    let workers: usize = flag(&args, "--workers")
        .and_then(|s| s.parse().ok())
        .unwrap_or(8)
        .max(2);
    let query_streams: usize = flag(&args, "--query-streams")
        .and_then(|s| s.parse().ok())
        .unwrap_or(workers / 2)
        .clamp(1, workers - 1);

    // The master passphrase is required, and required at startup — a console that could not
    // decrypt its stored secrets must fail loudly now, not when someone first opens a connection.
    let key = std::env::var("MESHDB_CONSOLE_KEY").map_err(|_| {
        "MESHDB_CONSOLE_KEY is required — the master passphrase that encrypts stored connection \
         secrets at rest. Set it in the environment and restart."
            .to_string()
    })?;
    if key.is_empty() {
        return Err("MESHDB_CONSOLE_KEY must not be empty".into());
    }

    let data_dir = std::path::PathBuf::from(&data);
    secure_data_dir(&data_dir)?;
    let users_path = data_dir.join("users.json");
    let conns_path = data_dir.join("connections.json");
    let audit_path = data_dir.join("audit.jsonl");
    let operations_path = data_dir.join("operations.json");

    // Bootstrap a first admin from the environment when the store has none — otherwise there
    // would be no way to log in and create one.
    let bootstrap = match (
        std::env::var("MESHDB_CONSOLE_ADMIN"),
        std::env::var("MESHDB_CONSOLE_ADMIN_PASSWORD"),
    ) {
        (Ok(u), Ok(p)) if !u.is_empty() && !p.is_empty() => Some((u, p)),
        _ => None,
    };
    let bootstrap_ref = bootstrap.as_ref().map(|(u, p)| (u.as_str(), p.as_str()));

    let users = Users::open(&users_path, bootstrap_ref).map_err(|e| e.to_string())?;
    let mut registry =
        Registry::open(&conns_path, Sealer::from_passphrase(&key)).map_err(|e| e.to_string())?;
    if args.iter().any(|arg| arg == "--rotate-key") {
        let new_key = std::env::var("MESHDB_CONSOLE_NEW_KEY")
            .map_err(|_| "MESHDB_CONSOLE_NEW_KEY is required with --rotate-key".to_string())?;
        if new_key.is_empty() || new_key == key {
            return Err("MESHDB_CONSOLE_NEW_KEY must be non-empty and different".into());
        }
        registry
            .rotate_key(Sealer::from_passphrase(&new_key))
            .map_err(|e| format!("rotating connection secrets: {e}"))?;
        eprintln!("meshdb-console rotated all saved connection secrets; restart with the new key");
        return Ok(());
    }
    let registry = Arc::new(registry);
    let metrics = Arc::new(Metrics::open(&data_dir.join("observability.json"))?);
    let sessions = Sessions::with_ttl(
        std::time::Duration::from_secs(env_u64("MESHDB_CONSOLE_SESSION_IDLE_SECS", 30 * 60)),
        std::time::Duration::from_secs(env_u64(
            "MESHDB_CONSOLE_SESSION_ABSOLUTE_SECS",
            12 * 60 * 60,
        )),
    );
    let audit = Audit::open(&audit_path)?;
    let operations = Operations::open(&operations_path)?;
    let ai = Arc::new(ai::AiConfig::open(
        &data_dir.join("ai.json"),
        Sealer::from_passphrase(&key),
    )?);
    let secure_cookie = env_bool("MESHDB_CONSOLE_SECURE_COOKIE", false);

    metrics::spawn(Arc::clone(&registry), Arc::clone(&metrics));
    operations.spawn(Arc::clone(&registry), audit.clone());

    let state = Arc::new(AppState {
        users,
        registry,
        sessions,
        metrics,
        operations,
        audit,
        ai,
        login_limiter: LoginLimiter::new(),
        secure_cookie,
        streams: StreamSlots::new(query_streams),
    });

    let server = Arc::new(
        tiny_http::Server::http(listen.as_str()).map_err(|e| format!("binding {listen}: {e}"))?,
    );
    eprintln!(
        "meshdb-console listening on http://{listen}  (data: {})",
        data_dir.display()
    );
    if bootstrap.is_some() {
        eprintln!("bootstrapped an admin from MESHDB_CONSOLE_ADMIN");
    }

    // Thread-per-connection over a fixed worker pool, matching the meshdb gateway. Each worker
    // blocks in recv(); a streaming proxy response holds one worker for the life of the stream,
    // which is why the pool exists rather than a single accept loop.
    let mut handles = Vec::new();
    for _ in 0..workers.max(1) {
        let server = Arc::clone(&server);
        let state = Arc::clone(&state);
        handles.push(std::thread::spawn(move || loop {
            match server.recv() {
                Ok(request) => {
                    let _ = api::handle(request, &state);
                }
                Err(_) => break,
            }
        }));
    }
    for h in handles {
        let _ = h.join();
    }
    Ok(())
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn secure_data_dir(path: &std::path::Path) -> Result<(), String> {
    std::fs::create_dir_all(path).map_err(|e| format!("creating {}: {e}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|e| format!("securing {}: {e}", path.display()))?;
    }
    Ok(())
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
}
