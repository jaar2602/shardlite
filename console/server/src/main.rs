//! meshdb console — a standalone web app for managing and observing meshdb clusters.
//!
//! It is deliberately separate from the database: its own binary, its own login, its own state.
//! The browser talks only to this backend (same origin, so no CORS), and this backend talks to
//! clusters over their stable HTTP `/v1` edge. See `docs/console-plan.md`.

mod api;
mod assets;
mod auth;
mod crypto;
mod metrics;
mod proxy;
mod registry;
mod respond;
mod store;
mod users;

use std::sync::Arc;

use api::AppState;
use auth::Sessions;
use crypto::Sealer;
use metrics::Metrics;
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
        .unwrap_or(8);

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
    let users_path = data_dir.join("users.json");
    let conns_path = data_dir.join("connections.json");

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
    let registry = Arc::new(
        Registry::open(&conns_path, Sealer::from_passphrase(&key)).map_err(|e| e.to_string())?,
    );
    let metrics = Arc::new(Metrics::new());
    let sessions = Sessions::new();

    metrics::spawn(Arc::clone(&registry), Arc::clone(&metrics));

    let state = Arc::new(AppState {
        users,
        registry,
        sessions,
        metrics,
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

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
}
