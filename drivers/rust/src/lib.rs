//! HTTP driver for the meshdb gateway. Streaming reads, thin wrapper over `/v1`.
//!
//! ```no_run
//! use meshdb_driver::Client;
//! let db = Client::with_auth("http://localhost:4680", "app", "s3cret");
//! for row in db.query("SELECT id, v FROM t WHERE id > ?1", 0, &[serde_json::json!(5)])? {
//!     let row = row?;
//!     println!("{} {}", row["id"], row["v"]);
//! }
//! db.execute("INSERT INTO t VALUES (?1, ?2)", 0, &[serde_json::json!(1), serde_json::json!("a")])?;
//! # Ok::<(), meshdb_driver::Error>(())
//! ```
//!
//! [`Client::query`] returns an iterator that reads rows from the socket one at a time, so a
//! million-row result costs the driver almost nothing. Auth is sent as
//! `Authorization: Bearer base64(user:secret)` — the programmatic scheme, no browser prompt.
//! Over a plaintext gateway the credential is exposed; use TLS on any untrusted network.

use std::io::BufRead;

use serde_json::Value;

#[derive(Debug)]
pub enum Error {
    /// A non-2xx response. Carries the status and the gateway's message.
    Http { status: u16, message: String },
    /// A transport or decoding failure.
    Transport(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Http { status, message } => write!(f, "HTTP {status}: {message}"),
            Error::Transport(m) => write!(f, "transport: {m}"),
        }
    }
}
impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

pub struct Client {
    base: String,
    auth: Option<String>,
    agent: ureq::Agent,
}

impl Client {
    pub fn new(base: &str) -> Self {
        Self {
            base: base.trim_end_matches('/').to_string(),
            auth: None,
            agent: ureq::Agent::new(),
        }
    }

    /// Authenticate with Bearer base64(user:secret) on every request.
    pub fn with_auth(base: &str, user: &str, secret: &str) -> Self {
        let mut c = Self::new(base);
        c.auth = Some(format!("Bearer {}", b64(format!("{user}:{secret}").as_bytes())));
        c
    }

    fn request(&self, method: &str, path: &str) -> ureq::Request {
        let req = self.agent.request(method, &format!("{}{path}", self.base));
        match &self.auth {
            Some(a) => req.set("Authorization", a),
            None => req,
        }
    }

    fn send(&self, method: &str, path: &str, body: Option<Value>) -> Result<ureq::Response> {
        let req = self.request(method, path);
        let result = match body {
            Some(b) => req
                .set("Content-Type", "application/json")
                .send_string(&b.to_string()),
            None => req.call(),
        };
        result.map_err(|e| match e {
            ureq::Error::Status(status, resp) => {
                let msg = resp
                    .into_string()
                    .ok()
                    .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                    .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(String::from))
                    .unwrap_or_else(|| "error".into());
                Error::Http { status, message: msg }
            }
            ureq::Error::Transport(t) => Error::Transport(t.to_string()),
        })
    }

    fn json(&self, method: &str, path: &str, body: Option<Value>) -> Result<Value> {
        let text = self
            .send(method, path, body)?
            .into_string()
            .map_err(|e| Error::Transport(e.to_string()))?;
        serde_json::from_str(&text).map_err(|e| Error::Transport(e.to_string()))
    }

    // -- reads --

    /// Stream a read. The returned iterator yields one row (a JSON object) at a time.
    pub fn query(&self, sql: &str, shard: u32, params: &[Value]) -> Result<Rows> {
        self.query_with(sql, shard, params, "linearizable")
    }

    pub fn query_with(
        &self,
        sql: &str,
        shard: u32,
        params: &[Value],
        consistency: &str,
    ) -> Result<Rows> {
        let body = serde_json::json!({
            "shard": shard, "sql": sql, "params": params, "consistency": consistency,
        });
        let resp = self.send("POST", "/v1/query", Some(body))?;
        Ok(Rows {
            lines: std::io::BufReader::new(resp.into_reader()).lines(),
            columns: None,
        })
    }

    pub fn query_all(&self, sql: &str) -> Result<Value> {
        self.json("POST", "/v1/query_all", Some(serde_json::json!({ "sql": sql })))
    }

    pub fn route(&self, key: &str) -> Result<u32> {
        let v = self.json("POST", "/v1/route", Some(serde_json::json!({ "key": key })))?;
        v.get("shard")
            .and_then(|s| s.as_u64())
            .map(|s| s as u32)
            .ok_or_else(|| Error::Transport("no shard in response".into()))
    }

    // -- writes --

    pub fn execute(&self, sql: &str, shard: u32, params: &[Value]) -> Result<Value> {
        self.json(
            "POST",
            "/v1/execute",
            Some(serde_json::json!({ "shard": shard, "sql": sql, "params": params })),
        )
    }

    /// Apply statements atomically and durably. Each statement is `{"sql":..,"params":[..]}`.
    pub fn tx(&self, statements: Vec<Value>, shard: u32) -> Result<Value> {
        self.json(
            "POST",
            "/v1/tx",
            Some(serde_json::json!({ "shard": shard, "statements": statements })),
        )
    }

    pub fn execute_all(&self, sql: &str) -> Result<Value> {
        self.json("POST", "/v1/execute_all", Some(serde_json::json!({ "sql": sql })))
    }

    // -- introspection & admin --

    pub fn info(&self) -> Result<Value> {
        self.json("GET", "/v1/info", None)
    }
    pub fn cluster(&self) -> Result<Value> {
        self.json("GET", "/v1/cluster", None)
    }
    pub fn stats(&self) -> Result<Value> {
        self.json("GET", "/v1/stats", None)
    }
    pub fn schema(&self, shard: u32) -> Result<Value> {
        self.json("GET", &format!("/v1/schema/{shard}"), None)
    }
    pub fn frames(&self, shard: u32) -> Result<Value> {
        self.json("GET", &format!("/v1/frames/{shard}"), None)
    }
    pub fn list_users(&self) -> Result<Value> {
        self.json("GET", "/v1/users", None)
    }
    pub fn create_user(&self, name: &str, secret: &str, role: &str) -> Result<()> {
        self.send(
            "POST",
            "/v1/users",
            Some(serde_json::json!({ "name": name, "secret": secret, "role": role })),
        )
        .map(|_| ())
    }
    pub fn drop_user(&self, name: &str) -> Result<()> {
        self.send("DELETE", &format!("/v1/users/{name}"), None).map(|_| ())
    }
}

/// A streaming query result: an iterator over rows, each a JSON object keyed by column name.
pub struct Rows {
    lines: std::io::Lines<std::io::BufReader<Box<dyn std::io::Read + Send + Sync + 'static>>>,
    columns: Option<Vec<String>>,
}

impl Iterator for Rows {
    type Item = Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let line = match self.lines.next()? {
                Ok(l) => l,
                Err(e) => return Some(Err(Error::Transport(e.to_string()))),
            };
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let obj: Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(e) => return Some(Err(Error::Transport(e.to_string()))),
            };
            if let Some(cols) = obj.get("columns").and_then(|c| c.as_array()) {
                self.columns =
                    Some(cols.iter().filter_map(|c| c.as_str().map(String::from)).collect());
                continue;
            }
            if let Some(err) = obj.get("error").and_then(|e| e.as_str()) {
                return Some(Err(Error::Http {
                    status: 200,
                    message: err.to_string(),
                }));
            }
            // A row array → object keyed by column.
            let cells = obj.as_array().cloned().unwrap_or_default();
            let cols = self.columns.clone().unwrap_or_default();
            let row: serde_json::Map<String, Value> = cols
                .into_iter()
                .zip(cells)
                .collect();
            return Some(Ok(Value::Object(row)));
        }
    }
}

fn b64(input: &[u8]) -> String {
    const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in input.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(A[((n >> 18) & 63) as usize] as char);
        out.push(A[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 { A[((n >> 6) & 63) as usize] as char } else { '=' });
        out.push(if chunk.len() > 2 { A[(n & 63) as usize] as char } else { '=' });
    }
    out
}

/// The native bincode-over-TCP client — the fastest transport, and the one a Rust program
/// that is itself a cluster member should use.
///
/// Enabled with `--features native`, which pulls in the meshdb crate. This is a re-export of
/// `meshdb::net::Client`; see its docs for the full surface (`connect`, `connect_as`,
/// `query`, `execute`, `begin`/transactions, and so on). The HTTP [`Client`] above and this
/// native client are deliberately separate types: HTTP is the stable cross-language edge,
/// native is the Rust-only fast path.
#[cfg(feature = "native")]
pub mod native {
    pub use meshdb::net::Client;
    pub use meshdb::storage::Value;
    pub use meshdb::storage::exec::Statement;
}
