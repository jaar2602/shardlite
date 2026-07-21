//! Forwarding to a cluster's meshdb HTTP `/v1` edge (scoping decision 2: the console talks JSON,
//! never bincode, so it survives cluster version skew).
//!
//! The forward is **uniform and streaming**: whatever the browser asked of a connection, the
//! same method and body go to `/v1`, and the upstream response — status, content type, and body
//! — is passed straight back. The body is streamed, never buffered, so a `SELECT` returning a
//! million NDJSON rows costs the console almost nothing: the "1 row to 1 million rows"
//! robustness the gateway has carries end to end through the console.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde_json::Value;
use tiny_http::{Header, Request, Response, StatusCode};

use crate::registry::Resolved;
use crate::respond;

fn bearer(resolved: &Resolved) -> Option<String> {
    match (&resolved.meshdb_user, &resolved.meshdb_secret) {
        (Some(u), Some(s)) => Some(format!("Bearer {}", B64.encode(format!("{u}:{s}")))),
        _ => None,
    }
}

/// Forward one request to `{url}/v1/{suffix}` and stream the reply back to `request`.
pub fn forward(
    request: Request,
    resolved: &Resolved,
    method: &str,
    suffix: &str,
    body: Vec<u8>,
) -> std::io::Result<()> {
    let url = format!("{}/v1/{}", resolved.url, suffix);
    let agent = ureq::agent();
    let mut req = agent.request(method, &url);
    if let Some(auth) = bearer(resolved) {
        req = req.set("Authorization", &auth);
    }

    // GET carries no body; everything else forwards the browser's JSON body verbatim.
    let result = if method.eq_ignore_ascii_case("GET") {
        req.call()
    } else {
        req.set("Content-Type", "application/json").send_bytes(&body)
    };

    let upstream = match result {
        Ok(r) => r,
        // meshdb answered with a non-2xx (a rejected statement, a 400, a 401): that is a real
        // result and its body/status must reach the browser unchanged, not be masked as a proxy
        // error.
        Err(ureq::Error::Status(_, r)) => r,
        // The cluster could not be reached at all — distinct from an error it returned.
        Err(ureq::Error::Transport(t)) => {
            return respond::error(request, 502, &format!("cannot reach cluster: {t}"));
        }
    };

    let status = upstream.status();
    let ctype = upstream
        .header("Content-Type")
        .unwrap_or("application/json")
        .to_string();
    // into_reader() streams off the socket; the body is not read into memory here.
    let reader = upstream.into_reader();
    let header = Header::from_bytes(&b"Content-Type"[..], ctype.as_bytes()).expect("ctype header");
    let response = Response::new(StatusCode(status), vec![header], reader, None, None);
    request.respond(response)
}

/// A buffered GET used by the metrics sampler (not the request path). Returns the parsed JSON on
/// a 2xx, or `None` on any failure — the sampler must never crash the console over one bad poll.
pub fn fetch_json(resolved: &Resolved, suffix: &str) -> Option<Value> {
    let url = format!("{}/v1/{}", resolved.url, suffix);
    let agent = ureq::agent();
    let mut req = agent.request("GET", &url);
    if let Some(auth) = bearer(resolved) {
        req = req.set("Authorization", &auth);
    }
    req.call().ok()?.into_json().ok()
}
