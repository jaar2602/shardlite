//! Forwarding to a cluster's shardlite HTTP `/v1` edge (scoping decision 2: the console talks JSON,
//! never bincode, so it survives cluster version skew).
//!
//! The forward is **uniform and streaming**: whatever the browser asked of a connection, the
//! same method and body go to `/v1`, and the upstream response — status, content type, and body
//! — is passed straight back. The body is streamed, never buffered, so a `SELECT` returning a
//! million NDJSON rows costs the console almost nothing: the "1 row to 1 million rows"
//! robustness the gateway has carries end to end through the console.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use rustls_pki_types::{pem::PemObject, CertificateDer};
use serde_json::Value;
use std::io::{BufRead, BufReader, Read};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tiny_http::{Header, Request, Response, StatusCode};

use crate::registry::Resolved;
use crate::respond;

fn bearer(resolved: &Resolved) -> Option<String> {
    match (&resolved.shardlite_user, &resolved.shardlite_secret) {
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
) -> std::io::Result<u16> {
    forward_response(request, resolved, method, suffix, body, None)
}

pub fn forward_download(
    request: Request,
    resolved: &Resolved,
    suffix: &str,
    body: Vec<u8>,
    filename: &str,
    format: ExportFormat,
    max_rows: Option<usize>,
) -> std::io::Result<u16> {
    forward_response(
        request,
        resolved,
        "POST",
        suffix,
        body,
        Some((filename, format, max_rows)),
    )
}

#[derive(Clone, Copy)]
pub enum ExportFormat {
    Ndjson,
    Csv,
}

fn agent(resolved: &Resolved) -> Result<ureq::Agent, String> {
    let timeout = Duration::from_millis(resolved.timeout_ms);
    let mut builder = ureq::AgentBuilder::new()
        .redirects(0)
        .timeout_connect(timeout.min(Duration::from_secs(10)))
        .timeout_read(timeout)
        .timeout_write(timeout);
    if let Some(pem) = &resolved.custom_ca_pem {
        let mut roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.iter().cloned().collect(),
        };
        let mut count = 0;
        for certificate in CertificateDer::pem_slice_iter(pem.as_bytes()) {
            let certificate = certificate.map_err(|_| "invalid custom CA PEM".to_string())?;
            roots
                .add(certificate)
                .map_err(|e| format!("invalid custom CA certificate: {e}"))?;
            count += 1;
        }
        if count == 0 {
            return Err("custom CA contains no certificates".into());
        }
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        builder = builder.tls_config(Arc::new(tls));
    }
    Ok(builder.build())
}

fn forward_response(
    request: Request,
    resolved: &Resolved,
    method: &str,
    suffix: &str,
    body: Vec<u8>,
    download: Option<(&str, ExportFormat, Option<usize>)>,
) -> std::io::Result<u16> {
    let url = format!("{}/v1/{}", resolved.url, suffix);
    let agent = match agent(resolved) {
        Ok(agent) => agent,
        Err(error) => {
            respond::error(request, 502, &format!("invalid TLS configuration: {error}"))?;
            return Ok(502);
        }
    };
    let mut req = agent.request(method, &url);
    if let Some(auth) = bearer(resolved) {
        req = req.set("Authorization", &auth);
    }

    // GET carries no body; everything else forwards the browser's JSON body verbatim.
    let result = if method.eq_ignore_ascii_case("GET") {
        req.call()
    } else {
        req.set("Content-Type", "application/json")
            .send_bytes(&body)
    };

    let upstream = match result {
        Ok(r) => r,
        // shardlite answered with a non-2xx (a rejected statement, a 400, a 401): that is a real
        // result and its body/status must reach the browser unchanged, not be masked as a proxy
        // error.
        Err(ureq::Error::Status(_, r)) => r,
        // The cluster could not be reached at all — distinct from an error it returned.
        Err(ureq::Error::Transport(t)) => {
            respond::error(request, 502, &format!("cannot reach cluster: {t}"))?;
            return Ok(502);
        }
    };

    let status = upstream.status();
    let ctype = upstream
        .header("Content-Type")
        .unwrap_or("application/json")
        .to_string();
    // into_reader() streams off the socket; the body is not read into memory here.
    let reader = upstream.into_reader();
    let mut headers = vec![
        Header::from_bytes(&b"Content-Type"[..], ctype.as_bytes()).expect("ctype header"),
        Header::from_bytes(&b"Cache-Control"[..], &b"no-store"[..]).expect("cache header"),
        Header::from_bytes(&b"X-Content-Type-Options"[..], &b"nosniff"[..])
            .expect("nosniff header"),
    ];
    if let Some((filename, format, max_rows)) = download {
        headers[0] = Header::from_bytes(
            &b"Content-Type"[..],
            match format {
                ExportFormat::Ndjson => &b"application/x-ndjson"[..],
                ExportFormat::Csv => &b"text/csv; charset=utf-8"[..],
            },
        )
        .expect("export content type");
        let value = format!("attachment; filename=\"{filename}\"");
        headers.push(
            Header::from_bytes(&b"Content-Disposition"[..], value.as_bytes())
                .expect("content disposition header"),
        );
        let response = Response::new(
            StatusCode(status),
            headers,
            ExportReader::new(reader, format, max_rows),
            None,
            None,
        );
        request.respond(response)?;
        return Ok(status);
    }
    let response = Response::new(StatusCode(status), headers, reader, None, None);
    request.respond(response)?;
    Ok(status)
}

/// Incrementally convert the gateway's NDJSON query stream into an export. Reading stops at the
/// optional row limit, which drops the upstream socket and therefore cancels work beyond the cap.
struct ExportReader<R: Read> {
    source: BufReader<R>,
    format: ExportFormat,
    max_rows: Option<usize>,
    rows: usize,
    pending: std::io::Cursor<Vec<u8>>,
    done: bool,
}

impl<R: Read> ExportReader<R> {
    fn new(source: R, format: ExportFormat, max_rows: Option<usize>) -> Self {
        Self {
            source: BufReader::new(source),
            format,
            max_rows,
            rows: 0,
            pending: std::io::Cursor::new(Vec::new()),
            done: false,
        }
    }

    fn next_line(&mut self) -> std::io::Result<Option<Vec<u8>>> {
        if self.done {
            return Ok(None);
        }
        let mut line = String::new();
        if self.source.read_line(&mut line)? == 0 {
            self.done = true;
            return Ok(None);
        }
        let value: Value = serde_json::from_str(line.trim_end()).map_err(|error| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
        })?;
        let is_row = value.is_array();
        if is_row && self.max_rows.is_some_and(|limit| self.rows >= limit) {
            self.done = true;
            return Ok(None);
        }
        if is_row {
            self.rows += 1;
        }
        match self.format {
            ExportFormat::Ndjson => Ok(Some(line.into_bytes())),
            ExportFormat::Csv => {
                let fields = if let Some(columns) = value.get("columns").and_then(Value::as_array) {
                    columns.clone()
                } else if let Some(row) = value.as_array() {
                    row.clone()
                } else if let Some(error) = value.get("error") {
                    vec![Value::String(format!(
                        "shardlite error: {}",
                        csv_scalar(error)
                    ))]
                } else {
                    Vec::new()
                };
                let mut csv = fields
                    .iter()
                    .map(csv_scalar)
                    .map(csv_quote)
                    .collect::<Vec<_>>()
                    .join(",");
                csv.push('\n');
                Ok(Some(csv.into_bytes()))
            }
        }
    }
}

impl<R: Read> Read for ExportReader<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let read = self.pending.read(out)?;
            if read > 0 {
                return Ok(read);
            }
            let Some(line) = self.next_line()? else {
                return Ok(0);
            };
            self.pending = std::io::Cursor::new(line);
        }
    }
}

fn csv_scalar(value: &Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

fn csv_quote(value: String) -> String {
    if value
        .chars()
        .any(|character| matches!(character, ',' | '"' | '\n' | '\r'))
    {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value
    }
}

#[cfg(test)]
mod export_tests {
    use super::*;

    #[test]
    fn converts_ndjson_rows_to_csv_without_buffering_the_source() {
        let input = b"{\"columns\":[\"id\",\"note\"]}\n[1,\"a,b\"]\n[2,null]\n";
        let mut export = ExportReader::new(&input[..], ExportFormat::Csv, None);
        let mut output = String::new();
        export.read_to_string(&mut output).unwrap();
        assert_eq!(output, "id,note\n1,\"a,b\"\n2,NULL\n");
    }

    #[test]
    fn export_row_limit_stops_after_header_and_requested_rows() {
        let input = b"{\"columns\":[\"id\"]}\n[1]\n[2]\n[3]\n";
        let mut export = ExportReader::new(&input[..], ExportFormat::Ndjson, Some(2));
        let mut output = String::new();
        export.read_to_string(&mut output).unwrap();
        assert_eq!(output, "{\"columns\":[\"id\"]}\n[1]\n[2]\n");
    }
}

/// A buffered GET used by the metrics sampler (not the request path). Returns the parsed JSON on
/// a 2xx, or `None` on any failure — the sampler must never crash the console over one bad poll.
pub fn fetch_json(resolved: &Resolved, suffix: &str) -> Option<Value> {
    fetch_json_result(resolved, suffix)
        .ok()
        .map(|(value, _)| value)
}

/// A bounded collector read with actionable status/transport errors and latency evidence.
pub fn fetch_json_result(resolved: &Resolved, suffix: &str) -> Result<(Value, u128), String> {
    const MAX_OBSERVATION_BYTES: u64 = 4 * 1024 * 1024;
    let started = Instant::now();
    let url = format!("{}/v1/{}", resolved.url, suffix);
    let agent = agent(resolved)?;
    let mut req = agent.request("GET", &url);
    if let Some(auth) = bearer(resolved) {
        req = req.set("Authorization", &auth);
    }
    let response = match req.call() {
        Ok(response) => response,
        Err(ureq::Error::Status(status, _)) => {
            return Err(format!("GET /v1/{suffix} returned HTTP {status}"))
        }
        Err(ureq::Error::Transport(error)) => return Err(error.to_string()),
    };
    let mut bytes = Vec::new();
    response
        .into_reader()
        .take(MAX_OBSERVATION_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MAX_OBSERVATION_BYTES {
        return Err(format!(
            "GET /v1/{suffix} exceeded the 4 MiB collector limit"
        ));
    }
    let value = serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    Ok((value, started.elapsed().as_millis()))
}

/// A bounded JSON POST for console-side coordinators. Unlike `forward`, this has no browser
/// response to stream into; it is used only for small operation-control responses.
pub fn post_json_result(resolved: &Resolved, suffix: &str, body: &Value) -> Result<Value, String> {
    const MAX_OPERATION_RESPONSE_BYTES: u64 = 4 * 1024 * 1024;
    let url = format!("{}/v1/{}", resolved.url, suffix);
    let agent = agent(resolved)?;
    let mut request = agent.post(&url).set("Content-Type", "application/json");
    if let Some(auth) = bearer(resolved) {
        request = request.set("Authorization", &auth);
    }
    let response = match request.send_json(body) {
        Ok(response) => response,
        Err(ureq::Error::Status(status, response)) => {
            let message = response
                .into_json::<Value>()
                .ok()
                .and_then(|value| {
                    value
                        .get("error")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .unwrap_or_else(|| format!("HTTP {status}"));
            return Err(format!("POST /v1/{suffix} returned {message}"));
        }
        Err(ureq::Error::Transport(error)) => return Err(error.to_string()),
    };
    let mut bytes = Vec::new();
    response
        .into_reader()
        .take(MAX_OPERATION_RESPONSE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MAX_OPERATION_RESPONSE_BYTES {
        return Err(format!(
            "POST /v1/{suffix} exceeded the 4 MiB operation response limit"
        ));
    }
    serde_json::from_slice(&bytes).map_err(|error| error.to_string())
}

/// Materialize one bounded ShardLite query for console-side catalog/diagnostic coordinators. The
/// public browser query path remains streaming; this helper is intentionally capped because its
/// callers need the complete, small metadata result before they can reconcile it.
pub fn query_rows(
    resolved: &Resolved,
    shard: u32,
    sql: &str,
    params: &[Value],
) -> Result<(Vec<String>, Vec<Vec<Value>>), String> {
    const MAX_METADATA_BYTES: u64 = 4 * 1024 * 1024;
    const MAX_METADATA_ROWS: usize = 20_000;
    let url = format!("{}/v1/query", resolved.url);
    let agent = agent(resolved)?;
    let mut request = agent.post(&url).set("Content-Type", "application/json");
    if let Some(auth) = bearer(resolved) {
        request = request.set("Authorization", &auth);
    }
    let response = match request.send_json(json::query_body(shard, sql, params)) {
        Ok(response) => response,
        Err(ureq::Error::Status(status, response)) => {
            let message = response
                .into_json::<Value>()
                .ok()
                .and_then(|value| {
                    value
                        .get("error")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .unwrap_or_else(|| format!("HTTP {status}"));
            return Err(format!("metadata query returned {message}"));
        }
        Err(ureq::Error::Transport(error)) => return Err(error.to_string()),
    };
    let mut bytes = Vec::new();
    response
        .into_reader()
        .take(MAX_METADATA_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MAX_METADATA_BYTES {
        return Err("metadata query exceeded the 4 MiB console limit".into());
    }
    let text = String::from_utf8(bytes).map_err(|error| error.to_string())?;
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let value: Value = serde_json::from_str(line).map_err(|error| error.to_string())?;
        if let Some(values) = value.as_array() {
            if rows.len() >= MAX_METADATA_ROWS {
                return Err("metadata query exceeded the 20,000 row console limit".into());
            }
            rows.push(values.clone());
        } else if let Some(values) = value.get("columns").and_then(Value::as_array) {
            columns = values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect();
        } else if let Some(error) = value.get("error").and_then(Value::as_str) {
            return Err(error.to_string());
        }
    }
    Ok((columns, rows))
}

mod json {
    use serde_json::{json, Value};

    pub fn query_body(shard: u32, sql: &str, params: &[Value]) -> Value {
        json!({
            "shard": shard,
            "sql": sql,
            "params": params,
            "consistency": "linearizable",
        })
    }
}

pub fn test_connection(resolved: &Resolved) -> Result<(u128, Value), String> {
    let started = Instant::now();
    let value = fetch_json(resolved, "info")
        .ok_or_else(|| "the cluster did not return a successful /v1/info response".to_string())?;
    Ok((started.elapsed().as_millis(), value))
}
