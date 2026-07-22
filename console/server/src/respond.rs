//! Small helpers for writing responses, so the routing code stays about routing.

use serde_json::Value;
use tiny_http::{Header, Request, Response, StatusCode};

fn json_header() -> Header {
    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).expect("static header")
}

fn security_headers() -> Vec<Header> {
    vec![
        Header::from_bytes(&b"Cache-Control"[..], &b"no-store"[..]).expect("cache header"),
        Header::from_bytes(&b"X-Content-Type-Options"[..], &b"nosniff"[..])
            .expect("nosniff header"),
        Header::from_bytes(&b"X-Frame-Options"[..], &b"DENY"[..]).expect("frame header"),
        Header::from_bytes(&b"Referrer-Policy"[..], &b"no-referrer"[..]).expect("referrer header"),
    ]
}

pub fn respond_json(request: Request, status: u16, body: &Value) -> std::io::Result<()> {
    let data = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
    let mut response = Response::from_data(data)
        .with_status_code(StatusCode(status))
        .with_header(json_header());
    for header in security_headers() {
        response.add_header(header);
    }
    request.respond(response)
}

pub fn respond_json_with_cookie(
    request: Request,
    status: u16,
    body: &Value,
    cookie: &str,
) -> std::io::Result<()> {
    let data = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
    let cookie_header =
        Header::from_bytes(&b"Set-Cookie"[..], cookie.as_bytes()).expect("cookie header");
    let mut response = Response::from_data(data)
        .with_status_code(StatusCode(status))
        .with_header(json_header())
        .with_header(cookie_header);
    for header in security_headers() {
        response.add_header(header);
    }
    request.respond(response)
}

pub fn error(request: Request, status: u16, message: &str) -> std::io::Result<()> {
    respond_json(request, status, &serde_json::json!({ "error": message }))
}
