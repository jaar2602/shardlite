//! A minimal, **blocking** S3 client — enough to archive a shard to object storage and read it
//! back, without pulling in an async runtime. meshdb is deliberately tokio-free, so this signs
//! requests with AWS Signature Version 4 by hand and speaks HTTP/1.1 (+ TLS) over `ureq`.
//!
//! Scope is deliberately small: `PUT` / `GET` (with byte ranges) / `DELETE` / `LIST`, path-style
//! addressing (`endpoint/bucket/key`), and SigV4 for the `s3` service. That covers the archival
//! sink (snapshot + change-log objects), the failover read path (range reads), and lifecycle
//! (list + delete of superseded objects). It is not a general SDK.
//!
//! The signing (`sign_v4`) is the correctness-critical part and is unit-tested against AWS's
//! published SigV4 example; the request/round-trip is tested against an in-process mock server.

use std::collections::BTreeMap;
use std::io::Read;
use std::time::Duration;

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

pub mod pager;
pub mod sink;
pub use pager::S3Pager;
pub use sink::S3Sink;

type HmacSha256 = Hmac<Sha256>;

/// Where and how to reach an S3(-compatible) bucket. `endpoint` is the scheme + host (+ optional
/// port), e.g. `https://s3.us-east-1.amazonaws.com` for AWS or `http://127.0.0.1:9000` for MinIO.
#[derive(Debug, Clone)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
}

/// A failed S3 operation.
#[derive(Debug)]
pub enum S3Error {
    /// The server answered with a non-2xx status (body included for diagnosis).
    Status { code: u16, body: String },
    /// The request never completed (DNS, connect, TLS, timeout, read).
    Transport(String),
}

impl std::fmt::Display for S3Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            S3Error::Status { code, body } => write!(f, "S3 returned {code}: {body}"),
            S3Error::Transport(e) => write!(f, "S3 transport error: {e}"),
        }
    }
}

impl std::error::Error for S3Error {}

pub type Result<T> = std::result::Result<T, S3Error>;

/// A blocking S3 client bound to one bucket.
pub struct S3Client {
    cfg: S3Config,
    agent: ureq::Agent,
}

impl S3Client {
    pub fn new(cfg: S3Config) -> Self {
        let agent = ureq::AgentBuilder::new()
            .timeout(Duration::from_secs(30))
            .build();
        Self { cfg, agent }
    }

    /// Upload `body` to `key`, overwriting.
    pub fn put(&self, key: &str, body: &[u8]) -> Result<()> {
        self.send("PUT", key, "", "", body, &[]).map(|_| ())
    }

    /// Download the whole object at `key`.
    pub fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.send("GET", key, "", "", &[], &[])
    }

    /// Download `len` bytes starting at `start` — an HTTP Range read, the failover page path.
    pub fn get_range(&self, key: &str, start: u64, len: u64) -> Result<Vec<u8>> {
        let end = start + len - 1;
        let range = format!("bytes={start}-{end}");
        // Range is not an x-amz-* header, so it need not be signed — sent, but not part of SigV4.
        self.send("GET", key, "", "", &[], &[("Range", range)])
    }

    /// The size of the object at `key`, without downloading it — a `HEAD`, so the pager knows the
    /// database length for `xFileSize` before faulting any page.
    pub fn head(&self, key: &str) -> Result<u64> {
        let (amzdate, datestamp) = amz_timestamp(now_unix());
        let host = host_of(&self.cfg.endpoint).to_string();
        let payload_hash = sha256_hex(&[]);
        let canonical_uri = canonical_path(&self.cfg.bucket, key);
        let mut signed: BTreeMap<String, String> = BTreeMap::new();
        signed.insert("host".into(), host);
        signed.insert("x-amz-content-sha256".into(), payload_hash.clone());
        signed.insert("x-amz-date".into(), amzdate.clone());
        let authorization = sign_v4(
            &self.cfg.access_key,
            &self.cfg.secret_key,
            &self.cfg.region,
            "s3",
            "HEAD",
            &canonical_uri,
            "",
            &signed,
            &payload_hash,
            &amzdate,
            &datestamp,
        );
        let url = format!("{}{canonical_uri}", self.cfg.endpoint.trim_end_matches('/'));
        let resp = self
            .agent
            .request("HEAD", &url)
            .set("Authorization", &authorization)
            .set("x-amz-content-sha256", &payload_hash)
            .set("x-amz-date", &amzdate)
            .call();
        match resp {
            Ok(r) => r
                .header("Content-Length")
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| S3Error::Transport("HEAD response had no Content-Length".into())),
            Err(ureq::Error::Status(code, r)) => Err(S3Error::Status {
                code,
                body: r.into_string().unwrap_or_default(),
            }),
            Err(e) => Err(S3Error::Transport(e.to_string())),
        }
    }

    /// Delete the object at `key`.
    pub fn delete(&self, key: &str) -> Result<()> {
        self.send("DELETE", key, "", "", &[], &[]).map(|_| ())
    }

    /// List object keys under `prefix` (ListObjectsV2). Follows continuation tokens.
    pub fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            // Query parameters must be sorted by key in both the canonical string and the URL.
            let mut params: BTreeMap<String, String> = BTreeMap::new();
            params.insert("list-type".into(), "2".into());
            if !prefix.is_empty() {
                params.insert("prefix".into(), prefix.into());
            }
            if let Some(t) = &token {
                params.insert("continuation-token".into(), t.clone());
            }
            let query: String = params
                .iter()
                .map(|(k, v)| format!("{}={}", uri_encode(k, false), uri_encode(v, false)))
                .collect::<Vec<_>>()
                .join("&");
            // The bucket root is the "key" for a list.
            let body = self.send("GET", "", &query, &query, &[], &[])?;
            let xml = String::from_utf8_lossy(&body);
            for k in between_all(&xml, "<Key>", "</Key>") {
                keys.push(xml_unescape(&k));
            }
            match between(&xml, "<NextContinuationToken>", "</NextContinuationToken>") {
                Some(t) => token = Some(xml_unescape(&t)),
                None => break,
            }
        }
        Ok(keys)
    }

    /// Sign and send one request, returning the response body. `canonical_query` is the SigV4
    /// canonical query string (sorted, encoded); `url_query` is what goes after `?` in the URL —
    /// they are the same here. `unsigned` headers are sent but excluded from the signature.
    fn send(
        &self,
        method: &str,
        key: &str,
        canonical_query: &str,
        url_query: &str,
        body: &[u8],
        unsigned: &[(&str, String)],
    ) -> Result<Vec<u8>> {
        let (amzdate, datestamp) = amz_timestamp(now_unix());
        let host = host_of(&self.cfg.endpoint).to_string();
        let payload_hash = sha256_hex(body);
        let canonical_uri = canonical_path(&self.cfg.bucket, key);

        // The three headers SigV4 must cover for S3: host, the payload hash, and the date.
        let mut signed: BTreeMap<String, String> = BTreeMap::new();
        signed.insert("host".into(), host);
        signed.insert("x-amz-content-sha256".into(), payload_hash.clone());
        signed.insert("x-amz-date".into(), amzdate.clone());

        let authorization = sign_v4(
            &self.cfg.access_key,
            &self.cfg.secret_key,
            &self.cfg.region,
            "s3",
            method,
            &canonical_uri,
            canonical_query,
            &signed,
            &payload_hash,
            &amzdate,
            &datestamp,
        );

        let base = self.cfg.endpoint.trim_end_matches('/');
        let url = if url_query.is_empty() {
            format!("{base}{canonical_uri}")
        } else {
            format!("{base}{canonical_uri}?{url_query}")
        };

        // `Host` is set by ureq from the URL and must match what we signed — so we never set it by
        // hand. The other signed headers are sent verbatim.
        let mut req = self
            .agent
            .request(method, &url)
            .set("Authorization", &authorization)
            .set("x-amz-content-sha256", &payload_hash)
            .set("x-amz-date", &amzdate);
        for (k, v) in unsigned {
            req = req.set(k, v);
        }

        let resp = if body.is_empty() {
            req.call()
        } else {
            req.send_bytes(body)
        };
        match resp {
            Ok(r) => {
                let mut buf = Vec::new();
                r.into_reader()
                    .read_to_end(&mut buf)
                    .map_err(|e| S3Error::Transport(e.to_string()))?;
                Ok(buf)
            }
            Err(ureq::Error::Status(code, r)) => Err(S3Error::Status {
                code,
                body: r.into_string().unwrap_or_default(),
            }),
            Err(e) => Err(S3Error::Transport(e.to_string())),
        }
    }
}

/// Compute an AWS SigV4 `Authorization` header value. Split out from the client and pure, so it can
/// be checked against AWS's published example independent of any network.
#[allow(clippy::too_many_arguments)]
pub fn sign_v4(
    access_key: &str,
    secret_key: &str,
    region: &str,
    service: &str,
    method: &str,
    canonical_uri: &str,
    canonical_query: &str,
    signed_headers: &BTreeMap<String, String>,
    payload_hash: &str,
    amzdate: &str,
    datestamp: &str,
) -> String {
    // Canonical headers: lowercase name, trimmed value, one per line, sorted (BTreeMap is sorted).
    let canonical_headers: String = signed_headers
        .iter()
        .map(|(k, v)| format!("{}:{}\n", k, v.trim()))
        .collect();
    let signed_list = signed_headers.keys().cloned().collect::<Vec<_>>().join(";");

    let canonical_request = format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_list}\n{payload_hash}"
    );
    let scope = format!("{datestamp}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amzdate}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );

    let signing_key = signing_key(secret_key, datestamp, region, service);
    let signature = hex(&hmac(&signing_key, string_to_sign.as_bytes()));

    format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_list}, Signature={signature}"
    )
}

fn signing_key(secret: &str, datestamp: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac(format!("AWS4{secret}").as_bytes(), datestamp.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, service.as_bytes());
    hmac(&k_service, b"aws4_request")
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Path-style canonical URI: `/bucket/key`, each part URI-encoded (slashes in the key preserved).
fn canonical_path(bucket: &str, key: &str) -> String {
    if key.is_empty() {
        format!("/{}", uri_encode(bucket, false))
    } else {
        format!("/{}/{}", uri_encode(bucket, false), uri_encode(key, true))
    }
}

/// RFC-3986 unreserved set stay literal; everything else is percent-encoded. `/` is kept when
/// `keep_slash` (path segments), encoded otherwise (query values).
fn uri_encode(s: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn host_of(endpoint: &str) -> &str {
    let no_scheme = endpoint.split("://").nth(1).unwrap_or(endpoint);
    no_scheme.split('/').next().unwrap_or(no_scheme)
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Unix seconds → (`YYYYMMDDTHHMMSSZ`, `YYYYMMDD`) in UTC. Hand-rolled civil-date conversion
/// (Howard Hinnant's algorithm) so no calendar crate is needed. Split out so signing is testable
/// against a fixed timestamp.
pub fn amz_timestamp(secs: u64) -> (String, String) {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);

    let z = days + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };

    (
        format!("{year:04}{month:02}{d:02}T{h:02}{m:02}{s:02}Z"),
        format!("{year:04}{month:02}{d:02}"),
    )
}

fn between(s: &str, open: &str, close: &str) -> Option<String> {
    let start = s.find(open)? + open.len();
    let end = s[start..].find(close)? + start;
    Some(s[start..end].to_string())
}

fn between_all(s: &str, open: &str, close: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = s;
    while let Some(start) = rest.find(open) {
        let from = start + open.len();
        if let Some(end) = rest[from..].find(close) {
            out.push(rest[from..from + end].to_string());
            rest = &rest[from + end + close.len()..];
        } else {
            break;
        }
    }
    out
}

fn xml_unescape(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}
