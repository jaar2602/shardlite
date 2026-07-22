//! The S3 client (slice 1 of S3-backed HA). Two things are worth proving without a real bucket:
//! that the SigV4 signature matches AWS's own worked example (the correctness-critical bit), and
//! that a real signed request round-trips — sent, received, parsed — against a mock S3 server.
#![cfg(feature = "s3")]

use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use meshdb::s3::{S3Client, S3Config, S3Error, amz_timestamp, sign_v4};

#[test]
fn sigv4_matches_the_aws_worked_example() {
    // AWS's published SigV4 test vector "get-vanilla": GET / with a fixed date and keys, whose
    // expected Authorization header is documented. If our canonical request, string-to-sign,
    // signing-key derivation, or HMAC chain is wrong, this signature won't match.
    let mut headers = BTreeMap::new();
    headers.insert("host".to_string(), "example.amazonaws.com".to_string());
    headers.insert("x-amz-date".to_string(), "20150830T123600Z".to_string());
    let empty_sha = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    let authz = sign_v4(
        "AKIDEXAMPLE",
        "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
        "us-east-1",
        "service",
        "GET",
        "/",
        "",
        &headers,
        empty_sha,
        "20150830T123600Z",
        "20150830",
    );

    assert_eq!(
        authz,
        "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
         SignedHeaders=host;x-amz-date, \
         Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
    );
}

#[test]
fn amz_timestamp_formats_utc() {
    // 2015-08-30T12:36:00Z — the same instant as the vector above.
    assert_eq!(
        amz_timestamp(1_440_938_160),
        ("20150830T123600Z".to_string(), "20150830".to_string())
    );
    // Epoch and a leap day, to exercise the civil-date math.
    assert_eq!(
        amz_timestamp(0),
        ("19700101T000000Z".to_string(), "19700101".to_string())
    );
    assert_eq!(
        amz_timestamp(1_709_164_800),
        ("20240229T000000Z".to_string(), "20240229".to_string())
    );
}

#[test]
fn a_signed_request_round_trips_against_a_mock_s3() {
    let (endpoint, seen_auth) = spawn_mock_s3();
    let client = S3Client::new(S3Config {
        endpoint,
        bucket: "meshdb".into(),
        region: "us-east-1".into(),
        access_key: "AKIDEXAMPLE".into(),
        secret_key: "secret".into(),
    });

    // PUT then GET the same object.
    client
        .put("shard_0/snapshot", b"hello object storage")
        .unwrap();
    assert_eq!(
        client.get("shard_0/snapshot").unwrap(),
        b"hello object storage"
    );

    // A byte-range read — the failover page path.
    assert_eq!(
        client.get_range("shard_0/snapshot", 6, 6).unwrap(),
        b"object"
    );

    // DELETE, then GET must 404.
    client.delete("shard_0/snapshot").unwrap();
    match client.get("shard_0/snapshot") {
        Err(S3Error::Status { code: 404, .. }) => {}
        other => panic!("expected a 404 after delete, got {other:?}"),
    }

    // LIST parses the keys out of the ListObjectsV2 XML.
    let mut keys = client.list("shard_0/").unwrap();
    keys.sort();
    assert_eq!(keys, vec!["shard_0/a".to_string(), "shard_0/b".to_string()]);

    // Every request the mock saw carried a well-formed SigV4 Authorization header.
    let auths = seen_auth.lock().unwrap();
    assert!(!auths.is_empty());
    for a in auths.iter() {
        assert!(
            a.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/")
                && a.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date")
                && a.contains("Signature="),
            "not a valid SigV4 header: {a}"
        );
    }
}

/// A tiny in-process S3-ish server: a key→bytes store over raw HTTP/1.1. It records every
/// Authorization header so the test can assert the client signed each request.
fn spawn_mock_s3() -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let store: Arc<Mutex<HashMap<String, Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_ret = Arc::clone(&seen);
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { continue };
            handle(&mut s, &store, &seen);
        }
    });
    (format!("http://{addr}"), seen_ret)
}

fn handle(
    s: &mut TcpStream,
    store: &Arc<Mutex<HashMap<String, Vec<u8>>>>,
    seen: &Arc<Mutex<Vec<String>>>,
) {
    let (method, path, headers, body) = read_request(s);
    if let Some(a) = headers.get("authorization") {
        seen.lock().unwrap().push(a.clone());
    }
    let key = path.trim_start_matches('/').to_string();

    match method.as_str() {
        "PUT" => {
            store.lock().unwrap().insert(key, body);
            respond(s, "200 OK", &[]);
        }
        "GET" if path.contains("list-type=2") => {
            let xml =
                b"<ListBucketResult><Key>shard_0/a</Key><Key>shard_0/b</Key></ListBucketResult>";
            respond(s, "200 OK", xml);
        }
        "GET" => match store.lock().unwrap().get(&key).cloned() {
            Some(bytes) => match headers.get("range").and_then(|r| parse_range(r)) {
                Some((start, end)) => {
                    let slice = &bytes[start..=end.min(bytes.len() - 1)];
                    respond(s, "206 Partial Content", slice);
                }
                None => respond(s, "200 OK", &bytes),
            },
            None => respond(s, "404 Not Found", b"NoSuchKey"),
        },
        "DELETE" => {
            store.lock().unwrap().remove(&key);
            respond(s, "204 No Content", &[]);
        }
        _ => respond(s, "400 Bad Request", &[]),
    }
}

fn read_request(s: &mut TcpStream) -> (String, String, HashMap<String, String>, Vec<u8>) {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while s.read(&mut byte).unwrap_or(0) == 1 {
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let text = String::from_utf8_lossy(&head).to_string();
    let mut lines = text.lines();
    let mut req = lines.next().unwrap_or("").split_whitespace();
    let method = req.next().unwrap_or("").to_string();
    let path = req.next().unwrap_or("").to_string();
    let mut headers = HashMap::new();
    let mut clen = 0usize;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim().to_ascii_lowercase();
            let v = v.trim().to_string();
            if k == "content-length" {
                clen = v.parse().unwrap_or(0);
            }
            headers.insert(k, v);
        }
    }
    let mut body = vec![0u8; clen];
    if clen > 0 {
        s.read_exact(&mut body).unwrap();
    }
    (method, path, headers, body)
}

fn parse_range(r: &str) -> Option<(usize, usize)> {
    let spec = r.strip_prefix("bytes=")?;
    let (a, b) = spec.split_once('-')?;
    Some((a.parse().ok()?, b.parse().ok()?))
}

fn respond(s: &mut TcpStream, status: &str, body: &[u8]) {
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = s.write_all(head.as_bytes());
    let _ = s.write_all(body);
    let _ = s.flush();
}
