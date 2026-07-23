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

// --- slice 2: the S3 sink (change-log + snapshots) ---

use meshdb::replication::{FrameSink, StreamTxn};
use meshdb::s3::S3Sink;
use meshdb::s3::sink::decode_change_log;
use meshdb::shard::ShardId;
use meshdb::vfs::{CommittedTxn, Frame};

fn sample_txn(lsn: u64, page: u32) -> StreamTxn {
    StreamTxn {
        lsn,
        txn: CommittedTxn {
            db_size_pages: page,
            page_size: 4096,
            frames: vec![Frame {
                page_no: page,
                data: vec![lsn as u8; 16],
            }],
            generation: 0,
        },
    }
}

#[test]
fn the_sink_uploads_the_change_log_and_snapshots() {
    let (endpoint, _seen) = spawn_mock_s3();
    let client = Arc::new(S3Client::new(S3Config {
        endpoint,
        bucket: "meshdb".into(),
        region: "us-east-1".into(),
        access_key: "AKIDEXAMPLE".into(),
        secret_key: "s".into(),
    }));
    let sink = S3Sink::new(Arc::clone(&client), "arch");

    let txns = vec![sample_txn(1, 1), sample_txn(2, 2)];
    sink.accept(ShardId(3), 1, txns.clone()).unwrap();
    // flush waits for the async upload to finish and reports the sink healthy.
    sink.flush().unwrap();

    // The change-log landed where the sink keys it (padded epoch_firstlsn), and decodes back to
    // exactly the transactions handed in.
    let key = "arch/shard_3/wal/00000000000000000001_00000000000000000001";
    let got = decode_change_log(&client.get(key).unwrap()).unwrap();
    assert_eq!(got, txns);

    // A snapshot uploads under its own (epoch, lsn) key.
    sink.put_snapshot(ShardId(3), 2, 9, b"SNAPSHOT-BYTES")
        .unwrap();
    assert_eq!(
        client
            .get("arch/shard_3/snapshot/00000000000000000002_00000000000000000009")
            .unwrap(),
        b"SNAPSHOT-BYTES"
    );
}

#[test]
fn a_persistently_failing_sink_reports_unhealthy() {
    // Nothing is listening on this port, so uploads fail. The failure must surface through flush,
    // so the writer stops rather than committing data that never reached S3.
    let client = Arc::new(S3Client::new(S3Config {
        endpoint: "http://127.0.0.1:1".into(),
        bucket: "b".into(),
        region: "us-east-1".into(),
        access_key: "a".into(),
        secret_key: "s".into(),
    }));
    let sink = S3Sink::new(client, "arch");
    sink.accept(ShardId(0), 1, vec![sample_txn(1, 1)]).unwrap();
    assert!(
        sink.flush().is_err(),
        "a sink that cannot reach S3 must report unhealthy"
    );
}

// --- slice 3: the S3 pager (chunked range reads + LRU cache) ---

use meshdb::s3::S3Pager;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A mock serving one fixed object: HEAD returns its size, GET (Range) returns the slice, and
/// every GET is counted so a test can prove the cache avoids re-fetching.
fn spawn_object_mock(data: Vec<u8>) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let gets = Arc::new(AtomicUsize::new(0));
    let gets_ret = Arc::clone(&gets);
    let data = Arc::new(data);
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { continue };
            let (method, _path, headers, _body) = read_request(&mut s);
            match method.as_str() {
                "HEAD" => {
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        data.len()
                    );
                    let _ = s.write_all(head.as_bytes());
                    let _ = s.flush();
                }
                "GET" => {
                    gets.fetch_add(1, Ordering::SeqCst);
                    match headers.get("range").and_then(|r| parse_range(r)) {
                        Some((a, b)) => {
                            let b = b.min(data.len() - 1);
                            respond(&mut s, "206 Partial Content", &data[a..=b]);
                        }
                        None => respond(&mut s, "200 OK", &data),
                    }
                }
                _ => respond(&mut s, "400 Bad Request", &[]),
            }
        }
    });
    (format!("http://{addr}"), gets_ret)
}

#[test]
fn the_pager_reads_ranges_and_caches_chunks() {
    // A 200 KiB object of predictable bytes.
    let data: Vec<u8> = (0..200 * 1024)
        .map(|i| ((i * 31 + 7) % 251) as u8)
        .collect();
    let (endpoint, gets) = spawn_object_mock(data.clone());
    let client = Arc::new(S3Client::new(S3Config {
        endpoint,
        bucket: "b".into(),
        region: "us-east-1".into(),
        access_key: "a".into(),
        secret_key: "s".into(),
    }));
    // 64 KiB chunks, cache room for 4.
    let pager =
        S3Pager::with_limits(Arc::clone(&client), "snap", 64 * 1024, 4 * 64 * 1024).unwrap();
    assert_eq!(pager.size(), data.len() as u64);

    // A 4 KiB read spanning the chunk-0/chunk-1 boundary.
    let mut buf = vec![0u8; 4096];
    assert_eq!(pager.read_at(65536 - 100, &mut buf).unwrap(), 4096);
    assert_eq!(&buf[..], &data[65536 - 100..65536 - 100 + 4096]);

    // A re-read fully inside chunk 0 must not hit S3 again.
    let before = gets.load(Ordering::SeqCst);
    let mut b2 = vec![0u8; 100];
    pager.read_at(65436, &mut b2).unwrap();
    assert_eq!(
        gets.load(Ordering::SeqCst),
        before,
        "a cached read must not fetch from S3"
    );
    assert_eq!(&b2[..], &data[65436..65536]);

    // The tail returns exactly the remaining bytes; past EOF returns nothing.
    let mut tail = vec![0u8; 4096];
    assert_eq!(
        pager.read_at(data.len() as u64 - 10, &mut tail).unwrap(),
        10
    );
    assert_eq!(&tail[..10], &data[data.len() - 10..]);
    assert_eq!(pager.read_at(data.len() as u64, &mut tail).unwrap(), 0);
}

// --- slice 3 part 2: the read-only S3-backed VFS ---

#[test]
fn a_snapshot_in_s3_is_queryable_over_the_read_vfs() {
    // Build a real SQLite database on disk...
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snap.db");
    {
        let c = rusqlite::Connection::open(&path).unwrap();
        // DELETE journal so the .db is self-contained (no -wal); immutable=1 then reads it directly.
        c.execute_batch("PRAGMA journal_mode=DELETE;").unwrap();
        c.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT) STRICT;")
            .unwrap();
        for i in 0..500i64 {
            c.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("row-{i}")],
            )
            .unwrap();
        }
    }

    // ...serve its bytes from a mock S3, and query it over the read-only VFS — no download.
    let bytes = std::fs::read(&path).unwrap();
    let (endpoint, gets) = spawn_object_mock(bytes);
    let client = Arc::new(S3Client::new(S3Config {
        endpoint,
        bucket: "b".into(),
        region: "us-east-1".into(),
        access_key: "a".into(),
        secret_key: "s".into(),
    }));

    let conn = meshdb::s3::open_readonly(client, "snap.db").unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 500);
    let v: String = conn
        .query_row("SELECT v FROM t WHERE id = 42", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, "row-42");

    // Pages were faulted from S3 (the read path actually ran), and writes are refused.
    assert!(gets.load(Ordering::SeqCst) > 0, "no page reads hit S3");
    assert!(
        conn.execute("INSERT INTO t VALUES (99999, 'x')", [])
            .is_err(),
        "a read-only S3 snapshot must reject writes"
    );
}

// --- slice 3c: the read-write overlay (S3 base + local WAL) ---

#[test]
fn a_failed_over_shard_takes_writes_over_a_local_wal() {
    // A WAL-mode snapshot with 300 "old" rows, checkpointed so the .db is complete.
    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("base.db");
    {
        let c = rusqlite::Connection::open(&snap).unwrap();
        c.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        c.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT) STRICT;")
            .unwrap();
        for i in 0..300i64 {
            c.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("old-{i}")],
            )
            .unwrap();
        }
        c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }
    let bytes = std::fs::read(&snap).unwrap();

    // Serve the base from S3; open it read-WRITE with the overlay VFS (base on S3, -wal local).
    let (endpoint, _gets) = spawn_object_mock(bytes);
    let client = Arc::new(S3Client::new(S3Config {
        endpoint,
        bucket: "b".into(),
        region: "us-east-1".into(),
        access_key: "a".into(),
        secret_key: "s".into(),
    }));
    let scratch = tempfile::tempdir().unwrap();
    let conn = meshdb::s3::open_readwrite(client, "base.db", scratch.path()).unwrap();

    // Old rows are readable straight from the S3 base.
    assert_eq!(
        conn.query_row("SELECT count(*) FROM t", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        300
    );
    assert_eq!(
        conn.query_row("SELECT v FROM t WHERE id = 42", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "old-42"
    );

    // New writes land in the local WAL — instantly, no download.
    for i in 1000..1010i64 {
        conn.execute(
            "INSERT INTO t VALUES (?1, ?2)",
            rusqlite::params![i, format!("new-{i}")],
        )
        .unwrap();
    }

    // Reads now merge the local WAL over the S3 base: both old and new rows are visible.
    assert_eq!(
        conn.query_row("SELECT count(*) FROM t", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        310
    );
    assert_eq!(
        conn.query_row("SELECT v FROM t WHERE id = 1005", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "new-1005"
    );
    assert_eq!(
        conn.query_row("SELECT v FROM t WHERE id = 7", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "old-7"
    );
}

// --- slice 4: serving a shard from S3 on failover ---

/// A fuller mock S3: a real key→bytes store supporting PUT / HEAD / GET(+Range) / DELETE / LIST.
/// Keys are stored without the leading bucket segment, matching the client's addressing.
fn spawn_full_s3() -> String {
    use std::collections::BTreeMap;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let store: Arc<Mutex<BTreeMap<String, Vec<u8>>>> = Arc::new(Mutex::new(BTreeMap::new()));
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { continue };
            let (method, path, headers, body) = read_request(&mut s);
            let (full, query) = match path.split_once('?') {
                Some((p, q)) => (p, q),
                None => (path.as_str(), ""),
            };
            // Drop the leading "/bucket" to get the object key.
            let key = full
                .trim_start_matches('/')
                .split_once('/')
                .map(|(_, k)| k.to_string())
                .unwrap_or_default();
            let mut st = store.lock().unwrap();
            match method.as_str() {
                "PUT" => {
                    st.insert(key, body);
                    respond(&mut s, "200 OK", &[]);
                }
                "HEAD" => match st.get(&key) {
                    Some(b) => {
                        let h = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            b.len()
                        );
                        let _ = s.write_all(h.as_bytes());
                    }
                    None => {
                        let _ = s.write_all(
                            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        );
                    }
                },
                "GET" if query.contains("list-type=2") => {
                    let prefix = query
                        .split('&')
                        .find_map(|kv| kv.strip_prefix("prefix="))
                        .map(urldecode)
                        .unwrap_or_default();
                    let mut xml = String::from("<ListBucketResult>");
                    for k in st.keys().filter(|k| k.starts_with(&prefix)) {
                        xml.push_str(&format!("<Key>{k}</Key>"));
                    }
                    xml.push_str("</ListBucketResult>");
                    respond(&mut s, "200 OK", xml.as_bytes());
                }
                "GET" => match st.get(&key).cloned() {
                    Some(b) => match headers.get("range").and_then(|r| parse_range(r)) {
                        Some((a, e)) => {
                            let e = e.min(b.len() - 1);
                            respond(&mut s, "206 Partial Content", &b[a..=e]);
                        }
                        None => respond(&mut s, "200 OK", &b),
                    },
                    None => respond(&mut s, "404 Not Found", b"NoSuchKey"),
                },
                "DELETE" => {
                    st.remove(&key);
                    respond(&mut s, "204 No Content", &[]);
                }
                _ => respond(&mut s, "400 Bad Request", &[]),
            }
        }
    });
    format!("http://{addr}")
}

fn urldecode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let Ok(byte) = u8::from_str_radix(&s[i + 1..i + 3], 16)
        {
            out.push(byte);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[test]
fn a_survivor_serves_a_shard_from_its_latest_s3_snapshot() {
    // Node A archives a WAL-mode snapshot of shard 3 to S3.
    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("s.db");
    {
        let c = rusqlite::Connection::open(&snap).unwrap();
        c.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        c.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT) STRICT;")
            .unwrap();
        for i in 0..200i64 {
            c.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("a-{i}")],
            )
            .unwrap();
        }
        c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }
    let bytes = std::fs::read(&snap).unwrap();

    let client = Arc::new(S3Client::new(S3Config {
        endpoint: spawn_full_s3(),
        bucket: "meshdb".into(),
        region: "us-east-1".into(),
        access_key: "a".into(),
        secret_key: "s".into(),
    }));
    let sink = S3Sink::new(Arc::clone(&client), "arch");
    // An older, stale snapshot plus the real latest one — the survivor must pick the latest.
    sink.put_snapshot(ShardId(3), 3, 100, b"OLD-STALE-SNAPSHOT")
        .unwrap();
    sink.put_snapshot(ShardId(3), 5, 200, &bytes).unwrap();

    // The latest snapshot is the epoch-5 one.
    assert_eq!(
        meshdb::s3::failover::latest_snapshot(&client, "arch", ShardId(3))
            .unwrap()
            .unwrap(),
        "arch/shard_3/snapshot/00000000000000000005_00000000000000000200"
    );

    // Node B takes over shard 3: opens it from S3, no download, and serves it.
    let scratch = tempfile::tempdir().unwrap();
    let conn =
        meshdb::s3::open_from_s3(Arc::clone(&client), "arch", ShardId(3), scratch.path()).unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM t", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        200
    );
    assert_eq!(
        conn.query_row("SELECT v FROM t WHERE id = 11", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "a-11"
    );

    // And it takes new writes locally, merged over the S3 base.
    conn.execute("INSERT INTO t VALUES (9999, 'b-write')", [])
        .unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM t", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        201
    );
    assert_eq!(
        conn.query_row("SELECT v FROM t WHERE id = 9999", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "b-write"
    );
}

/// The SQLite page size, from the database header (offset 16, big-endian; a stored 1 means 65536).
fn page_size_of(db: &[u8]) -> usize {
    let raw = u16::from_be_bytes([db[16], db[17]]) as usize;
    if raw == 1 { 65536 } else { raw }
}

/// The page-level diff `base → after`, as the capture VFS would emit it: every page that changed or
/// was newly added, plus the resulting database size. Overlaying these on `base` reproduces `after`.
fn change_log_txn(base: &[u8], after: &[u8], lsn: u64) -> meshdb::replication::StreamTxn {
    let ps = page_size_of(after);
    let after_pages = after.len() / ps;
    let base_pages = base.len() / ps;
    let mut frames = Vec::new();
    for p in 0..after_pages {
        let a = &after[p * ps..(p + 1) * ps];
        let changed = p >= base_pages || a != &base[p * ps..(p + 1) * ps];
        if changed {
            frames.push(meshdb::vfs::Frame {
                page_no: (p + 1) as u32,
                data: a.to_vec(),
            });
        }
    }
    meshdb::replication::StreamTxn {
        lsn,
        txn: meshdb::vfs::CommittedTxn {
            db_size_pages: after_pages as u32,
            page_size: ps as u32,
            frames,
            generation: 0,
        },
    }
}

#[test]
fn a_failover_replays_the_change_log_since_the_snapshot() {
    // Node A's snapshot of shard 3: rows 0..200. This is the base a failover would otherwise be
    // pinned to — everything after it is only in the change-log.
    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("base.db");
    let build = |path: &std::path::Path| {
        let c = rusqlite::Connection::open(path).unwrap();
        c.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        c
    };
    {
        let c = build(&snap);
        c.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT) STRICT;")
            .unwrap();
        for i in 0..200i64 {
            c.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("a-{i}")],
            )
            .unwrap();
        }
        c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }
    let base_bytes = std::fs::read(&snap).unwrap();

    // After the snapshot, node A commits two more transactions: ten new rows, then an update. We
    // materialise each committed state and diff it to reproduce the frames the capture VFS teed.
    let after1 = dir.path().join("after1.db");
    std::fs::copy(&snap, &after1).unwrap();
    {
        let c = build(&after1);
        for i in 200..210i64 {
            c.execute(
                "INSERT INTO t VALUES (?1, ?2)",
                rusqlite::params![i, format!("new-{i}")],
            )
            .unwrap();
        }
        c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }
    let after1_bytes = std::fs::read(&after1).unwrap();

    let after2 = dir.path().join("after2.db");
    std::fs::copy(&after1, &after2).unwrap();
    {
        let c = build(&after2);
        c.execute("UPDATE t SET v = 'changed' WHERE id = 11", [])
            .unwrap();
        c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    }
    let after2_bytes = std::fs::read(&after2).unwrap();

    // Snapshot at lsn 200; two change-log transactions at lsn 201 and 202.
    let client = Arc::new(S3Client::new(S3Config {
        endpoint: spawn_full_s3(),
        bucket: "meshdb".into(),
        region: "us-east-1".into(),
        access_key: "a".into(),
        secret_key: "s".into(),
    }));
    let sink = S3Sink::new(Arc::clone(&client), "arch");
    sink.put_snapshot(ShardId(3), 7, 200, &base_bytes).unwrap();
    let t1 = change_log_txn(&base_bytes, &after1_bytes, 201);
    let t2 = change_log_txn(&after1_bytes, &after2_bytes, 202);
    // Two separate WAL objects, exactly as the sink would upload two batches.
    let enc = meshdb::s3::sink::encode_change_log;
    client
        .put(
            "arch/shard_3/wal/00000000000000000007_00000000000000000201",
            &enc(&[t1]).unwrap(),
        )
        .unwrap();
    client
        .put(
            "arch/shard_3/wal/00000000000000000007_00000000000000000202",
            &enc(&[t2]).unwrap(),
        )
        .unwrap();

    // Node B takes over shard 3. Without replay it would see 200 rows as of the snapshot; with
    // replay it is current — 210 rows, and id 11 carries the post-snapshot update.
    let scratch = tempfile::tempdir().unwrap();
    let conn =
        meshdb::s3::open_from_s3(Arc::clone(&client), "arch", ShardId(3), scratch.path()).unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM t", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        210,
        "the change-log transactions after the snapshot were not replayed"
    );
    assert_eq!(
        conn.query_row("SELECT v FROM t WHERE id = 205", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "new-205"
    );
    assert_eq!(
        conn.query_row("SELECT v FROM t WHERE id = 11", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "changed"
    );

    // And it still takes new writes locally over the replayed base.
    conn.execute("INSERT INTO t VALUES (9999, 'b-write')", [])
        .unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM t", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        211
    );
}

#[test]
fn a_failover_refuses_a_change_log_with_a_gap() {
    // Snapshot at lsn 5, then a WAL object that starts at lsn 8 — lsn 6 and 7 are missing. Serving
    // this would silently drop two transactions, so the overlay build must refuse.
    let client = Arc::new(S3Client::new(S3Config {
        endpoint: spawn_full_s3(),
        bucket: "meshdb".into(),
        region: "us-east-1".into(),
        access_key: "a".into(),
        secret_key: "s".into(),
    }));
    let txn = meshdb::replication::StreamTxn {
        lsn: 8,
        txn: meshdb::vfs::CommittedTxn {
            db_size_pages: 1,
            page_size: 4096,
            frames: vec![meshdb::vfs::Frame {
                page_no: 1,
                data: vec![0u8; 4096],
            }],
            generation: 0,
        },
    };
    client
        .put(
            "arch/shard_1/wal/00000000000000000004_00000000000000000008",
            &meshdb::s3::sink::encode_change_log(&[txn]).unwrap(),
        )
        .unwrap();

    let err = meshdb::s3::failover::build_overlay(&client, "arch", ShardId(1), 4, 5).unwrap_err();
    assert!(
        err.to_string().contains("gap"),
        "expected a gap refusal, got: {err}"
    );
}

// --- Phase B: runtime S3 archival endpoints (needs the http gateway too) ---

#[cfg(feature = "http")]
#[test]
fn s3_archival_is_configurable_at_runtime_over_http() {
    use meshdb::net::{HttpConfig, HttpGateway, NodeServices};
    use meshdb::shard::{ShardConfig, ShardManager};
    use meshdb::storage::exec::Statement;
    use std::time::Duration;

    // A capture-ready node with NO S3 target — exactly what `--s3-ready` gives.
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(
        ShardManager::open_with_sink(
            dir.path(),
            ShardConfig {
                shard_count: 2,
                capture: true,
                ..ShardConfig::floor()
            },
            None,
        )
        .unwrap(),
    );
    let gateway = Arc::new(
        HttpGateway::bind(
            Arc::clone(&manager),
            NodeServices::default(),
            HttpConfig {
                addr: "127.0.0.1:0".into(),
                workers: 2,
                insecure: true,
            },
        )
        .unwrap(),
    );
    let base = format!("http://{}", gateway.local_addr().unwrap());
    let g = Arc::clone(&gateway);
    std::thread::spawn(move || g.serve());
    std::thread::sleep(Duration::from_millis(50));

    let get = |p: &str| -> serde_json::Value {
        serde_json::from_str(
            &ureq::get(&format!("{base}{p}"))
                .call()
                .unwrap()
                .into_string()
                .unwrap(),
        )
        .unwrap()
    };
    let post = |p: &str, body: serde_json::Value| -> serde_json::Value {
        serde_json::from_str(
            &ureq::post(&format!("{base}{p}"))
                .send_string(&body.to_string())
                .unwrap()
                .into_string()
                .unwrap(),
        )
        .unwrap()
    };

    // Before configuring: capture-ready but no sink.
    let before = get("/v1/s3/status");
    assert_eq!(before["capture_ready"], true);
    assert_eq!(before["configured"], false);

    // Turn archival on, pointed at a mock S3.
    let endpoint = spawn_full_s3();
    let cfg = post(
        "/v1/s3/config",
        serde_json::json!({
            "bucket": "b", "endpoint": endpoint, "access_key": "a",
            "secret_key": "s", "prefix": "rt",
        }),
    );
    assert_eq!(cfg["configured"], true);

    // Writes now archive through the runtime-attached sink.
    manager
        .execute_all_shards(Statement::new(
            "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT",
        ))
        .unwrap();
    manager
        .execute_one(
            ShardId(0),
            Statement::new("INSERT INTO t VALUES (1),(2),(3)"),
        )
        .unwrap();

    // On-demand snapshot uploads every shard; status then shows per-shard progress.
    let snap = post("/v1/s3/snapshot", serde_json::json!({}));
    assert_eq!(snap["ok"], true, "snapshot errors: {}", snap["errors"]);
    assert!(snap["snapshotted"].as_u64().unwrap() >= 1);

    let status = get("/v1/s3/status");
    assert_eq!(status["configured"], true);
    assert_eq!(status["health"], true);
    let shards = status["shards"].as_array().unwrap();
    assert!(
        shards
            .iter()
            .any(|s| s["last_snapshot_lsn"].as_u64().unwrap() >= 1),
        "a snapshot should have been recorded for at least one shard"
    );

    // Detach turns it back off.
    let off = post("/v1/s3/config", serde_json::json!({ "enabled": false }));
    assert_eq!(off["configured"], false);
    assert_eq!(get("/v1/s3/status")["configured"], false);
}
