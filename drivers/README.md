# meshdb drivers

Clients for meshdb in Python, JavaScript, Go, and Rust.

## Two transports, and which is available where

meshdb speaks three protocols, two of them cross-language:

- **HTTP/JSON** (`meshdb serve --http ADDR`) — the stable, stateless cross-language edge. Any
  language with an HTTP client works, against any meshdb version that serves `/v1`. Best for
  request/response and for going through proxies.
- **JSON over TCP** (`meshdb serve --json-tcp ADDR`) — a persistent-connection edge with lower
  per-request overhead than HTTP: one socket, authenticated once, many requests. Frames are
  `[4-byte big-endian length][JSON]`; the payloads are the same JSON shapes as HTTP, so it is
  just as stable and cross-language. Use it for long-lived clients issuing many small calls.
- **Native bincode over TCP** — the fastest path, and what a Rust program that is itself a
  cluster member uses. It is `bincode`-encoded, which is **Rust-specific and version-locked**:
  reimplementing it in another language would be brittle (a bincode bump or a message-enum
  change silently breaks every non-Rust client), so it is deliberately Rust-only.

| Language | HTTP/JSON | JSON/TCP | Native TCP (bincode) |
|---|---|---|---|
| **Rust** | yes (default) | — | yes — `--features native` (re-exports `meshdb::net::Client`) |
| Python / JS / Go | yes | yes (`TcpClient`) | no — bincode is Rust-only, see above |

**Why JSON/TCP and not native bincode for Python/JS/Go?** The stable contract across languages
is JSON. Hand-porting bincode framing to each language would reintroduce exactly the
cross-version fragility the JSON edges were built to remove — a bincode bump or a message-enum
change silently breaks every non-Rust client. JSON/TCP gives those languages the persistent,
lower-overhead socket without that fragility: the frame is a trivial length prefix and the body
is the same JSON the HTTP edge speaks.

Each driver is **dependency-free** (standard-library HTTP; the Rust crate uses `ureq`), and
each **streams reads**: `query` yields one row at a time, so a million-row result costs the
driver almost nothing — matching the gateway, which never materialises a result either.

## Common shape

| Operation | Meaning |
|---|---|
| `query(sql, shard, params, consistency)` | **streaming** read → rows one at a time |
| `query_all(sql)` | fan-out read across all shards, merged |
| `execute(sql, shard, params)` | single-shard write → `{rows_affected, last_insert_rowid}` |
| `tx(statements, shard)` | atomic, durable transaction |
| `execute_all(sql)` | rolling cluster-wide DDL → per-shard results |
| `route(key)` | which shard a key maps to |
| `info` / `cluster` / `stats` / `schema(shard)` / `frames(shard)` | introspection |
| `list_users` / `create_user` / `drop_user` | admin (requires the Admin role) |

## Auth

Pass `user` and `secret` when constructing a client. Over **HTTP** the driver sends
`Authorization: Bearer base64(user:secret)` — the programmatic scheme, which (unlike Basic)
does not trigger a browser login prompt. Over **JSON/TCP** the driver sends the secret once, as
the first frame (`{"op":"auth","name","secret"}`), before any other request; the connection
stays authenticated for its lifetime. **In both cases the credential is only as safe as the
transport:** over a plaintext gateway it is exposed. Run the gateway behind TLS (or an
`stunnel`/mesh tunnel for JSON/TCP) on any untrusted network; the gateway refuses to start with
auth enabled and no TLS unless the matching `--http-insecure` / `--json-tcp-insecure` flag is
set.

## Consistency

Reads take a consistency level: `"linearizable"` (default — the shard leader, every
acknowledged write), `"stale"` (any replica, may lag), or `{"at_least_lsn": N}` (a replica
that has applied at least N).

## Per language

- **Python** (`python/meshdb.py`) — stdlib only. HTTP `Client` (`query()` is a generator) and
  persistent `TcpClient` (`connect(host, port, ...)`, streaming `query()`). `python3 example.py`
- **JavaScript** (`javascript/meshdb.mjs`) — Node 18+. HTTP `Client` (built-in `fetch`,
  async-generator `query()`) and `TcpClient` (built-in `net`, `await TcpClient.connect(...)`,
  async-generator `query()`). `node example.mjs`
- **Go** (`go/meshdb.go`) — stdlib only. HTTP `Client` (`Query` → streaming `*Rows`) and
  `TCPClient` (`DialTCP(addr, user, secret)`, `Query` → streaming `*TCPRows`). `go run ./example`
- **Rust** (`rust/`) — HTTP by default (`ureq` + `serde_json`), streaming iterator. Native
  bincode-over-TCP with `--features native`, which re-exports `meshdb::net::Client` under
  `meshdb_driver::native` — the fastest path, and full-featured (transactions, consistency,
  cluster verbs). (Rust reaches TCP through the native client, not JSON/TCP.)

Verified by `scripts/driver_test.sh`, which runs each present driver against a live gateway over
**both** HTTP and JSON/TCP (Go is exercised where a toolchain is present).
