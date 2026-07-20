# meshdb drivers

Clients for meshdb in Python, JavaScript, Go, and Rust.

## Two transports, and which is available where

meshdb speaks two protocols:

- **Native bincode over TCP** — the fastest path, and what a Rust program that is itself a
  cluster member uses. It is `bincode`-encoded, which is **Rust-specific and version-locked**:
  reimplementing it in another language would be brittle (a bincode bump or a message-enum
  change silently breaks every non-Rust client), so it is deliberately Rust-only.
- **HTTP/JSON** (`meshdb serve --http ADDR`) — the stable cross-language edge. Any language
  with an HTTP client works, against any meshdb version that serves `/v1`.

| Language | HTTP/JSON | Native TCP (bincode) |
|---|---|---|
| **Rust** | yes (default) | yes — `--features native` (re-exports `meshdb::net::Client`) |
| Python / JS / Go | yes | no — see note below |

**Why not native TCP for Python/JS/Go?** The stable contract across languages is HTTP/JSON.
Hand-porting bincode framing to each language would reintroduce exactly the cross-version
fragility the HTTP edge was built to remove. If you need a persistent, lower-overhead socket
for those languages, HTTP/1.1 keep-alive already reuses the connection; a dedicated
length-prefixed-JSON-over-TCP server protocol is a possible future addition (stable and
cross-language, unlike bincode) — not built yet.

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

Pass `user` and `secret` to the constructor. The driver sends
`Authorization: Bearer base64(user:secret)` — the programmatic scheme, which (unlike Basic)
does not trigger a browser login prompt. **The credential is only as safe as the transport:**
over a plaintext gateway it is exposed. Run the gateway behind TLS on any untrusted network;
the gateway refuses to start with auth enabled and no TLS unless `--http-insecure` is set.

## Consistency

Reads take a consistency level: `"linearizable"` (default — the shard leader, every
acknowledged write), `"stale"` (any replica, may lag), or `{"at_least_lsn": N}` (a replica
that has applied at least N).

## Per language

- **Python** (`python/meshdb.py`) — stdlib only. `query()` is a generator.
  `python3 example.py`
- **JavaScript** (`javascript/meshdb.mjs`) — Node 18+, built-in `fetch`. `query()` is an async
  generator. `node example.mjs`
- **Go** (`go/meshdb.go`) — stdlib `net/http`. `Query` returns streaming `*Rows` with
  `Next()`/`Row()`. `go run ./example`
- **Rust** (`rust/`) — HTTP by default (`ureq` + `serde_json`), streaming iterator. Native
  bincode-over-TCP with `--features native`, which re-exports `meshdb::net::Client` under
  `meshdb_driver::native` — the fastest path, and full-featured (transactions, consistency,
  cluster verbs).

Verified by `scripts/driver_test.sh`, which runs the Python, JS, and Rust drivers against a
live gateway (Go is exercised where a toolchain is present).
