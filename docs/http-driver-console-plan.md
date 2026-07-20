# Design: HTTP gateway, cross-language drivers, and a web console

> Status: **plan only.** No code, no dependencies added. This document is the deliverable of
> a planning step, to be reviewed before any implementation.

## Why the order is HTTP first, not last

The three initiatives — a driver client, an HTTP client, and a web console — are not
independent. They share one dependency:

- The only wire protocol today is **length-prefixed bincode over TCP** (`src/net/protocol.rs`).
  bincode is a Rust-specific encoding and **not a stable cross-language format**; a Python,
  JS, or Go client cannot reasonably reimplement it.
- A **web console runs in a browser**, and browsers speak HTTP/JSON, not bincode.

So a single HTTP + JSON gateway is the enabler for both other pieces. Build it once and every
language with an HTTP client becomes a meshdb client, and the console has an API to consume.

```
              ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
              │ web console  │   │ py / js / go │   │ curl / other │
              │  (browser)   │   │   drivers    │   │              │
              └──────┬───────┘   └──────┬───────┘   └──────┬───────┘
                     │ HTTP/JSON        │ HTTP/JSON        │ HTTP/JSON
                     └──────────────────┴──────────┬───────┘
                                                    ▼
                                        ┌───────────────────────┐
                                        │  HTTP gateway (new)    │  ← Phase 1
                                        │  JSON ⇄ Request/Resp   │
                                        └──────────┬────────────┘
                                                   │ in-process call
                                                   ▼
                                    existing sync core: auth, TLS,
                                    routing/forwarding, shards, cluster
```

The gateway is a **translation layer**, not a second server. It reuses the existing
`handle`/`session_step` request path, the auth doorman, TLS transport, and shard routing
unchanged. Nothing about the storage or cluster core is touched.

---

## Architectural constraint that shapes everything

The core is **synchronous, thread-per-connection, deliberately tokio-free** — the decision
that keeps a node inside ~150 MB and that made hand-rolled Raft preferable to openraft.

The gateway must not reverse that. The recommended stack is a **synchronous HTTP/1.1 server**
(`tiny_http`, thread-per-connection — the same model the existing TCP server uses) plus
`serde_json`, behind an `http` cargo feature so a deployment that does not want it compiles
none of it. This preserves the footprint and keeps the gateway an optional edge, exactly as
TLS is today.

**Decision (locked): sync now, async as a future drop-in feature.** The gateway's only contact
with the core is one function, `handle(req, shards, services) -> Response`. That boundary
insulates the core from the transport's concurrency model, so:

- Today: `--features http` = a synchronous `tiny_http` gateway. Core stays sync, small,
  loom-checkable, tokio-free at ~150 MB.
- Later, *if and when* connections must scale to thousands: `--features http-async` = a tokio
  gateway bridging to the same `handle()` via `spawn_blocking`. An **additive feature swap on
  the gateway module only — not a rewrite**, and only that feature carries tokio.

The trigger is measurable: `max_connections` is 256 today and every request blocks on an
fsync, so async buys nothing below that. The signal to build the async gateway is a real need
for thousands of concurrent HTTP clients. Until then, sync thread-per-connection is the
better fit on merit, not a compromise. The sync gateway will be built with its HTTP server
behind a thin internal boundary precisely so the async variant is a drop-in later.

---

## Phase 1 — HTTP + JSON gateway

### Endpoint map (each maps to an existing `Request` variant)

| Method & path | Maps to `Request` | Role required | Notes |
|---|---|---|---|
| `POST /v1/query` | `Query { shard, statement, consistency }` | Read | body: `{shard, sql, params, consistency}` |
| `POST /v1/query_all` | `QueryAll { statement }` | Read | fan-out read, merged |
| `POST /v1/execute` | `Execute { shard, statements }` | Write | single-shard write |
| `POST /v1/execute_all` | `ExecuteAll { statement }` | Admin | cluster-wide DDL |
| `POST /v1/tx` | `Transaction { shard, statements }` | Write | one atomic batch (the buffered BEGIN/COMMIT path, in one call) |
| `POST /v1/route` | `Route { key }` | Read | key → shard |
| `GET  /v1/info` | `Info` | Read | shard count, epoch, wal_retries |
| `GET  /v1/schema/{shard}` | `SchemaApply`/version read | Read | schema version per shard |
| `GET  /v1/users` | `ListUsers` | Admin | names + roles, never keys |
| `POST /v1/users` | `CreateUser { name, key, role }` | Admin | client derives key from secret; **key on the wire → require TLS** |
| `DELETE /v1/users/{name}` | `DropUser { name }` | Admin | |
| `GET  /v1/stats` | `ServerStats` (new accessor exposure) | Read | connection + auth counters |
| `GET  /v1/cluster` | `ClusterStats` + `placement()` | Read | topology, leader, placement map |
| `GET  /v1/frames/{shard}` | `vfs::inspect_wal` report | Admin | the frame inspector, as JSON |

The interactive multi-round-trip `BEGIN`…`COMMIT` is intentionally **not** exposed over HTTP:
HTTP is request-per-call and stateless, and a server-buffered transaction is bound to a TCP
connection. `POST /v1/tx` gives the same atomic-durable outcome in a single stateless call,
which is the right shape for HTTP.

### Request/response JSON shapes (illustrative, mapping to real types)

```jsonc
// POST /v1/query
{ "shard": 0, "sql": "SELECT * FROM t WHERE id = ?", "params": [7],
  "consistency": "linearizable" }        // "stale" | {"at_least_lsn": 42} | "linearizable"
// -> 200
{ "columns": ["id","v"], "rows": [[7,"a"]] }

// POST /v1/tx  (atomic, durable at return)
{ "shard": 0, "statements": [
    {"sql":"INSERT INTO t VALUES (?,?)","params":[1,"a"]},
    {"sql":"INSERT INTO t VALUES (?,?)","params":[2,"b"]} ] }
// -> 200 { "rows_affected": 2, "last_insert_rowid": 2 }   (quorum-confirmed)

// GET /v1/cluster
{ "node": 1, "term": 4, "role": "leader", "leader": 1,
  "placement": { "term": 4, "assignments": { "0": 1, "1": 2 } },
  "led_shards": [0],
  "stats": { "elections_started": 1, "became_leader": 1, ... } }
```

### Status-code mapping

The existing `Response` already distinguishes a rejection from a fault; the gateway maps that
faithfully rather than flattening everything to 500:

| `Response` | HTTP status |
|---|---|
| success (`Rows`/`Changed`/`Ok`/…) | 200 |
| `Rejected` (bad SQL, constraint) | 400 |
| `Error { retryable: true }` (WriterBusy, TooManyConnections) | 503 |
| `Error { retryable: false }` (auth, not-leader, unsupported) | 4xx per cause (401/403/409/422) |
| `TooStale` | 409 (retry against leader) |
| authz refusal | 403 |
| auth failure / missing | 401 |

### Auth mapping — the one security-critical decision

- **HTTP Basic or a token header carries the credential.** The gateway performs the same
  challenge-response against `AuthConfig` internally — the client's secret is turned into the
  keyed proof server-side, so the wire still never carries a reusable password *if TLS is on*.
- **Without TLS, HTTP Basic sends the secret in clear.** The gateway must therefore **refuse
  to start in HTTP mode without TLS unless an explicit `--insecure` flag is given**, and warn
  loudly — the same fail-loud posture the TCP server takes for open auth.
- The role ladder (`Read < Write < Admin`, `Cluster` off-ladder) applies unchanged; the
  `required()` map already decides per request, and the gateway routes each endpoint to its
  `Request`, so authorization is inherited, not re-implemented.
- `Cluster`-role endpoints (Subscribe, Snapshot, Vote, Beat) are **not exposed over HTTP at
  all** — they are node-to-node verbs and have no business on a public gateway.

### What Phase 1 needs from the core (small, additive)

1. Public accessors returning `ServerStats` / `ClusterStats` / placement as plain data
   (mostly exist; confirm all are `pub` and `Serialize`).
2. A `serde` derive audit on the handful of types that cross into JSON (Statement, Value,
   Role, ReadConsistency already derive it).
3. No change to `handle`, routing, auth, or storage.

### Testing posture (to match the rest of the project)

- Integration tests over real HTTP: query/execute/tx round trips, the status-code mapping,
  auth required + role refusals, TLS-required-in-http-mode refusal, and `/v1/tx` durability.
- Revert-verify the security guards (drop the TLS requirement → the refusal test fails).
- CLI smoke assertions for `meshdb serve --http :8080`.

---

## Phase 2 — Cross-language drivers

With JSON-over-HTTP, a driver is a **thin idiomatic wrapper**, not a protocol implementation.
Each language gets:

- `connect(url, user, secret, tls_ca?)`
- `query(shard, sql, params, consistency?)`, `query_all(sql)`
- `execute(shard, sql, params)`, `tx(shard, [statements])`
- `route(key)`, `info()`, `cluster()`
- admin: `users()`, `create_user()`, `drop_user()`

The Rust `net::Client` is the reference semantics. Priority order suggestion: **Python**
(data/ops audience), then **JavaScript/TypeScript** (shared with the console), then **Go**.

Each driver is small enough that its whole surface is a day or two plus tests against a live
gateway. They live outside the Rust crate (a `drivers/` directory or separate repos) so they
do not entangle the core's build.

**Deliberately not offered:** a native bincode driver per language. The wire format is not
stable across bincode/serde versions and would be a maintenance trap; HTTP/JSON is the stable
contract.

---

## Phase 3 — Web console: a standalone management tool

**Decision (locked): the console is its own process — backend + frontend — not served by the
database.** Serving it from the DB binary was rejected for two real reasons: it would compete
for the 256-connection cap meant for data clients, and it would put a public web entry point
on the process that holds the data (a DDoS/attack-surface concern). A standalone console also
removes CORS entirely — the browser talks only to the console's own backend, same origin.

The console is best understood not as a dashboard for one cluster but as a **database client
for many meshdb connections** — think DBeaver / pgAdmin / TablePlus, for meshdb:

```
browser ──HTTP (same origin)──► meshdb-console (standalone process)
                                  • serves the frontend (its own backend)
                                  • its OWN login (console username/password)
                                  • holds meshdb credentials server-side, never in the browser
                                  • manages MANY saved meshdb connections
                                  • authenticates + rate-limits the operator before touching a cluster
                                        │ HTTP/JSON (Phase 1 gateway) or native driver
                                        ▼
                                  one or more meshdb clusters (data processes, untouched)
```

Why this shape wins:
- **Isolation** — flooding the console never touches a DB process directly; separate failure
  and attack domains. (The console must still auth + rate-limit so it cannot become a DB load
  amplifier — easy in a dedicated tier, awkward bolted onto the DB.)
- **No CORS** — browser → console backend is same-origin; console backend → cluster is
  server-to-server.
- **Credentials stay server-side** in the console, not shipped to a browser tab.
- **Multi-connection** — one console manages many meshdb clusters/nodes, with its own account
  system separate from any cluster's users.
- **Concurrency fully decoupled** — a separate process can be sync or async on its own terms
  with zero bearing on meshdb's core; a UI backend facing bursty browsers is exactly where
  async is least controversial.

### Console features

v1 (management):

| Screen | Data source | Purpose |
|---|---|---|
| **Connections** | console's own store | saved meshdb clusters; console login gates access |
| **Cluster topology** | `GET /v1/cluster` | node/term/role, leader, placement map |
| **Node health** | `GET /v1/stats` per node | connection + auth + writer/reader counters |
| **Shards** | placement + `GET /v1/schema` | per-shard owner, schema version; version-skew flag |
| **SQL runner** | `POST /v1/query` `/execute` `/tx` | shard + consistency selector; results grid |
| **Users** | `/v1/users` | list / create (role picker) / drop — live, no restart |
| **Replication / frames** | `GET /v1/frames/{shard}` | frame inspector in the browser |

Later (the console is where new operator features accrue, off the data path):
- **Data ingestion** — CSV/JSON bulk import, routed to shards via `/v1/tx`.
- **Monitoring** — time-series of the counters, alerts on step-down churn / auth failures /
  WAL growth / divergence.
- Query history, saved queries, schema browser, export.

### Console stack

Its own small backend (language open — could reuse the Phase 2 JS/TS driver on Node, or a
Rust backend using `net::Client`) plus a frontend SPA. Kept in a separate directory or repo
so it never entangles the meshdb crate's build. The console's account system (console
login) is distinct from meshdb's user roles: logging into the console authorizes *using the
console*; the meshdb credentials it stores authorize *acting on a cluster*.

## Effort and risk, honestly

| Phase | Effort | Risk | Footprint cost |
|---|---|---|---|
| 1. HTTP gateway (sync, feature-gated) | ~3–5 days | Low–medium (new edge, but reuses the tested core) | `tiny_http` + `serde_json`, only under `--features http` |
| 2. Drivers (per language) | ~1–2 days each | Low (thin HTTP wrappers) | none (outside the crate) |
| 3. Web console | ~1–2 weeks | Medium (frontend surface area; mostly its own concern) | none (static assets) |

The whole plan preserves the two decisions that define meshdb: **tokio-free core** and
**footprint under the floor profile**. HTTP, drivers, and the console are all *edges* on top
of an unchanged core — the same way TLS and auth were added without disturbing storage or
replication.

## Decisions (resolved)

1. **Sync vs async HTTP** — **sync now** (`tiny_http`), async as a future drop-in feature
   (`http-async`) behind the `handle()` boundary, triggered by a real need for thousands of
   concurrent connections. Not before.
2. **Where drivers and the console live** — **outside the meshdb crate** (separate directory
   or repos), so the core build stays lean and the console's concurrency model is fully
   decoupled.
3. **Console hosting** — **standalone process**, its own backend + frontend, its own login,
   managing many meshdb connections. Not served by the DB binary.
4. **Live updates** — start with polling; a server-sent-events counter stream is a later
   nicety, and lives in the console tier (or the gateway) where it belongs, not in the core.
