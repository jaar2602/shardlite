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

*(The alternative — axum/hyper on tokio — is more ergonomic but pulls tokio into the core's
process and grows the footprint. Rejected for the same reason openraft was.)*

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

## Phase 3 — Web console

A browser SPA (TypeScript, reusing the Phase 2 JS driver) served either as static files or by
the gateway. Screens, each backed by existing data:

| Screen | Data source | Purpose |
|---|---|---|
| **Cluster topology** | `GET /v1/cluster` | node/term/role, leader, placement map (which node leads which shard), step-down/election counters |
| **Node health** | `GET /v1/stats` per node | connections, auth failures, authz refusals, abandoned freezes, writer/reader stats |
| **Shards** | placement + `GET /v1/schema` | per-shard owner, schema version, size; flag version skew during a rolling DDL |
| **SQL runner** | `POST /v1/query` / `/execute` / `/tx` | run reads/writes with a shard + consistency selector; results table |
| **Users** | `/v1/users` | list, create (role picker), drop — live, no restart |
| **Replication / frames** | `GET /v1/frames/{shard}` | the frame inspector in the browser: transactions, commit markers, leftover frames, WAL growth |
| **Divergence check** | `content_hash` per shard (new endpoint) | compare a follower against its primary from the UI |

Console-specific concerns to decide at build time: how it discovers all nodes (seed list vs
one node reporting peers), whether it polls or the gateway offers a lightweight
server-sent-events stream for live counters, and read-only vs admin console modes gated by the
logged-in role.

**Security note:** the console is an admin surface. It must be served over TLS, behind auth,
and — because it can create users and run DDL — should default to the same trusted-network
posture as the rest of meshdb, with the open-mode warning applying doubly.

---

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

## Open questions for the reviewer

1. **Sync vs async HTTP** — this plan assumes sync (`tiny_http`) to protect the footprint. If
   throughput demands or a preference for axum's ergonomics outweigh that, the gateway can be
   async with a threadpool bridge, at a footprint cost.
2. **Where drivers and the console live** — in-repo (`drivers/`, `console/`) or separate
   repos. In-repo is simpler to keep in sync with the protocol; separate keeps the Rust
   crate's build lean.
3. **Node discovery for the console** — seed list, or teach the gateway to report peer
   addresses so the console learns the whole cluster from one node.
4. **Live updates** — polling (simplest) vs a server-sent-events counter stream (nicer,
   slightly more gateway code).
