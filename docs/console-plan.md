# shardlite console — Phase 3 plan (for scope confirmation)

> Status: **plan, not built.** The user asked to plan Phase 3, confirm scope, then build.
> This document is the thing to confirm. Open scoping questions are at the end.

## What it is

A **standalone** web app — its own backend and frontend, its own login — that manages many
shardlite clusters, the way a database client (DBeaver, TablePlus) manages many database
connections. Two jobs:

1. **Database management** — connect to a shardlite cluster, browse schema, run SQL, run atomic
   transactions, manage users.
2. **Cluster observability** — topology and shard placement, per-shard replication state,
   live stats, WAL frame inspection.

It is deliberately **not** served from the shardlite binary. That decision is already made and its
reasons hold: keeping the console out of the DB process avoids adding a browser-facing attack
surface and request load to a 150 MB / 0.33 CPU database, and a standalone backend means the
console owns its own login and has no CORS problem (the browser talks only to the console
backend, same origin).

## Why this is mostly composition, not new database work

The gateway already exposes **every endpoint the console needs** — verified against
`src/net/http_gateway.rs`:

| Console feature | Endpoint(s) it drives |
|---|---|
| SQL editor (reads, streaming) | `POST /v1/query`, `/v1/query_all` |
| SQL editor (writes) | `POST /v1/execute` |
| Transactions (BEGIN/COMMIT) | `POST /v1/tx` (atomic, durable) |
| Cluster-wide DDL | `POST /v1/execute_all` |
| Schema browser | `GET /v1/schema/{shard}` |
| Key → shard routing | `POST /v1/route` |
| Topology / placement | `GET /v1/cluster` |
| Live stats | `GET /v1/stats`, `GET /v1/info` |
| WAL frame inspector | `GET /v1/frames/{shard}` |
| User management | `GET/POST/DELETE /v1/users` |

So Phase 3 adds **no shardlite core changes**. The one thing the DB does not provide — a *history*
of stats for charting — the console backend supplies by sampling `/v1/stats` on a timer and
keeping a short in-memory ring buffer. The database stays stateless about metrics; the console
owns its own observability retention.

## Architecture

```
  browser ──(same origin, JSON + NDJSON)──► console backend (Rust) ──(HTTP/JSON /v1)──► shardlite cluster(s)
     React SPA                                 - console login/session               (one or many, saved as
     Tailwind (Carbon-mocked)                  - connection registry                  connection profiles)
                                               - proxy to /v1, streaming pass-through
                                               - stats sampler + ring buffer
                                               - serves the built SPA
```

Repo layout under `console/`:

```
console/
  server/            Rust backend — its own binary + crate, NOT in the shardlite workspace
    Cargo.toml
    src/
      main.rs        arg parsing, bind, serve
      auth.rs        console login + session cookies
      users.rs       console user store (multi-user: create/list/delete, roles, password hashing)
      registry.rs    connection profiles (CRUD, persisted, secrets encrypted at rest)
      proxy.rs       forward console API calls to a cluster's /v1, streaming pass-through
      metrics.rs     periodic /v1/stats sampler + in-memory ring buffer
      api.rs         the console's own JSON API surface the SPA calls
      assets.rs      serve the embedded built frontend
  web/               React frontend — Vite + React + TypeScript + Tailwind
    package.json
    src/
      lib/api.ts     typed client for the console backend
      components/    Carbon-mocked primitives (DataTable, Button, Tabs, SideNav, Toast, …)
      views/         Connections, SqlEditor, Schema, Cluster, Shards, Frames, Users
    index.html  vite.config.ts  tailwind.config.js
  README.md          how to build (web → static assets → embedded in server) and run
```

The backend reaches shardlite over the **HTTP/JSON edge**, not the native bincode client, so the
console can be released and upgraded independently of the exact shardlite build it points at.
(Native bincode is version-locked; a management tool must survive talking to a slightly-different
cluster version.) *(This is scoping question 2 — confirm.)*

## Backend (Rust)

Small, sync, standard-library-leaning, matching shardlite's discipline — but this is a separate
crate outside the shardlite workspace, so its dependency choices don't touch the DB's footprint.
Likely deps: a minimal HTTP server (`tiny_http`, already in the tree), `serde_json`, `ureq` for
the outbound `/v1` calls (all already used by the Rust driver), and one crate for at-rest
secret encryption. Responsibilities:

- **Console auth (multi-user).** A console user store — username + password hash + a console
  role (`admin` / `user`) — with login issuing a signed session cookie. An `admin` bootstraps
  from config/env on first run; admins create further console users. Console-user management and
  connection-registry writes are admin-gated. This is the console's own identity layer, separate
  from the per-connection shardlite credentials.
- **Connection registry.** CRUD of connection profiles: name, shardlite address, transport,
  and the shardlite credential (a shardlite username + secret). Persisted to a file. The stored
  credential is a real secret, so it is **encrypted at rest** with a console master key from
  env/config — the file alone must not grant cluster access. *(Scoping question 3.)*
- **Proxy + streaming.** The SPA never talks to shardlite directly; the backend forwards to the
  selected connection's `/v1`. Large query results **stream straight through** (NDJSON in from
  shardlite, NDJSON out to the browser) so the "handle 1 row to 1 million rows" robustness carries
  end to end — nothing is materialised in the console backend either. The UI grid paginates/caps
  what it renders while the stream stays bounded underneath.
- **Metrics sampler.** A timer thread polls each connected cluster's `/v1/stats` and keeps a
  bounded in-memory ring (say last N minutes) for sparklines/charts. Bounded and in-memory —
  the console does not become a time-series database.
- **Serve the SPA.** The built frontend is embedded into the backend binary so the console
  ships as **one self-contained executable** (matches shardlite's bundled-SQLite spirit).
  *(Scoping question 4 — embed vs serve-from-disk.)*

## Frontend (React + Tailwind, Carbon-mocked)

Vite + React + TypeScript. **IBM Carbon look mocked with Tailwind, not the Carbon React
library** (too heavy) — a small hand-built primitive set carrying Carbon's visual language: IBM
Plex type scale, Carbon's grid and spacing tokens, the g10/g100 gray palette, and Carbon-styled
DataTable, Button, Tabs, SideNav, TextInput, and Toast. Views:

**Management**
- **Connections** — list/add/edit/remove clusters; connect; shows reachability.
- **SQL editor** — pick a shard (or all-shards read); run arbitrary SQL; streaming results grid
  with pagination; writes show `rows_affected` / `last_insert_rowid`; multi-statement **atomic
  transactions** via `/v1/tx`.
- **Schema** — per-shard tables, columns, indexes, schema version (from `/v1/schema/{shard}`).
- **Users** — list/create/drop shardlite users with roles (admin-gated; `/v1/users`).

**Observability**
- **Cluster** — nodes, epoch/leader, per-shard primary placement (`/v1/cluster`).
- **Shards** — per-shard LSN, frame counts, replication state; drill into one shard.
- **Stats** — live counters (`wal_retries`, `contended_opens`, request rates) with sparklines
  from the backend ring buffer.
- **Frames** — the WAL frame report per shard (`/v1/frames/{shard}`), the inspector already
  built, rendered as a table with committed/leftover labelling.

## Explicitly deferred (mentioned as "later" by the user, not v1)

- **Data ingestion** (CSV / bulk import) — a later increment.
- **Per-connection console ACLs** (which console user may see which cluster) and an **audit log**
  — v1 has console roles but not per-connection scoping.
- **Long-term metrics retention / external time-series export.**
- **Alerting.**

Naming them here so v1 stays a tight, genuinely useful management + observability tool rather
than sprawling.

## Build order (once scope is confirmed)

1. `console/server` skeleton: bind, console login, session cookie, serve a placeholder page.
2. Connection registry (CRUD + encrypted-at-rest persistence) and the `/v1` proxy with
   **streaming pass-through** — proven with a 1M-row stream end to end, the same robustness bar.
3. Frontend scaffold: Vite + React + Tailwind, the Carbon-mocked primitive set, SideNav shell,
   the console login screen.
4. Management views: Connections, SQL editor (streaming grid), Schema, Users.
5. Observability views: Cluster, Shards, Stats (with the backend sampler), Frames.
6. Embed the built SPA into the backend binary; single-binary run; `console/README.md`; a smoke
   script that starts a shardlite gateway + the console and drives the API.

Steps 1–2 (backend proxy + streaming) are the load-bearing correctness work; everything after
is UI over endpoints that already exist and are already tested.

## Confirmed scope (settled 2026-07-21)

1. **Console login — multi-user from the start.** The console has its own user store: username +
   password hash + a console role. An `admin` console role manages console users and connection
   profiles; a regular console role uses connections. This is the console's own identity layer,
   entirely separate from the shardlite credentials it stores per connection.
2. **Backend→shardlite transport — HTTP/JSON edge.** The console reaches every cluster over `/v1`,
   so it is decoupled from the exact shardlite build and survives cluster version skew.
3. **Connection secrets — encrypted at rest.** Profiles are encrypted with a console master key
   from env/config; the profile file alone grants no cluster access. One encryption dependency
   in the console crate only (the DB is untouched).
4. **Frontend delivery — embedded in the binary.** The built SPA is compiled into the backend,
   so the console ships as one self-contained executable via a two-step build (web → server).

### What "multi-user" adds to the backend

Beyond the single-operator sketch above, the backend gains a **console user store**
(`users.rs`): create/list/delete console users (admin-gated), password hashing, and a console
role on the session. Connection-registry writes and console-user management require the `admin`
console role; everyone authenticated may use connections and read observability. This is the
console's own auth ladder — do not confuse it with shardlite's `Read/Write/Admin`, which still
governs what each stored shardlite credential can do on the cluster.
