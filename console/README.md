# meshdb console

A standalone web app for managing and observing meshdb clusters — the console's own login and
state, managing many clusters the way a database client manages many connections. It is
deliberately **not** part of the database binary: keeping it out of the 150 MB / 0.33 CPU
database avoids adding a browser-facing surface and request load there, and a standalone backend
means its own login and no CORS (the browser only ever talks to the console backend).

See `../docs/console-plan.md` for the original design and confirmed v1 scope. The upgrade path to
an operator-ready distributed database console is in `../docs/console-v2-plan.md`.

## Shape

```
console/
  server/   Rust backend: console login (multi-user), connection registry (secrets encrypted
            at rest), a streaming proxy to each cluster's HTTP /v1 edge, a stats sampler, and it
            serves the embedded SPA. Its own crate, outside the meshdb workspace.
  web/      React + TypeScript + Tailwind SPA (IBM Carbon look, mocked with Tailwind — not the
            heavy @carbon/react library). Built and embedded into the server binary.
```

The backend reaches clusters over the stable HTTP `/v1` edge (not the native bincode client), so
the console is decoupled from the exact meshdb build it points at. Every feature is composition
over endpoints meshdb already exposes. The database does not retain a *history* of stats for
charts, so the backend supplies a bounded collector and persists its recent window locally for
restart continuity.

Fleet pages and the active database workspace share one left navigation rail. Database pages are
grouped beneath the active connection, and the rail can collapse to an icon-only view; that choice
is retained in the browser.

## Build

The SPA is embedded into the server binary at compile time, so **build the frontend first**:

```sh
# 1. frontend -> console/web/dist
npm --prefix web ci
npm --prefix web run build

# 2. backend embeds web/dist
cargo build --manifest-path server/Cargo.toml --release
```

`./build.sh` does both. A fresh `cargo build` with no `web/dist` present will fail to compile —
that is the embed step telling you to build the frontend.

## Run

```sh
export MESHDB_CONSOLE_KEY="a-strong-master-passphrase"   # required: encrypts stored secrets
export MESHDB_CONSOLE_ADMIN=admin                        # optional: bootstrap the first admin
export MESHDB_CONSOLE_ADMIN_PASSWORD=change-me           #           (only used when no admin exists)

server/target/release/meshdb-console --listen 127.0.0.1:7100 --data ./console-data
```

Then open <http://127.0.0.1:7100>, sign in, add a connection (a cluster's `--http` address plus a
meshdb user/secret), and use it.

- `MESHDB_CONSOLE_KEY` is **required** and checked at startup — a console that could not decrypt
  its stored secrets fails loudly now, not when a connection is first opened. The connections file
  stores only authenticated ciphertext; new secrets use an Argon2id-derived key, while old records
  remain readable for migration.
- Rotate that key offline with the current key in `MESHDB_CONSOLE_KEY`, the replacement in
  `MESHDB_CONSOLE_NEW_KEY`, and `meshdb-console --data ./console-data --rotate-key`. The registry
  is rewritten only after every existing secret decrypts successfully; restart with the new key.
- `MESHDB_CONSOLE_SESSION_IDLE_SECS` (default `1800`) and
  `MESHDB_CONSOLE_SESSION_ABSOLUTE_SECS` (default `43200`) control session lifetime. Sessions are
  in-memory, so a restart signs everyone out.
- Set `MESHDB_CONSOLE_SECURE_COOKIE=true` when the browser reaches the console over HTTPS. Put the
  console behind TLS on any untrusted network.
- `--workers` defaults to 8. `--query-streams` defaults to half the worker count and is always
  clamped below it, reserving capacity for login, health, cancellation, and other control requests.
- New connection profiles require HTTPS by default. Plain HTTP must be explicitly acknowledged in
  the connection editor and is intended only for local or otherwise trusted development networks.
  HTTPS profiles can add a private CA PEM bundle without disabling normal public roots, hostname
  checks, or certificate verification.

Console roles are enforced before a stored cluster credential is used:

| Role | Access |
|---|---|
| Viewer | Cluster/schema/metrics views and read-only queries |
| Developer | Viewer access plus execute and transaction actions |
| Operator | Viewer access plus operational diagnostics such as frame reports |
| Admin | Connection, console-user, meshdb-user, and audit administration |

The effective permission is still the intersection of the console role and the credential accepted
by meshdb. Keep meshdb credentials least-privileged as a second enforcement layer.

Phase 0 also adds CSRF protection, login throttling, a 1 MiB request limit, bounded query streams,
outbound timeouts with redirects disabled, strict route/method permissions, security headers,
`/healthz` and `/readyz`, and an append-only `console-data/audit.jsonl` event log. Audit events record
actors, targets, action classes, and outcomes, but deliberately exclude SQL text, parameters,
passwords, and secrets.

## Distributed observability

Each connection profile accepts one database endpoint plus up to 31 failover endpoints. Legacy
`url`-only records migrate automatically. The collector polls configured endpoints through a fixed
worker pool with a four-request per-cluster cap, bounded queue and response size, five-second poll
timeout, jitter, exponential failure backoff, and a 15-second stale-evidence threshold. A successful
endpoint is preferred for interactive proxy traffic without changing the saved profile.

MeshDB exposes versioned read-only contracts at `/v1/meta`, `/v1/health`, `/v1/topology`, and
`/v1/shards`; `/v1/stats` includes checkpoint counters. The collector remains compatible with older
nodes by deriving conservative health and shard views from `/v1/info` and `/v1/cluster` and labels
that evidence as derived.

The SPA presents one logical database across Fleet, Overview, Schema, SQL, and Topology. Exact
placement and WAL evidence live only in the operator-only Storage internals view. It detects
unreachable or stale observers, leader/term disagreement, version skew, unavailable or conflicting
primaries, epoch-incomparable replicas, replica lag, election step-downs, handover failures, and
checkpoint stalls/failures. Conflicting observations are never flattened into a false healthy state.

Recent metric and observation evidence is stored in `console-data/observability.json` with atomic
0600 state-file writes. Full per-node shard payloads are recollected after restart instead of being
persisted, keeping the local state file bounded.

## Database workbench

The SQL workbench uses one CodeMirror editor for every statement type. **Run** can target the
statement at the cursor, the current selection, or the complete document. The console maps reads,
writes, atomic write groups, and schema changes onto MeshDB's existing APIs; normal reads span the
database and writes ask only for an application data key. Consistency, explain plans, and streamed
export stay behind **Options**; the console never asks a normal user to select physical placement.

The editor and results use a persistent resizable split and scroll independently. Saved queries,
the auto-restored draft, browser-local SQL history, and an expandable table/column reference live
in the searchable left library. Table and column names can be inserted into SQL or copied without
exposing physical shard placement. Parameter values and data keys are never stored in history.

Schema rollout is rolling rather than globally atomic. The workbench requires a current preflight
and explicit acknowledgement, reports a database-wide outcome, and labels any mixed result as
partial. The logical schema catalog reads columns, indexes, foreign keys, triggers, and stored
definitions while checking consistency internally.

Administrators can verify a newly deployed node from Connections before adding it as a failover
endpoint. Verification is read-only and checks reachability, compatibility, reported membership,
health, and distribution stability; joining still happens through the existing deployment process.

## Controlled operations

Schema rollout submission is coordinated entirely by the console over MeshDB's existing
`/v1/info`, `/v1/schema/{shard}`, and `/v1/execute_all` APIs. Approved operations are written
atomically to `console-data/operations.json`, then processed by one background worker. The worker
revalidates every schema version before execution, preserves each shard outcome, and records
submit/cancel/completion audit events without SQL text.

Idempotency prevents a browser retry from duplicating work. Queued or preflight-stage work can be
cancelled; after the request reaches MeshDB it cannot be cancelled safely. A console restart resumes
queued work but marks previously running work `interrupted` rather than replaying potentially
applied DDL. The Operations tab exposes these states and the required manual roll-forward evidence.
This coordinator never reads or changes MeshDB database/WAL files and does not offer backup,
restore, checkpoint, placement, rebalance, failover, or individual-shard retry controls.

## Frontend development

`npm --prefix web run dev` serves the SPA with hot reload and proxies `/api` to a console backend
on `127.0.0.1:7100`, so run the backend alongside it.

For a guided set of SQL covering every workbench and controlled-operation path, see
[`examples/workbench-test.sql`](examples/workbench-test.sql).

## Verify

`../scripts/console_smoke.sh` builds everything, starts a meshdb gateway and the console, and
drives the whole API end to end. Its 51 checks cover login, CSRF, role boundaries,
encrypted-at-rest secrets, connection testing, a 60,000-row streaming query, bounded CSV/NDJSON
downloads, key routing, at-least-LSN, typed blobs, fan-out reads, transactions, all-shard rollout,
durable operation preflight/submission/idempotency/completion/persistence, meshdb users, audit
events, health, and the embedded SPA.
