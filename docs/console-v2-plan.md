# shardlite console v2 — distributed database management plan

> Status: Phase 0, Phase 1, Phase 2, and the console-only Phase 3 controlled-operation slice are
> implemented as of 2026-07-21. Remaining production gates are recorded under each phase instead
> of being implied complete.

## Outcome

The console should let an operator answer, from one place:

1. Is the fleet healthy, and what changed?
2. Which node owns each shard, and are its replicas caught up?
3. Is schema consistent across shards?
4. Can I safely inspect or change data without guessing which node to contact?
5. What administrative action happened, who requested it, and what was its result?

It remains a standalone application. The browser talks only to the console backend, credentials
stay server-side, and the database process does not serve UI assets.

## Current baseline

The current project already has a sound initial shape:

- React, TypeScript, Vite, and Tailwind frontend embedded in one Rust console binary.
- Separate console login with `admin` and `user` roles.
- Saved multi-cluster connections with shardlite secrets encrypted at rest.
- Streaming proxy to shardlite's HTTP `/v1` API.
- Initial screens for connections, SQL, schema, cluster placement, WAL frames, stats, and users.
- A bounded in-memory metrics sampler and an end-to-end smoke script.

The next release should build on this rather than replace it indiscriminately. However, the
current implementation has several gaps that must be fixed before adding operational controls.

### Important gaps found in the current code

- A connection is a single URL, not a cluster with seed nodes, discovered members, failover, and
  per-node health.
- `/v1/cluster` describes only the contacted node's view. It does not expose enough information
  for replica lag, replication health, node reachability, version skew, or shard availability.
- The Cluster screen interprets the current placement object incorrectly: it treats `term` and
  `assignments` as if both were shard rows.
- The SQL editor decides read versus write from the first few characters of SQL. A `WITH` write,
  comments, and other valid SQLite forms make this unsafe and inaccurate.
- Query results are collected into browser memory and up to 5,000 DOM rows are rendered. Cancelling
  at the cap also means the complete result is not actually downloaded.
- Transactions, all-shard query/DDL, and key-to-shard routing exist in the client but have no
  complete UI workflow.
- Any authenticated console user can exercise the stored shardlite credential through the proxy.
  If that credential is an admin, a regular console user can manage shardlite users and inspect admin
  endpoints. Console roles therefore do not yet provide meaningful least privilege.
- Connection editing/testing, audit history, session expiry, login throttling, proxy timeouts,
  request-size limits, and a production TLS mode are missing.
- Stats are rendered as generic numeric sparklines. Counters are not converted to rates, gauges
  are not distinguished from counters, collection failures are invisible, and history disappears
  on restart.
- The synchronous fixed worker pool is also used by long query streams. This is acceptable for the
  prototype, but fleet polling and multiple simultaneous streams will exhaust it predictably.
- Most frontend-to-backend contracts are `Record<string, unknown>`, so API drift becomes a runtime
  UI failure rather than a compile-time or contract-test failure.

## Product principles

1. **Safe by default.** Open in read-only mode. Label writes and cluster operations clearly.
   Require explicit confirmation for destructive actions. Never infer authorization by parsing SQL.
2. **Distributed state first.** The primary objects are cluster, node, shard, replica, and operation;
   raw endpoint JSON is only a diagnostic fallback.
3. **Truth with provenance.** Every status includes the reporting node and sample time. Conflicting
   node views are displayed as disagreement, not silently merged.
4. **Capability-driven compatibility.** Older nodes remain usable. The UI hides or disables features
   not advertised by the connected shardlite version.
5. **Bounded resource use.** Polling, result rendering, exports, histories, and logs all have explicit
   concurrency and retention limits.
6. **No hidden control plane.** The console may orchestrate documented shardlite operations, but it must
   not mutate database files or invent failover state outside shardlite's own protocol.

## Target information architecture

### Fleet level

- **Overview** — clusters by health, unavailable nodes, unavailable shards, replica lag, schema
  drift, recent elections, and failed operations.
- **Connections** — add, edit, test, disable, label, and remove a cluster; configure multiple seed
  endpoints, TLS, and credentials.
- **Activity** — durable audit log and long-running operation history.
- **Administration** — console users, roles, connection access, session policy, and retention.

### Cluster level

- **Overview** — health summary, current leader/term, version distribution, capacity summary,
  alert-like findings, and recent events.
- **Topology** — all nodes, reachability, role, advertised addresses, last successful sample, and
  each node's view of leadership and placement.
- **Shards** — virtualized inventory with primary, replicas, epoch/LSN, lag, schema version, WAL
  bytes, checkpoint state, and availability. Drill-down shows replica history and WAL details.
- **SQL workbench** — tabs, shard/key routing, consistency, typed parameters, transaction mode,
  explain plan, bounded result grid, cancellation, history, saved statements, and streaming export.
- **Schema** — tables, columns, indexes, triggers, definitions, shard coverage, and schema drift.
- **Metrics** — curated rates and gauges first; raw counters second; node/shard filters and time range.
- **Operations** — schema rollout, backup/snapshot, restore validation, checkpoint, and placement
  operations only as shardlite gains explicit APIs for them.
- **Access** — shardlite users and roles, with secret rotation and clear effective-permission warnings.

## Topology view design direction

Use a dense, dark infrastructure view similar to the supplied Alluvium Console reference: factual
membership and configuration on the left, with an interactive 2.5D infrastructure map and selected
node details on the right. Adapt its visual grammar to shardlite's actual model rather than introducing
OLTP/OLAP or object-storage concepts that shardlite does not have.

> Initial implementation: the responsive layout, membership table, interactive map, selected-node
> details, five-second polling, version/forwarding metadata, configured members, voter count, and
> leader-observed peer status are built. Replica counts, per-remote-node workload, lag, WAL size, and
> schema drift remain explicitly unreported until the richer shard telemetry endpoint is available.

### Desktop layout

```text
+-----------------------------------+-------------------------------------------+
| Ownership & membership            | Infrastructure map                        |
| node count · clustered · term     |                                           |
|                                   |       [node-2]      [node-0 / leader]      |
| observer / leader / term          |        replica       primary + replica    |
| shard count / forwarding          |             [node-1]                      |
|                                   |                                           |
| Members                           |                       Selected node drawer  |
| node · endpoint · status · seen   |                       health, role, term,   |
|                                   |                       shards, lag, WAL      |
| Effective configuration           |                                           |
| version · mode · consistency      | legend · observed at · refresh state       |
+-----------------------------------+-------------------------------------------+
```

The left column should contain:

- **Ownership and membership:** node count, current term, leader, reporting/selected node,
  placement version, forwarding state, and shard count.
- **Consensus summary:** role, voters, leader stability, and whether nodes disagree about the
  current leader or term.
- **Member table:** node ID, advertised HTTP address, liveness, role, version, last successful
  observation, and a `this node` marker for the reporting node.
- **Effective configuration:** shardlite version, standalone/clustered mode, replication/quorum policy,
  shard count, and relevant durability settings. Secrets are represented only as configured/not
  configured—never displayed.

The right column should contain:

- An isometric cluster plane with one selectable stack per node.
- A crown or compact badge for the leader and a separate marker for the reporting node.
- Green/yellow/red/gray status treatment for healthy, suspected, down, and unknown. Color must be
  reinforced with text/icon/shape and not be the only signal.
- Stack layers that encode **primary shards** and **replica shards**. Layer height represents a
  count or bucket, not one DOM/SVG object per shard; this keeps large clusters readable.
- Optional external backup storage as a separate tile only when a real backup target is configured.
  Local SQLite files remain attached to their node rather than being drawn as shared storage.
- A selected-node detail card showing status, role, term, version, endpoints, primary/replica shard
  counts, worst replica lag, schema drift count, WAL bytes, checkpoint condition, and last sample.
- A legend, observation timestamp, refresh state, and a short statement naming which node(s)
  supplied the displayed view.

### Interaction and scale behavior

- Clicking a node selects it and filters the Shards view; clicking a primary/replica layer opens the
  same shard inventory with the appropriate filter.
- Hover/focus gives exact values without making the diagram carry every number permanently.
- A search field locates node IDs, addresses, and shard IDs. Filters cover role, liveness, version,
  primary ownership, lag, and schema drift.
- The view updates on the normal collector interval without moving nodes between refreshes unless
  membership changes. Stable positioning is essential for operators to notice change.
- A visible banner reports topology disagreement, stale data, partial collection, or unsupported
  fields. The diagram must never manufacture a single authoritative view from conflicting reports.
- Provide a **Map / Table** switch. The table is the accessible, copyable, small-screen, and high-node-
  count fallback; every map action must be keyboard reachable.
- Above roughly 25 nodes, group nodes by status/role or use a zoomable canvas while preserving the
  table as the exact representation. Do not attempt to show thousands of individual shards in 2.5D.

### Suggested visual encoding

| Meaning | Encoding |
|---|---|
| Primary shards | blue stack layer |
| Replica shards | purple stack layer |
| Healthy | green dot and `UP` label |
| Suspected/stale | yellow dot and elapsed-since-seen label |
| Down | red outline/dot and `DOWN` label |
| Unknown/not sampled | gray outline and `UNKNOWN` label |
| Leader | crown plus `LEADER` badge |
| Reporting node | `THIS NODE` badge |
| Selected node | brighter outline and persistent detail card |
| Schema drift or excessive lag | warning badge on the affected stack, with exact cause in details |

Liveness is a console observation, not a new consensus state: `healthy` means a recent successful
poll, `suspected` means the configured stale threshold has passed, `down` means repeated failed
polls have crossed the failure threshold, and `unknown` means there is not enough evidence. Show
the last-seen duration beside every non-healthy state.

### Data needed for this view

The topology contracts proposed below must supply node ID, advertised endpoints, build version,
term, role, leader, voters, placement version, and assignments. The shard contract must supply
local primary/replica role, epoch/LSN, lag, schema version, WAL bytes, and checkpoint condition.
Every observation also needs reporter node ID and collection time. The first implementation should
use fixture data to build the responsive view while those additive shardlite endpoints are developed.

## Target architecture

```text
 browser
    |
    | same-origin HTTPS, JSON/NDJSON/SSE
    v
 console API
    |-- identity + policy
    |-- cluster registry + credential vault
    |-- topology/health collector
    |-- query streaming + export
    |-- operation coordinator + audit
    |-- local SQLite state store
    |
    | bounded concurrent HTTPS requests
    v
 shardlite seed/member HTTP APIs
    |-- data API: query, execute, tx, route
    `-- operator API: meta, health, topology, shards, metrics, operations
```

### Console backend

Move small durable state from JSON files into a local SQLite database with schema migrations:

- users, password hashes, roles, and connection grants;
- cluster profiles, seed endpoints, labels, TLS policy, and encrypted credentials;
- saved queries and query metadata (never result contents by default);
- audit events and operation records;
- optional downsampled metric history with bounded retention.

The local database is console state, not part of the managed shardlite cluster. It must remain usable
when every managed cluster is unavailable. Use WAL mode, atomic migrations, restrictive file
permissions, and a documented backup/restore procedure.

Introduce explicit backend modules rather than growing the path proxy:

```text
api/          versioned console HTTP contracts and validation
auth/         sessions, password/OIDC providers, policy checks
clusters/     profiles, seed discovery, capability negotiation
collector/    bounded topology, health, shard, and metric polling
queries/      streaming execution, cancellation, export, history metadata
operations/   durable jobs, idempotency, approvals, progress
audit/        append-only security and change events
store/        local SQLite repositories and migrations
shardlite/       typed client for supported shardlite API versions
```

Before fleet polling is introduced, migrate the console server to an async HTTP stack such as
Axum/Reqwest. This is a console-only dependency decision and does not affect shardlite's tokio-free
core. Long query streams, browser clients, collectors, and operation progress must use separate
concurrency limits so one class cannot starve the others.

### Frontend

Keep React and the small Carbon-inspired design system. Add:

- a query/data cache with cancellation and deduplication;
- runtime validation of server responses in addition to generated TypeScript types;
- a virtualized table primitive for shards and query results;
- an accessible dialog/toast/notification system and route-level error boundaries;
- responsive layouts, keyboard navigation, and explicit empty/loading/stale/error states.

Use CodeMirror 6 for the SQL workbench rather than implementing editor behavior in a textarea.
Avoid a large charting package until the required charts cannot be expressed with a small component.

## ShardLite API work required

The current `/v1` endpoints are sufficient for the prototype but not for a distributed management
view. Add additive, versioned, typed responses. Do not make the console scrape logs or infer state
from unrelated counters.

### Foundation endpoints

- `GET /v1/meta` — API version, shardlite build/version, node ID, cluster ID, advertised endpoints,
  and capability names.
- `GET /v1/health` — process readiness plus structured degraded reasons; cheap enough for frequent
  polling and available to a read-only role.
- Extend `GET /v1/cluster` or add `GET /v1/topology` — known members, term, leader, member roles,
  placement version, assignments, and observation timestamp.
- `GET /v1/shards` — per local shard: ownership role, epoch, primary LSN, applied LSN, known replica
  positions, lag, schema version, WAL size, checkpoint counters, and status/reason.
- Extend `GET /v1/stats` — include the already-available writer/reader open and eviction counters,
  checkpoint counters, replication log/ack/replica counters, forwarding counters, election counters,
  and metric type metadata (counter versus gauge and unit).

Every response should include `api_version` and enough identity to reject accidental cross-cluster
merges. Unknown fields must be tolerated. Missing capabilities must disable features gracefully.

### Later operator endpoints

Long-running or destructive work should use a job contract:

- `POST /v1/operations` with an idempotency key and typed operation body;
- `GET /v1/operations/{id}` for status, progress, warnings, result, and error;
- `POST /v1/operations/{id}/cancel` where cancellation is safe.

Candidate operation types are checkpoint, backup/snapshot, restore validation, schema rollout, and
placement change. They are not part of the first v2 milestone. Each must first exist as a safe,
tested shardlite core operation with explicit authorization and failure semantics.

### Contract ownership

Define shared JSON schemas or checked fixtures for shardlite and the console client. CI should test the
current console against the oldest supported shardlite API fixture and current shardlite against the
previous console fixture. This is more valuable than relying on untyped `Record<string, unknown>`.

## Security and authorization model

Replace the two broad console roles with permissions and connection-level grants. Suggested built-in
roles:

| Role | Intended permissions |
|---|---|
| Viewer | fleet/topology/schema/metrics and read-only queries |
| Developer | Viewer plus data writes and transactions |
| Operator | Viewer plus backup/checkpoint/placement operations; no user administration |
| Admin | console configuration, grants, credentials, and shardlite user administration |

The effective permission is the intersection of the console grant and the credential accepted by
shardlite. The console must enforce policy before proxying, and shardlite must still enforce its own role.
Do not use SQL keyword parsing as a security boundary. Read-only query enforcement belongs in
shardlite using SQLite's prepared-statement read-only classification, or connections must use a
genuinely read-only shardlite credential.

Phase 0 security work:

- session idle and absolute expiry, rotation after login, logout-all/revoke, and configurable
  `Secure` cookie behavior;
- login throttling and security audit events;
- CSRF protection for mutations in addition to `SameSite`;
- request body, row, duration, and concurrency limits;
- outbound connect/read/total timeouts and cancellation;
- HTTPS verification by default, optional custom CA, and an explicit development-only plaintext
  mode;
- URL validation and an SSRF policy for saved endpoints, including redirect restrictions;
- strong master-key handling and key rotation. Prefer a random 256-bit key or KMS/secret-manager
  source; do not treat a fast hash of a human passphrase as a password KDF;
- restrictive state-file permissions, redaction of credentials/SQL parameters from logs, CSP and
  other browser security headers;
- immutable audit records for login, connection, access, SQL write, user, and operation changes.

## Delivery plan

### Phase 0 — make the existing console safe and truthful

Goal: a stable v1.1 foundation before broadening scope.

- Correct the placement rendering and replace unknown response maps with typed models.
- Add connection edit, test, enable/disable, timeout, and TLS fields.
- Replace automatic SQL classification with explicit Query / Execute / Transaction actions.
- Gate every proxied route by console permission; hide controls the session cannot use.
- Add session expiry, login throttling, proxy/request limits, security headers, audit storage, and
  console self-health endpoints.
- Fix result behavior: virtualize the visible grid, expose cancellation, and implement a separate
  streamed download path instead of implying a capped grid received the full result.
- Add unit and integration tests for permissions, route validation, timeouts, cancellation, and all
  currently supported UI actions.

Acceptance gate:

- A Viewer cannot write SQL, manage either user system, or call admin-only proxy routes even when
  the stored cluster credential is an admin.
- A query stream cannot exhaust all control-plane workers or grow browser/server memory without
  bound.
- The UI renders the current shardlite response fixtures without raw-object assumptions.
- Security events and every data-changing request have an actor, cluster, action, timestamp, and
  outcome in the audit log.

#### Phase 0 implementation status (July 2026)

The v1.1 foundation is implemented in `console/`:

- typed cluster/topology models and a responsive infrastructure map replace raw-object rendering;
- Viewer, Developer, Operator, and Admin permissions gate every supported proxy method and the UI;
- sessions have idle/absolute expiry and revoke-all, login attempts are throttled, and mutations use
  CSRF tokens;
- connection profiles can be created, edited, tested, enabled/disabled, and assigned timeouts;
  HTTPS is the default, private CA bundles are supported without weakening verification, and
  plaintext HTTP requires an explicit acknowledgement;
- saved URLs are restricted to HTTP(S) origins without embedded credentials, redirects are disabled,
  request bodies are capped, and query-stream concurrency leaves control-plane workers available;
- new stored secrets use ChaCha20-Poly1305 with an Argon2id-derived key, legacy ciphertext remains
  readable, offline key rotation is atomic, and console state files/directories use restrictive
  Unix permissions;
- SQL has explicit Query, Execute, and Transaction modes, cancellable query requests, a virtualized
  bounded grid, truthful truncation messaging, and a separate streaming NDJSON download;
- append-only audit storage, an admin activity view, CSP/security headers, and console `/healthz`
  and `/readyz` endpoints are present;
- backend unit tests, shardlite HTTP integration tests, the production frontend build, and the
  end-to-end console smoke suite pass.

The remaining Phase 0 hardening is intentionally tracked rather than implied complete: structured
request IDs and graceful stream-draining shutdown, plus browser-level Playwright/accessibility
tests. Those items do not weaken the current permission or memory-safety acceptance gates, but they
are required before calling Phase 0 production-complete.

### Phase 1 — distributed observability MVP

Goal: make cluster, node, shard, and replica health first-class.

- Add `/v1/meta`, `/v1/health`, typed topology, typed shard state, and expanded metrics to shardlite.
- Change a connection profile from one URL to a cluster with multiple seeds and discovered members.
- Build the bounded collector with per-cluster concurrency, jitter, backoff, and stale-sample rules.
- Add Fleet Overview, Cluster Overview, Topology, and Shard Inventory screens.
- Detect and display unreachable nodes, leader disagreement, unavailable shards, replica lag,
  version skew, schema drift, election churn, checkpoint stalls, and repeated bootstraps.
- Store short metric/event history locally with retention and downsampling.

Acceptance gate:

- In an automated three-node scenario, the console detects a stopped node and a leader change
  within two polling intervals and shows which observation produced the status.
- Conflicting topology reports are visible and never flattened into a false single truth.
- A fleet of at least 25 clusters, 100 nodes, and 10,000 shard rows remains responsive through
  bounded polling and table virtualization.
- Restarting the console preserves profiles, grants, audit history, and the configured metric window.

#### Phase 1 implementation status (July 2026)

The distributed read model is implemented:

- ShardLite serves versioned `/v1/meta`, `/v1/health`, `/v1/topology`, and `/v1/shards` contracts plus
  checkpoint metrics; health states name their local observation boundary rather than inventing
  follower-side peer liveness;
- profiles support up to 32 manually configured HTTP seeds with legacy single-URL migration and an
  ephemeral preferred healthy seed for interactive traffic;
- an eight-worker collector enforces a four-poll per-cluster cap, bounded queue, 4 MiB response cap,
  five-second timeout cap, deterministic jitter, exponential backoff, and 15-second staleness;
- last-good evidence and its timestamp survive individual poll failures, while leader, term, and
  version disagreement remain visible as conflicts;
- recent per-seed metric/evidence history persists atomically in `observability.json` across console
  restarts with bounded retention;
- Fleet Overview, Cluster Overview, multi-observer Topology, per-seed Stats, and a virtualized Shard
  Inventory are available to Viewer and higher roles;
- shard positions aggregate server-side to one row per shard; primary/replica lag is calculated only
  within a matching epoch, and unavailable, conflicting, or incomparable states are explicit;
- the three-node integration scenario observes leader loss, a higher-term replacement, the stopped
  member, and renewed quorum health; HTTP contracts, backend tests, the production frontend build,
  and expanded end-to-end smoke suite pass.

Remaining Phase 1 production work is explicit: automatic seed discovery requires ShardLite to advertise
peer HTTP origins (native peer addresses cannot safely be guessed as HTTP), schema-drift collection
needs a cheap bulk schema-version contract, repeated-bootstrap counters do not yet exist in the core,
and the 25-cluster/100-node/10,000-shard load target plus Playwright/accessibility workflows still
need dedicated CI jobs. These gaps are not represented as healthy or supported in the UI.

### Phase 2 — database workbench

Goal: make daily SQLite work productive without weakening safety.

Implemented:

- CodeMirror SQLite editors with tabs, explicit modes, `Cmd/Ctrl+Enter`, cancellation, explain,
  browser-scoped saved SQL, and metadata-only execution history.
- Typed positional parameters for null, safe integer, real, text, boolean, and explicit hex blobs;
  the blob contract is additive at the ShardLite JSON edge and never guesses that an array is binary.
- Shard and route-by-key targeting, linearizable/stale/at-least-LSN reads, fan-out query, atomic
  single-shard transactions, and guarded all-shard schema rollouts.
- Rollout preflight shows every starting schema version; result handling preserves every shard
  outcome and labels any rejection as partial, with no global-success presentation.
- Virtualized result rows, adjustable column widths, cell/row copying, distinct NULL/blob display,
  explain output, and streamed NDJSON/CSV downloads capped from 10,000 through 10,000,000 rows or
  uncapped. Hitting a cap or cancelling the browser download drops the upstream reader.
- Schema objects and definitions plus table columns, indexes, foreign keys, triggers, and an exact
  cross-shard drift report grouped by definition and shard.

- CodeMirror SQL tabs, explicit execution mode, typed parameters, keyboard shortcuts, cancellation,
  explain plan, query history metadata, and saved queries.
- Shard selector plus key-to-shard routing, visible consistency semantics, and `at_least_lsn` input.
- Complete transaction builder and all-shard query/DDL workflows with preflight and per-shard result.
- Virtualized results, column sizing, copy cells/rows, NULL/blob rendering, and streamed CSV/NDJSON
  export with configurable limits.
- Rich schema explorer for tables, columns, indexes, triggers, foreign keys, and definitions.
- Cross-shard schema comparison with a clear drift report.

Acceptance gate:

- Query, execute, transaction, route-by-key, query-all, and execute-all are each covered end to end.
- A million-row export remains streamed and bounded; cancelling it closes the upstream request.
- The UI never presents a partial all-shard result as a global success.
- Schema differences identify the exact shards and definitions involved.

The API/backend acceptance path passes in the 38-check smoke suite, including route-by-key,
at-least-LSN, blob binding, fan-out query, transaction, all-shard rollout, and bounded CSV export.
Rust unit/contract tests and the production TypeScript build also pass.

Remaining Phase 2 production work: browser automation for keyboard, clipboard, cancellation, and
accessibility; a dedicated one-million-row soak job (the converter is bounded and the smoke suite
currently streams 60,000 rows); server-side multi-device saved-query synchronization; and a durable
export-job API if product requirements demand an in-console cancel button instead of the browser
download manager's cancellation. Fan-out parameters and a cross-shard snapshot are intentionally
not offered because ShardLite's planner/protocol do not currently guarantee them.

### Phase 3 — controlled operations

Goal: coordinate only operations ShardLite already exposes, entirely inside the standalone console.
Phase 3 does not add or change a ShardLite/SQLite endpoint, protocol, database file, or WAL behavior.

Implemented:

- A bounded, atomic `console-data/operations.json` journal with restrictive file permissions. It
  stores the approved schema SQL because the background worker must execute it; audit events still
  exclude SQL and parameters.
- A single console worker with durable queued/running/succeeded/partial/failed/cancelled/interrupted
  states, timestamps, stages, actor, cluster, immutable SQL fingerprint, approval evidence, and exact
  per-shard outcomes.
- Server-side preflight over the existing `/v1/info` and `/v1/schema/{shard}` endpoints. The worker
  repeats preflight and refuses to call ShardLite if any approved version changed.
- Idempotency scoped to actor, cluster, and key. A duplicate retry returns the original operation;
  reusing its key for different SQL or approval evidence is rejected.
- Safe cancellation while queued or revalidating. Once `/v1/execute_all` has been sent, cancellation
  is refused because some shards may already have changed.
- Restart recovery that resumes queued jobs but marks an in-flight job `interrupted` for manual
  verification. DDL is never replayed automatically after an ambiguous process crash.
- An Operations workspace with polling, filtering, approval evidence, SQL, lifecycle status,
  cancellation, partial/interrupted warnings, and exact shard results. Developers see their own
  records; administrators see all records; viewers and operators cannot access the operation API.
- Submit/cancel/completion audit events containing operation ID and cluster but no SQL text.

Explicit non-goals under the no-server-change constraint:

- backup/snapshot creation, restore, explicit checkpoint, placement/rebalance, automatic failover,
  or individual-shard DDL retry;
- direct access from the console to ShardLite database/WAL files;
- simulated controls for capabilities absent from the current ShardLite HTTP API.

Acceptance gate:

- Refreshing or retrying with the same idempotency key cannot duplicate a submitted operation.
- Restarting the console cannot replay an operation whose result is ambiguous.
- A changed preflight prevents execution, and every ShardLite response remains attributable per shard.
- Partial execution is never presented as global success.

The 51-check end-to-end smoke suite covers preflight, durable submit, duplicate retry, completion,
per-shard result, journal persistence, stale-approval refusal, lifecycle audit, history, and permission denial. Console unit tests cover
idempotency conflict, cancellation, persistence, and restart interruption.

Remaining Phase 3 production work is console-side only: scheduled failure injection for node loss,
leader change, timeout, and process termination at each stage; browser automation for the Operations
screen; journal schema migration/versioning; retention configuration and backup guidance; and an
optional SSE status stream to replace two-second UI polling. None requires a ShardLite server change.

### Phase 4 — organization and production scale

- OIDC/SAML login while retaining a local break-glass admin.
- Fine-grained custom roles, teams, and connection groups.
- External secrets providers and master-key rotation workflow.
- Prometheus/OpenTelemetry export and integration with an existing metrics backend for long retention.
- Alert routing, maintenance windows, and acknowledgement.
- Optional highly available console deployment after state/session coordination is designed.

## Testing strategy

- **Contract tests:** versioned JSON fixtures for all shardlite APIs and capability combinations.
- **Backend tests:** policy matrix, persistence migrations, encryption/key rotation, SSRF validation,
  timeouts, rate limits, cancellation, and audit completeness.
- **Frontend tests:** component tests for status states and Playwright workflows for every primary
  screen, including keyboard and accessibility checks.
- **Distributed scenarios:** one-node, three-node, node loss, partition, election, lagging replica,
  forced bootstrap, schema drift, bad credentials, and mixed versions.
- **Load tests:** simultaneous query streams, slow clients, collector fan-out, large shard lists, and
  million-row export under fixed memory/concurrency budgets.
- **Security tests:** CSRF, session fixation/expiry, privilege escalation, path manipulation, malicious
  upstream responses, endpoint redirects, and secret/log leakage.

CI should keep the existing smoke test, add console server tests and frontend tests separately, then
run a smaller distributed scenario on each change. Full failure-injection and load suites can run on
a scheduled pipeline.

## Operational requirements for the console itself

- `/healthz`, `/readyz`, and `/metrics` endpoints that do not depend on managed clusters.
- Structured logs with request/trace IDs, actor, cluster, endpoint, latency, and redaction.
- Graceful shutdown that cancels collectors, drains short requests, and terminates streams within a
  configured deadline.
- Single binary and container image releases, versioned local-store migrations, upgrade/rollback
  documentation, and signed artifacts.
- Documented sizing by clusters/nodes/shards, state backup, key backup/rotation, reverse-proxy TLS,
  and disaster recovery.

## Deliberate non-goals

- The console will not create cross-shard transactions; shardlite does not provide that guarantee.
- It will not edit SQLite files, WAL files, placement manifests, or election state directly.
- It will not automatically force failover or rebalance based on UI-side heuristics.
- It will not become a general long-term metrics database.
- It will not store unrestricted query result contents unless a future explicit feature requires it.

## Recommended first implementation slice

Start with Phase 0 and the read-only half of Phase 1:

1. Define typed `meta`, `health`, `topology`, `shard`, and `stats` contracts and fixtures.
2. Fix the existing topology/SQL/result correctness issues and enforce a route permission matrix.
3. Move console state to local SQLite and add audit/session hardening.
4. Add multi-seed profiles and the bounded collector.
5. Ship Fleet Overview, Cluster Overview, Topology, and Shards before building operational buttons.

This slice converts the console from a collection of endpoint viewers into a trustworthy distributed
read model. SQL workbench and operator actions can then build on identities, permissions, health,
and audit semantics that are already correct.
