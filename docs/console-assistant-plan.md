# meshdb console — AI assistant design

An in-console assistant that manages a meshdb cluster from natural language, backed by an
**OpenAI-compatible** chat-completions endpoint (OpenAI, Azure OpenAI, or a self-hosted
vLLM/Ollama/LiteLLM gateway — anything that speaks `/v1/chat/completions` with tool calling).

The assistant is a **thin, safe orchestration layer over the management surface that already exists**
(the A–E endpoints + SQL, reached through the console's `conn(name)` client and its
proxy/permission/audit machinery). It adds no new authority: it can only do what the signed-in user
could already do by clicking, and every action goes through the same policy and audit path.

## What it is for

Two jobs, in one chat panel scoped to a connection:

1. **Observe & explain** — "is the cluster healthy?", "which shard is hottest?", "why did node 2 step
   down?", "are we reshuffling too often?", "show me orders per region". It calls the read tools,
   grounds itself in live state, and answers with tables/verdicts instead of prose guesses.
2. **Author & manage data — full CRUD** — tables ("create an `orders` table", "add a `status`
   column", "drop the `tmp_import` table"), **reports** ("save this as a monthly-revenue report"),
   and **dashboards** ("build an ops dashboard from the revenue and shard-health reports"). The model
   writes the SQL/DDL or composes the tiles; the user confirms before anything is created, changed,
   or dropped.
3. **Operate, with a human in the loop** — "drain node 3 for maintenance", "move shard 7 to node 2",
   "vacuum the shards over 1 GB", "recover shard 5 from S3", "turn on S3 archival to this bucket". It
   *proposes* the exact API calls; the user confirms before anything mutates.

Non-goal: autonomous operation. The assistant never mutates, creates, or deletes without an explicit
human confirm — and **every delete confirms**, destructive ones with a typed confirmation (below).

## Architecture

```
  ┌─ console/web ─────────────┐        ┌─ console/server ───────────────────────────────┐
  │  Assistant panel (chat)   │  POST  │  /api/connections/<n>/assistant                 │
  │  - streams tokens         │ ─────▶ │   orchestrator (tool-calling loop):             │
  │  - renders tool calls     │        │    1. build tools = DEFINED ∩ user's Permissions │
  │  - Confirm/Reject on a     │        │    2. call OpenAI-compatible /chat/completions   │
  │    proposed mutation      │ ◀───── │    3. read tool  → execute via conn proxy (auto) │
  └───────────────────────────┘  SSE   │       write tool → emit PROPOSED action, pause   │
                                        │    4. feed tool results back, loop               │
                                        │  audit every executed tool (actor + "via ai")    │
                                        └──────────────┬───────────────────────────────────┘
                                                       │ reuses proxy.rs (forward / post_json_result)
                                                       ▼
                                        meshdb  /v1/*  (Admin/Operate/... enforced AGAIN)
```

- **Frontend**: a `views/Assistant.tsx` workspace sub-view (permission `observe`), a streaming chat
  UI. Tool calls and their results are shown inline (transparency); a proposed mutation renders as a
  card with the exact endpoint+args and **Confirm / Reject** buttons.
- **Backend orchestrator**: a new module `console/server/src/assistant.rs` running the tool-calling
  loop synchronously (matching the console's thread-per-request model), streaming the model's tokens
  back over SSE/chunked (the query path already streams NDJSON, same mechanism).
- **LLM client**: `assistant/llm.rs` — a small blocking `ureq` client for `POST {base}/chat/completions`
  (streaming), reusing the console's existing HTTP/TLS setup.
- **Tool layer**: each tool maps 1:1 to an existing `conn(name).*` capability and is executed through
  the SAME `crate::proxy` call the manual UI uses — so `proxy_permission` and the meshdb-side
  `Requirement` check both still run.

## The AI harness (the agent runtime the assistant owns and maintains)

The assistant is not a one-shot LLM call; it runs on a small, self-contained **agent harness**
(`console/server/src/assistant/`) that the console owns, versions, and tests. Making the harness a
first-class component — rather than scattering prompt strings and HTTP calls through the UI — is what
keeps the assistant safe, debuggable, and maintainable as models and tools change. Its parts:

- **Turn engine (the agent loop).** A bounded loop — assemble context → call the provider → parse tool
  calls → dispatch → feed results → repeat — capped by `max_tool_calls`, a token budget, and a
  wall-clock timeout so a turn can never run away. It is a small explicit state machine:
  `Thinking → ToolCall(read → run | mutating → propose) → AwaitConfirm → Resume → Answer`.
- **Tool registry.** Tools are declared once as data — `Tool { name, json_schema, permission,
  mutating, destructive, handler }`. The harness derives the model's tool list from
  `registry ∩ the user's role`, dispatches by name, validates arguments against the schema, and
  enforces the confirm/delete rules **from the flags** in one place — the model cannot opt out of them.
- **Session store.** Conversations are persisted server-side (`store/assistant/<session>.json`, dir
  `0700`): the message history, every tool call + result, the pending proposed-action, token usage,
  and the model + prompt/tool **versions** the turn ran under. So sessions are resumable across
  reloads, the confirm state survives between HTTP requests, and the whole transcript is auditable and
  reproducible. Context-window pressure is handled by summarising old turns, not silently truncating.
- **Provider client.** One OpenAI-compatible client (streaming SSE parse, retry/backoff, timeout,
  typed error mapping) behind an interface, so provider/model is pure config and a bad gateway
  degrades to a clear error rather than a hang.
- **Guardrail layer.** Permission parity, confirm gating, typed-delete, treating tool output as data
  (injection defence), and secret redaction are enforced *by the harness around* the model, never
  delegated to the prompt.
- **Prompt & schema versioning.** The system prompt and tool schemas are versioned artifacts; each
  session records the version it used, so an upgrade is deliberate and old sessions stay interpretable.
- **Observability & evaluation.** Every turn emits a trace (tokens, latency, tools chosen, cost,
  outcome). A **golden-conversation eval suite** (recorded scenarios → expected tool selection and
  guardrail behaviour, run against a mock provider) is CI-checked, so a prompt or tool change that
  regresses tool-choice or weakens a confirm is caught before it ships. This eval harness is how the
  assistant is *maintained* over time — the same "verify, don't assume" bar as the rest of the system.

Everything below (tools, resources, confirm flow, config) plugs into this harness; the harness is the
single place that turns a model's suggestion into a policy-checked, audited action.

## Tools (mapped to the management surface, grouped by the console `Permission` that gates them)

| Permission | Tools (→ endpoint / resource) | Auto-run? |
|---|---|---|
| **Observe** | `get_health`→`/v1/health`, `get_topology`, `get_shards`, `get_replication`, `get_stats`, `get_config`, `get_schema_agreement`, `get_s3_status`, `list_schema`→schema-catalog, `shard_inventory`, `get_fleet` | ✅ auto |
| **Query** | `run_query(sql)` → `/v1/query_all` (read-only SQL) | ✅ auto |
| **Write** | `run_write(sql)` → `/v1/run` | ⛔ confirm |
| **Table CRUD** (DDL, Admin) | `create_table` / `alter_table` → durable schema-rollout op; `describe_table` → schema-catalog (read, auto); `drop_table` | ⛔ confirm · **drop = typed confirm** |
| **Report CRUD** (author = Developer, read = Observe) | `list_reports` / `get_report` / `run_report` (read, auto); `create_report` / `update_report`; `delete_report` | create/update ⛔ confirm · **delete = confirm** |
| **Dashboard CRUD** (author = Developer, read = Observe) | `list_dashboards` / `get_dashboard` (read, auto); `create_dashboard` / `update_dashboard` (add/remove/arrange tiles); `delete_dashboard` | create/update ⛔ confirm · **delete = confirm** |
| **Operate** | `vacuum_shard(n)`, `checkpoint_shard(n)`, `cordon_node(bool)`, `prefer_shard(shard,prefer)`, `step_down`, `drain_node`, `recover_shard(n)`, `s3_snapshot`, `s3_flush`, `s3_configure(...)`, `s3_apply` | ⛔ confirm |
| **Admin** | `declare_shard_key(table,col)`, mesh-user tools | ⛔ confirm |

The tool set offered to the model is the **intersection of the defined tools and the signed-in user's
role permissions** (`Role::permits`). A Viewer's assistant is literally handed only the read tools —
it *cannot* propose a mutation because the function isn't in its schema. This is permission parity by
construction, not by prompt instruction.

Tool arguments are validated in the backend before use (shard/node numbers in range, identifiers for
`declare_shard_key` match `[A-Za-z_][A-Za-z0-9_]*`), and SQL is still subject to meshdb's own
read/write classification — the model's output is never trusted as a security boundary.

## The tool-calling loop + human-in-the-loop

Read-only tools execute automatically and their results are fed back to the model, so a question like
"why is node 2 unhealthy?" resolves in one turn (health → topology → replication → explanation).

A **mutating** tool call does NOT execute. Instead the orchestrator:
1. stops the loop and returns a **proposed action** `{tool, args, endpoint, http_method, human_summary}`;
2. the frontend renders it as a confirm card with the *exact* call and its blast radius;
3. on **Confirm**, the frontend re-posts with `confirm_action_id`; the backend executes it through
   the proxy (policy + meshdb authz + audit), feeds the result back to the model, and continues;
4. on **Reject**, the rejection is fed back so the model can revise.

So the model *plans and explains*; the human *authorises every change*. This also neutralises
prompt-injection: even if untrusted data (a query result, a log line) coaxes the model into proposing
something harmful, a human sees the concrete action and blast radius before it runs.

## Managed resources: tables, reports, dashboards

The assistant does full CRUD on three kinds of thing. Two are new console-server resources; one is
meshdb schema. **Every delete confirms** (see below).

- **Tables (meshdb schema).** `create_table` / `alter_table` / `drop_table` are DDL, so they do **not**
  go through the raw `/v1/run` proxy — they route through the console's existing **durable
  schema-rollout operation** (`/api/operations` preflight → submit → per-shard rollout with status),
  the same path the Operations view uses, because a shard-count-wide DDL must be coordinated and
  resumable, not fire-and-forget. `describe_table` reads the assembled schema catalog. The assistant
  proposes the DDL; on confirm it opens an operation and reports progress. Adopting a table's primary
  key as its shard key still happens automatically on `create_table` (no extra step).

- **Reports (new console resource).** A *report* is a saved, named, shareable analytical query:
  `{ id, name, description, connection, sql, params[], viz: table|bar|line|number, created_by, updated_at }`.
  Persisted server-side in a `reports.json`-style registry (mirroring `registry.rs`; no secrets, so no
  sealing), with `GET/POST/PUT/DELETE /api/reports[/…]` and role gating (author = Developer, read =
  Observe). This promotes the workbench's current *localStorage* saved-queries into first-class,
  shareable objects. Assistant tools: `list_reports`, `get_report`, `run_report(id, params)` (executes
  its SQL through the read path), `create_report`, `update_report`, `delete_report`. So "make a report
  of monthly revenue by region" → the model writes the SQL, previews it via `run_query`, and (on
  confirm) saves a report.

- **Dashboards (new console resource).** A *dashboard* is an arrangement of report tiles:
  `{ id, name, description, tiles: [{ report_id, position, size, viz_override }], created_by, updated_at }`.
  Persisted alongside reports, `GET/POST/PUT/DELETE /api/dashboards[/…]`, same gating. Tools:
  `list_dashboards`, `get_dashboard`, `create_dashboard`, `update_dashboard` (add/remove/rearrange
  tiles, referencing existing reports), `delete_dashboard`. So "build me an ops dashboard from the
  revenue and shard-health reports" → the model composes tiles from existing reports and (on confirm)
  saves the dashboard.

Reports and dashboards are ordinary console features with their own UI (a Reports gallery + Dashboard
canvas); the assistant is one way to author them, and the confirm/audit rules apply identically
whether a change comes from the assistant or the UI.

## Grounding

- **System prompt** carries meshdb's model so the assistant reasons correctly: shards are fixed at
  creation; cross-shard transactions are unavailable; the *safe* control operations (drain / step-down
  / cordon / prefer are subtractive/advisory, never an imperative override); reads fan out and merge;
  "refuse over approximate". A trimmed version of `docs/console-management-plan.md` seeds it.
- **Live state, not memory**: the assistant is instructed to call observe tools before asserting
  cluster facts, so answers reflect the cluster now rather than a hallucination. The current
  `/v1/health`, `/v1/replication`, and the churn signal (`placement_changes`, the "reshuffling too
  frequently" warning) are its diagnostic inputs.

## Configuration (OpenAI-compatible, provider-agnostic)

Console-global AI settings (admin-managed), stored like connection secrets:
- `base_url` (e.g. `https://api.openai.com/v1`, an Azure deployment URL, or `http://localhost:11434/v1`),
- `model` (e.g. `gpt-4o`, `claude-sonnet-…` via a gateway, a local model name),
- `api_key` — **sealed** with the existing `crypto::Sealer` (ChaCha20-Poly1305 under `MESHDB_CONSOLE_KEY`),
  never returned by the API, preserve-on-omit on edit — identical to connection/S3 secrets in `registry.rs`,
- `enabled`, optional `organization`, per-turn `max_tool_calls`, token budget, request timeout.

Because it's just an OpenAI-compatible base URL + key, it works with OpenAI, Azure, or a self-hosted
model — no vendor lock-in. A self-hosted endpoint also keeps cluster data on-prem.

## Safety model (this is the point)

- **No new authority.** Tools run through `crate::proxy`; `proxy_permission` and the meshdb `Requirement`
  check both still apply. The assistant can never exceed the user's role or the stored credential.
- **Confirm every mutation.** Read auto-runs; write/operate/admin/CRUD require an explicit human confirm.
- **Delete always confirms — and destructive deletes are *typed* confirms.** `drop_table`,
  `delete_report`, `delete_dashboard`, mesh-user removal, connection removal: every delete requires an
  explicit confirm, and an irreversible/data-losing one (`drop_table`) requires the user to type the
  object's name to proceed — the same bar as the manual UI's destructive actions. A delete tool is
  never auto-run and never batched away behind another action; the confirm card names exactly what
  disappears and whether it is recoverable.
- **Audit with provenance.** Every executed tool is written to the append-only ledger as the signed-in
  user with a `via: assistant` marker and the model/prompt id — AI actions are *more* traceable, not less.
- **Untrusted tool output.** Query results, logs, and node responses fed back to the model are treated
  as data, never instructions; the confirm gate is the backstop.
- **Bounded turns.** `max_tool_calls` per turn, a token budget, and request timeouts stop runaway loops
  and cost. Rate-limited per user (reuse `LoginLimiter`-style throttling).
- **Redaction.** The ledger already refuses to record SQL params/credentials; assistant transcripts
  follow the same rule, and secrets are never placed in the prompt.

## UX

- A workspace **Assistant** tab: chat with streaming answers, inline tool-call chips (name + result
  summary, expandable), tables for query results, and status cards for health/topology answers.
- Proposed mutations render as a **confirm card** showing the exact endpoint, args, and blast radius,
  with Confirm / Reject — the same confirmation ethos as the manual UI's destructive actions.
- A "what can you do?" affordance lists the tools available to *this* user's role.

## Delivery slices

0. **AI settings** — sealed `base_url`/`model`/`api_key`, admin UI, a "test" round-trip. (registry +
   api + a small settings form.)
1. **The harness + read-only assistant** — build the agent runtime itself (turn engine, tool registry,
   session store, provider client, guardrail layer, versioning, traces) with only the observe +
   `run_query` + `run_report` tools registered, plus the **golden-conversation eval suite** in CI.
   Highest value, lowest risk: NL Q&A + SQL grounded in live state, *zero* mutation — and every later
   slice is just registering more tools on the same, already-tested harness.
2. **Reports & dashboards resources** — the server-side `reports`/`dashboards` registries + REST API +
   their own UI (gallery + canvas). A prerequisite for authoring; useful on its own, no AI required.
3. **CRUD with confirm** — `create/update_table` (via the rollout op) and report/dashboard authoring
   tools behind the proposed-action + confirm flow; **every delete a typed/plain confirm**; audited
   with `via: assistant`.
4. **Operate with confirm** — the cluster control tools (drain / step-down / cordon / prefer / vacuum /
   checkpoint / recover / S3) behind the same confirm + audit flow.
5. **Diagnostics** — proactive summaries from the churn/health signals ("3 handovers in 5 min — node 2
   is flapping; here's the evidence"), a one-click "explain this alert", and "turn this into a report".
6. **Fleet awareness** — operate across connections from one chat; org-scoped guardrails.

## Risks / non-goals

- **Hallucinated confidence** — mitigated by tool-grounding (call before asserting) and by never
  auto-mutating.
- **Prompt injection via cluster data** — mitigated by the confirm gate and treating tool output as data.
- **Cost / runaway loops** — bounded turns, token budget, rate limits.
- **Not autonomous.** No unattended operation; no self-approved mutations. The assistant is an operator
  *aid*, and the human stays the authority — the same principle as the safe control plane it drives.
