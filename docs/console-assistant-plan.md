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
2. **Operate, with a human in the loop** — "drain node 3 for maintenance", "move shard 7 to node 2",
   "vacuum the shards over 1 GB", "recover shard 5 from S3", "turn on S3 archival to this bucket". It
   *proposes* the exact API calls; the user confirms before anything mutates.

Non-goal: autonomous operation. The assistant never mutates without an explicit human confirm (below).

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

## Tools (mapped to the management surface, grouped by the console `Permission` that gates them)

| Permission | Tools (→ endpoint) | Auto-run? |
|---|---|---|
| **Observe** | `get_health`→`/v1/health`, `get_topology`, `get_shards`, `get_replication`, `get_stats`, `get_config`, `get_schema_agreement`, `get_s3_status`, `list_schema`→schema-catalog, `shard_inventory`, `get_fleet` | ✅ auto |
| **Query** | `run_query(sql)` → `/v1/query_all` (read-only SQL) | ✅ auto |
| **Write** | `run_write(sql)` → `/v1/run` | ⛔ confirm |
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
- **Confirm every mutation.** Read auto-runs; write/operate/admin require an explicit human confirm.
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
1. **Read-only assistant** — observe + `run_query` tools, streaming answers, tool-call transparency.
   Highest value, lowest risk: NL Q&A + SQL grounded in live state, *zero* mutation capability.
2. **Operate with confirm** — write/operate/admin tools behind the proposed-action + confirm flow,
   audited with `via: assistant`.
3. **Diagnostics** — proactive summaries from the churn/health signals ("3 handovers in 5 min — node 2
   is flapping; here's the evidence"), and a one-click "explain this alert".
4. **Fleet awareness** — operate across connections from one chat; org-scoped guardrails.

## Risks / non-goals

- **Hallucinated confidence** — mitigated by tool-grounding (call before asserting) and by never
  auto-mutating.
- **Prompt injection via cluster data** — mitigated by the confirm gate and treating tool output as data.
- **Cost / runaway loops** — bounded turns, token budget, rate limits.
- **Not autonomous.** No unattended operation; no self-approved mutations. The assistant is an operator
  *aid*, and the human stays the authority — the same principle as the safe control plane it drives.
