// Typed client for the console's own backend API. Same origin, so the session cookie rides along
// automatically. Everything the SPA does goes through here; nothing talks to meshdb directly.

export type Role = "viewer" | "developer" | "operator" | "admin";
export type Permission = "observe" | "query" | "write" | "operate" | "admin";

export function permits(role: Role | undefined, permission: Permission): boolean {
  if (!role) return false;
  if (role === "admin") return true;
  if (permission === "observe" || permission === "query") return true;
  if (permission === "write") return role === "developer";
  if (permission === "operate") return role === "operator";
  return false;
}

export interface Me {
  user: string;
  role: Role;
  csrf_token: string;
}

export interface Connection {
  name: string;
  url: string;
  seeds: string[];
  meshdb_user?: string | null;
  enabled: boolean;
  timeout_ms: number;
  allow_insecure_http: boolean;
  custom_ca_pem?: string | null;
}

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

let csrfToken: string | null = null;

async function req<T>(method: string, path: string, body?: unknown, signal?: AbortSignal): Promise<T> {
  const headers: Record<string, string> = {};
  if (body !== undefined) headers["Content-Type"] = "application/json";
  if (method !== "GET" && csrfToken) headers["X-CSRF-Token"] = csrfToken;
  const res = await fetch(path, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
    credentials: "same-origin",
    signal,
  });
  const text = await res.text();
  const data = text ? JSON.parse(text) : {};
  if (!res.ok) {
    if (res.status === 401) csrfToken = null;
    throw new ApiError(res.status, data?.error ?? res.statusText);
  }
  return data as T;
}

// --- console auth ---
export async function me(): Promise<Me> {
  const value = await req<Me>("GET", "/api/me");
  csrfToken = value.csrf_token;
  return value;
}
export async function login(username: string, password: string): Promise<Me> {
  const value = await req<Me>("POST", "/api/login", { username, password });
  csrfToken = value.csrf_token;
  return value;
}
export async function logout(): Promise<{ ok: boolean }> {
  const value = await req<{ ok: boolean }>("POST", "/api/logout");
  csrfToken = null;
  return value;
}

// --- console users (admin) ---
export const consoleUsers = {
  list: () => req<{ name: string; role: Role }[]>("GET", "/api/console-users"),
  create: (username: string, password: string, role: Role) =>
    req("POST", "/api/console-users", { username, password, role }),
  remove: (name: string) => req("DELETE", `/api/console-users/${encodeURIComponent(name)}`),
};

export interface AuditEvent {
  t: number;
  actor?: string | null;
  action: string;
  target: string;
  outcome: string;
}

export const audit = {
  list: () => req<AuditEvent[]>("GET", "/api/audit"),
};

export type OperationStatus =
  | "queued"
  | "running"
  | "succeeded"
  | "partial"
  | "failed"
  | "cancelled"
  | "interrupted";

export interface ShardVersion {
  shard: number;
  schema_version: number;
}

export interface OperationPreflight {
  connection: string;
  sql_fingerprint: string;
  token: string;
  versions: ShardVersion[];
  observed_at_ms: number;
}

export interface OperationRecord {
  id: string;
  kind: "schema_rollout";
  actor: string;
  connection: string;
  status: OperationStatus;
  stage: string;
  created_at_ms: number;
  updated_at_ms: number;
  idempotency_key: string;
  sql: string;
  sql_fingerprint: string;
  preflight_token: string;
  expected_versions: ShardVersion[];
  observed_versions: ShardVersion[];
  outcomes: { shard: number; ok: boolean; error?: string | null }[];
  error?: string | null;
  cancel_requested: boolean;
}

export const operations = {
  preflight: (connection: string, sql: string) =>
    req<OperationPreflight>("POST", "/api/operations/preflight", { connection, sql }),
  submit: (value: {
    connection: string;
    sql: string;
    idempotency_key: string;
    preflight_token: string;
    expected_versions: ShardVersion[];
  }) => req<OperationRecord>("POST", "/api/operations", value),
  list: () => req<OperationRecord[]>("GET", "/api/operations"),
  get: (id: string) => req<OperationRecord>("GET", `/api/operations/${encodeURIComponent(id)}`),
  cancel: (id: string) => req<OperationRecord>("POST", `/api/operations/${encodeURIComponent(id)}/cancel`),
};

// --- connections ---
export const connections = {
  list: () => req<Connection[]>("GET", "/api/connections"),
  create: (c: {
    name: string;
    url: string;
    seeds?: string[];
    meshdb_user?: string;
    meshdb_secret?: string;
    replace?: boolean;
    enabled?: boolean;
    timeout_ms?: number;
    allow_insecure_http?: boolean;
    custom_ca_pem?: string;
  }) => req("POST", "/api/connections", c),
  remove: (name: string) => req("DELETE", `/api/connections/${encodeURIComponent(name)}`),
  test: (name: string) =>
    req<{ ok: boolean; seed: string; latency_ms: number; info: NodeInfo }>(
      "POST",
      `/api/connections/${encodeURIComponent(name)}/test`,
    ),
};

export type ClusterHealthStatus = "healthy" | "degraded" | "unavailable";

export interface FleetSummary {
  name: string;
  status: ClusterHealthStatus;
  observed_at_ms: number;
  last_success_ms?: number | null;
  stale: boolean;
  seeds: number;
  reachable_nodes: number;
  node_count: number;
  leader?: string | null;
  versions: string[];
  preferred_seed?: string | null;
  issues: string[];
}

export interface NodeObservation {
  seed: string;
  last_attempt_ms: number;
  last_success_ms?: number | null;
  latency_ms?: number | null;
  error?: string | null;
  meta?: NodeMeta | null;
  health?: NodeHealth | null;
  topology?: ClusterInfo | null;
  shards?: ShardContract | null;
  stats?: NodeStats | null;
}

export interface ClusterObservation extends FleetSummary {
  nodes: NodeObservation[];
}

export const fleet = {
  list: () => req<FleetSummary[]>("GET", "/api/fleet"),
};

// --- per-connection proxy to meshdb /v1 ---
function base(name: string) {
  return `/api/connections/${encodeURIComponent(name)}`;
}

export interface MetricSample {
  t: number;
  source?: string;
  stats: Record<string, unknown>;
}

export interface NodeInfo {
  shard_count: number;
  epoch?: number | null;
  version?: string;
  forwarding?: boolean;
}

export type ReadConsistency = "linearizable" | "stale" | { at_least_lsn: number };

export interface NodeMeta extends NodeInfo {
  api_version?: number;
  node?: string | number | null;
  clustered?: boolean;
  epoch?: number | null;
  capabilities?: Record<string, boolean>;
}

export interface NodeHealth {
  status: ClusterHealthStatus | "unknown";
  observed_at_ms?: number;
  node?: string | number | null;
  term?: number;
  role?: string;
  leader?: string | number | null;
  derived?: boolean;
  checks?: Record<string, { status?: string; [key: string]: unknown }>;
}

export type MemberStatus = "up" | "suspected" | "down" | "unknown";

export interface ClusterMember {
  node: string | number;
  address?: string | null;
  this_node?: boolean;
  status?: MemberStatus;
}

export interface ClusterCounters {
  elections_started?: number;
  became_leader?: number;
  stepped_down?: number;
  heartbeats_sent?: number;
  peer_unreachable?: number;
  votes_granted?: number;
  votes_refused?: number;
  handover_failed?: number;
}

export interface ClusterInfo {
  clustered: boolean;
  shard_count?: number;
  node?: string | number;
  term?: number;
  role?: string;
  leader?: string | number | null;
  voters?: number;
  members?: ClusterMember[];
  led_shards?: number[];
  placement?: {
    term?: number;
    assignments?: Record<string, string | number>;
  };
  stats?: ClusterCounters;
}

export interface NodeStats {
  writer?: {
    batches?: number;
    requests?: number;
    max_batch?: number;
    open_now?: number;
    threads?: number;
  };
  reader?: {
    queries?: number;
    rejected_busy?: number;
    timed_out?: number;
    threads?: number;
  };
  http?: {
    requests?: number;
    errors?: number;
    auth_failures?: number;
    authz_refused?: number;
  };
  checkpoint?: {
    passive?: number;
    truncated?: number;
    stalls?: number;
    failures?: number;
    wal_bytes?: number;
  };
}

export interface ShardObservation {
  id: number;
  owner?: string | number | null;
  local_role: "primary" | "replica" | "unassigned" | "unknown";
  epoch: number;
  lsn: number;
  derived?: boolean;
}

export interface ShardContract {
  api_version?: number;
  observed_at_ms?: number;
  node?: string | number | null;
  shards: ShardObservation[];
}

export interface ShardInventoryRow {
  id: number;
  owner?: string | null;
  primary_node?: string | null;
  epoch: number;
  primary_lsn: number;
  replicas: { node: string; epoch: number; lsn: number }[];
  max_lag?: number | null;
  state: "available" | "unavailable" | "incomparable" | "conflict";
  evidence: number;
}

export interface ShardInventory {
  observed_at_ms: number;
  rows: ShardInventoryRow[];
}

export interface SchemaObject {
  type: string;
  name: string;
  table: string;
  sql?: string | null;
}

export interface SchemaTable {
  name: string;
  sql?: string | null;
  columns: unknown[][];
  indexes: unknown[][];
  foreign_keys: unknown[][];
}

export interface SchemaCatalog {
  objects: SchemaObject[];
  tables: SchemaTable[];
  schema_version?: number | null;
  consistency: {
    status: "consistent" | "drifted" | "unknown";
    coverage: "complete" | "partial";
    summary: string;
  };
}

export interface NodeVerification {
  endpoint: string;
  status: "ready" | "not_member" | "wrong_database" | "incompatible" | "unhealthy" | "stabilizing";
  reachable: boolean;
  latency_ms: number;
  node?: string | null;
  version?: string | null;
  api_version?: number | null;
  compatible: boolean;
  member: boolean;
  health: string;
  distribution_stable: boolean;
  guidance: string[];
}

export function conn(name: string) {
  const b = base(name);
  return {
    info: () => req<NodeInfo>("GET", `${b}/info`),
    cluster: () => req<ClusterInfo>("GET", `${b}/cluster`),
    stats: () => req<NodeStats>("GET", `${b}/stats`),
    schema: (shard: number) => req<{ shard: number; schema_version: number }>("GET", `${b}/schema/${shard}`),
    frames: (shard: number) => req<Record<string, unknown>>("GET", `${b}/frames/${shard}`),
    route: (key: string) => req<{ shard: number }>("POST", `${b}/route`, { key }),
    execute: (sql: string, shard = 0, params: unknown[] = []) =>
      req<{ rows_affected: number; last_insert_rowid: number }>("POST", `${b}/execute`, {
        shard,
        sql,
        params,
      }),
    tx: (statements: { sql: string; params?: unknown[] }[], shard = 0) =>
      req<{ rows_affected: number; last_insert_rowid: number }>("POST", `${b}/tx`, {
        shard,
        statements,
      }),
    metrics: () => req<MetricSample[]>("GET", `${b}/metrics`),
    observation: () => req<ClusterObservation>("GET", `${b}/observation`),
    schemaCatalog: () => req<SchemaCatalog>("GET", `${b}/schema-catalog`),
    verifyNode: (endpoint: string) => req<NodeVerification>("POST", `${b}/verify-node`, { endpoint }),
    shardInventory: () => req<ShardInventory>("GET", `${b}/shard-inventory`),
    meshUsers: {
      list: () => req<{ users: { name: string; role: string }[] }>("GET", `${b}/users`),
      create: (name: string, secret: string, role: string) =>
        req("POST", `${b}/users`, { name, secret, role }),
      remove: (name: string) => req("DELETE", `${b}/users/${encodeURIComponent(name)}`),
    },
    query: (
      sql: string,
      opts?: QueryOptions,
    ) =>
      streamQuery(b, sql, opts),
    queryAll: (sql: string, signal?: AbortSignal) => req<MaterializedQueryResult>("POST", `${b}/query_all`, { sql }, signal),
  };
}

export interface QueryResult {
  columns: string[];
  rows: unknown[][];
  truncated: boolean;
}

export interface MaterializedQueryResult {
  columns: string[];
  rows: unknown[][];
}

export interface QueryOptions {
  shard?: number;
  params?: unknown[];
  consistency?: ReadConsistency;
  cap?: number;
  signal?: AbortSignal;
}

export function downloadQuery(
  name: string,
  sql: string,
  opts?: {
    shard?: number;
    params?: unknown[];
    consistency?: ReadConsistency;
    format?: "ndjson" | "csv";
    maxRows?: number | null;
  },
): void {
  if (!csrfToken) throw new Error("session is missing its CSRF token");
  const form = document.createElement("form");
  form.method = "POST";
  form.action = `${base(name)}/query-download`;
  form.style.display = "none";
  for (const [field, value] of [
    ["csrf", csrfToken],
    [
      "payload",
      JSON.stringify({
        shard: opts?.shard ?? 0,
        sql,
        params: opts?.params ?? [],
        consistency: opts?.consistency ?? "linearizable",
        format: opts?.format ?? "ndjson",
        max_rows: opts?.maxRows ?? null,
      }),
    ],
  ]) {
    const input = document.createElement("input");
    input.type = "hidden";
    input.name = field;
    input.value = value;
    form.appendChild(input);
  }
  document.body.appendChild(form);
  form.submit();
  form.remove();
}

/// Stream a query and collect up to `cap` rows (the UI does not render more than that, but the
/// backend and gateway still stream the whole result — this bounds only what the browser holds).
export async function streamQuery(
  b: string,
  sql: string,
  opts?: QueryOptions,
): Promise<QueryResult> {
  const cap = opts?.cap ?? 5000;
  const res = await fetch(`${b}/query`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(csrfToken ? { "X-CSRF-Token": csrfToken } : {}),
    },
    body: JSON.stringify({
      shard: opts?.shard ?? 0,
      sql,
      params: opts?.params ?? [],
      consistency: opts?.consistency ?? "linearizable",
    }),
    credentials: "same-origin",
    signal: opts?.signal,
  });

  if (!res.ok || !res.body) {
    const text = await res.text();
    let msg = res.statusText;
    try {
      msg = JSON.parse(text)?.error ?? msg;
    } catch {
      /* keep statusText */
    }
    throw new ApiError(res.status, msg);
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let columns: string[] = [];
  const rows: unknown[][] = [];
  let truncated = false;

  const handleLine = (line: string) => {
    if (!line) return;
    const obj = JSON.parse(line);
    if (Array.isArray(obj)) {
      if (rows.length < cap) rows.push(obj);
      else truncated = true;
    } else if (obj.columns) {
      columns = obj.columns;
    } else if (obj.error) {
      throw new ApiError(200, obj.error);
    }
  };

  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let nl: number;
    while ((nl = buffer.indexOf("\n")) >= 0) {
      const line = buffer.slice(0, nl).trim();
      buffer = buffer.slice(nl + 1);
      handleLine(line);
    }
    // Once capped, we can stop pulling — the stream is bounded server-side by backpressure.
    if (truncated) {
      await reader.cancel();
      break;
    }
  }
  handleLine(buffer.trim());
  return { columns, rows, truncated };
}
