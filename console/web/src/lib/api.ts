// Typed client for the console's own backend API. Same origin, so the session cookie rides along
// automatically. Everything the SPA does goes through here; nothing talks to meshdb directly.

export type Role = "admin" | "user";

export interface Me {
  user: string;
  role: Role;
}

export interface Connection {
  name: string;
  url: string;
  meshdb_user?: string | null;
}

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function req<T>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(path, {
    method,
    headers: body === undefined ? {} : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
    credentials: "same-origin",
  });
  const text = await res.text();
  const data = text ? JSON.parse(text) : {};
  if (!res.ok) {
    throw new ApiError(res.status, data?.error ?? res.statusText);
  }
  return data as T;
}

// --- console auth ---
export const me = () => req<Me>("GET", "/api/me");
export const login = (username: string, password: string) =>
  req<Me>("POST", "/api/login", { username, password });
export const logout = () => req<{ ok: boolean }>("POST", "/api/logout");

// --- console users (admin) ---
export const consoleUsers = {
  list: () => req<{ name: string; role: Role }[]>("GET", "/api/console-users"),
  create: (username: string, password: string, role: Role) =>
    req("POST", "/api/console-users", { username, password, role }),
  remove: (name: string) => req("DELETE", `/api/console-users/${encodeURIComponent(name)}`),
};

// --- connections ---
export const connections = {
  list: () => req<Connection[]>("GET", "/api/connections"),
  create: (c: {
    name: string;
    url: string;
    meshdb_user?: string;
    meshdb_secret?: string;
    replace?: boolean;
  }) => req("POST", "/api/connections", c),
  remove: (name: string) => req("DELETE", `/api/connections/${encodeURIComponent(name)}`),
};

// --- per-connection proxy to meshdb /v1 ---
function base(name: string) {
  return `/api/connections/${encodeURIComponent(name)}`;
}

export interface MetricSample {
  t: number;
  stats: Record<string, unknown>;
}

export function conn(name: string) {
  const b = base(name);
  return {
    info: () => req<Record<string, unknown>>("GET", `${b}/info`),
    cluster: () => req<Record<string, unknown>>("GET", `${b}/cluster`),
    stats: () => req<Record<string, unknown>>("GET", `${b}/stats`),
    schema: (shard: number) => req<Record<string, unknown>>("GET", `${b}/schema/${shard}`),
    frames: (shard: number) => req<Record<string, unknown>>("GET", `${b}/frames/${shard}`),
    route: (key: string) => req<{ shard: number }>("POST", `${b}/route`, { key }),
    execute: (sql: string, shard = 0, params: unknown[] = []) =>
      req<{ rows_affected: number; last_insert_rowid: number }>("POST", `${b}/execute`, {
        shard,
        sql,
        params,
      }),
    executeAll: (sql: string) => req<Record<string, unknown>>("POST", `${b}/execute_all`, { sql }),
    tx: (statements: { sql: string; params?: unknown[] }[], shard = 0) =>
      req<{ rows_affected: number; last_insert_rowid: number }>("POST", `${b}/tx`, {
        shard,
        statements,
      }),
    metrics: () => req<MetricSample[]>("GET", `${b}/metrics`),
    meshUsers: {
      list: () => req<{ users: { name: string; role: string }[] }>("GET", `${b}/users`),
      create: (name: string, secret: string, role: string) =>
        req("POST", `${b}/users`, { name, secret, role }),
      remove: (name: string) => req("DELETE", `${b}/users/${encodeURIComponent(name)}`),
    },
    query: (sql: string, opts?: { shard?: number; params?: unknown[]; consistency?: string }) =>
      streamQuery(b, sql, opts),
  };
}

export interface QueryResult {
  columns: string[];
  rows: unknown[][];
  truncated: boolean;
}

/// Stream a query and collect up to `cap` rows (the UI does not render more than that, but the
/// backend and gateway still stream the whole result — this bounds only what the browser holds).
export async function streamQuery(
  b: string,
  sql: string,
  opts?: { shard?: number; params?: unknown[]; consistency?: string; cap?: number },
): Promise<QueryResult> {
  const cap = opts?.cap ?? 5000;
  const res = await fetch(`${b}/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      shard: opts?.shard ?? 0,
      sql,
      params: opts?.params ?? [],
      consistency: opts?.consistency ?? "linearizable",
    }),
    credentials: "same-origin",
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
