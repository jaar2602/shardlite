// meshdb HTTP driver — Node 18+ (uses built-in fetch), zero dependencies, streaming reads.
//
//   import { Client } from "./meshdb.mjs";
//   const db = new Client("http://localhost:4680", { user: "app", secret: "s3cret" });
//   for await (const row of db.query("SELECT id, v FROM t WHERE id > ?", { params: [5] }))
//     console.log(row.id, row.v);
//   await db.execute("INSERT INTO t VALUES (?, ?)", { params: [1, "a"] });
//
// query() is an async generator: rows arrive one at a time, so a million-row result costs the
// driver almost nothing, matching the gateway's streaming. Auth is sent as
// `Authorization: Bearer base64(user:secret)` — the programmatic scheme (no browser prompt).
// Over a plaintext gateway the credential is exposed; use TLS on any untrusted network.

export class MeshdbError extends Error {
  constructor(status, message) {
    super(`HTTP ${status}: ${message}`);
    this.name = "MeshdbError";
    this.status = status;
  }
}

export class Client {
  constructor(baseUrl, { user, secret } = {}) {
    this.base = baseUrl.replace(/\/+$/, "");
    this.auth =
      user != null && secret != null
        ? "Bearer " + Buffer.from(`${user}:${secret}`).toString("base64")
        : null;
  }

  async _fetch(method, path, body) {
    const headers = {};
    if (this.auth) headers["Authorization"] = this.auth;
    if (body !== undefined) headers["Content-Type"] = "application/json";
    const res = await fetch(this.base + path, {
      method,
      headers,
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    if (!res.ok) {
      let msg = await res.text();
      try {
        msg = JSON.parse(msg).error ?? msg;
      } catch {}
      throw new MeshdbError(res.status, msg);
    }
    return res;
  }

  async _json(method, path, body) {
    return (await this._fetch(method, path, body)).json();
  }

  // -- reads --

  async *query(sql, { shard = 0, params = [], consistency = "linearizable" } = {}) {
    const res = await this._fetch("POST", "/v1/query", { shard, sql, params, consistency });
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    let columns = null;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      let nl;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, nl).trim();
        buf = buf.slice(nl + 1);
        if (!line) continue;
        const obj = JSON.parse(line);
        if (obj && !Array.isArray(obj) && obj.columns) {
          columns = obj.columns;
          continue;
        }
        if (obj && !Array.isArray(obj) && obj.error) {
          throw new MeshdbError(200, obj.error);
        }
        yield columns ? Object.fromEntries(columns.map((c, i) => [c, obj[i]])) : obj;
      }
    }
  }

  async queryAll(sql) {
    return this._json("POST", "/v1/query_all", { sql });
  }

  async route(key) {
    return (await this._json("POST", "/v1/route", { key })).shard;
  }

  // -- writes --

  async execute(sql, { shard = 0, params = [] } = {}) {
    return this._json("POST", "/v1/execute", { shard, sql, params });
  }

  async tx(statements, { shard = 0 } = {}) {
    const norm = statements.map((s) => (typeof s === "string" ? { sql: s } : s));
    return this._json("POST", "/v1/tx", { shard, statements: norm });
  }

  async executeAll(sql) {
    return this._json("POST", "/v1/execute_all", { sql });
  }

  // -- introspection --

  info() {
    return this._json("GET", "/v1/info");
  }
  cluster() {
    return this._json("GET", "/v1/cluster");
  }
  stats() {
    return this._json("GET", "/v1/stats");
  }
  schema(shard) {
    return this._json("GET", `/v1/schema/${shard}`);
  }
  frames(shard) {
    return this._json("GET", `/v1/frames/${shard}`);
  }

  // -- admin --

  async listUsers() {
    return (await this._json("GET", "/v1/users")).users;
  }
  async createUser(name, secret, role) {
    await this._fetch("POST", "/v1/users", { name, secret, role });
  }
  async dropUser(name) {
    await this._fetch("DELETE", `/v1/users/${name}`);
  }
}
