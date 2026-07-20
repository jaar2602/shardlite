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

// -- Persistent TCP transport (JSON-over-TCP) --

import net from "node:net";

class FrameReader {
  constructor(socket) {
    this.buf = Buffer.alloc(0);
    this.frames = [];
    this.waiters = [];
    this.err = null;
    socket.on("data", (d) => {
      this.buf = Buffer.concat([this.buf, d]);
      while (this.buf.length >= 4) {
        const n = this.buf.readUInt32BE(0);
        if (this.buf.length < 4 + n) break;
        const frame = JSON.parse(this.buf.subarray(4, 4 + n).toString());
        this.buf = this.buf.subarray(4 + n);
        if (this.waiters.length) this.waiters.shift().resolve(frame);
        else this.frames.push(frame);
      }
    });
    const fail = (e) => {
      this.err = this.err || e || new Error("connection closed");
      while (this.waiters.length) this.waiters.shift().reject(this.err);
    };
    socket.on("error", fail);
    socket.on("close", () => fail(null));
  }
  nextFrame() {
    if (this.frames.length) return Promise.resolve(this.frames.shift());
    if (this.err) return Promise.reject(this.err);
    return new Promise((resolve, reject) => this.waiters.push({ resolve, reject }));
  }
}

/// A persistent-connection client over meshdb's JSON-over-TCP protocol. Lower per-request
/// overhead than HTTP; one request at a time per connection (not shared across concurrent
/// callers). query() streams. Auth is sent once at connect; the secret crosses the wire, so
/// use a trusted network or a TLS tunnel.
///
///     const db = await TcpClient.connect("127.0.0.1", 4620, { user: "app", secret: "s3cret" });
///     for await (const row of db.query("SELECT id, v FROM t")) { ... }
///     db.close();
export class TcpClient {
  static async connect(host, port, { user, secret } = {}) {
    const socket = net.connect({ host, port });
    socket.setNoDelay(true);
    await new Promise((res, rej) => {
      socket.once("connect", res);
      socket.once("error", rej);
    });
    const c = new TcpClient(socket);
    if (user != null && secret != null) {
      const r = await c._call({ op: "auth", name: user, secret });
      if (!r.ok) throw new MeshdbError(401, "authentication failed");
    }
    return c;
  }

  constructor(socket) {
    this.socket = socket;
    this.reader = new FrameReader(socket);
  }

  close() {
    this.socket.end();
  }

  _send(frame) {
    const body = Buffer.from(JSON.stringify(frame));
    const header = Buffer.alloc(4);
    header.writeUInt32BE(body.length, 0);
    this.socket.write(Buffer.concat([header, body]));
  }

  async _call(frame) {
    this._send(frame);
    const r = await this.reader.nextFrame();
    if (r.error) throw new MeshdbError(r.status ?? 0, r.error);
    return r.result;
  }

  async *query(sql, { shard = 0, params = [], consistency = "linearizable" } = {}) {
    this._send({ op: "query", shard, sql, params, consistency });
    let columns = null;
    for (;;) {
      const f = await this.reader.nextFrame();
      if (f.columns) columns = f.columns;
      else if (f.row) yield columns ? Object.fromEntries(columns.map((c, i) => [c, f.row[i]])) : f.row;
      else if (f.end) return;
      else if (f.error) throw new MeshdbError(f.status ?? 200, f.error);
    }
  }

  queryAll(sql) { return this._call({ op: "query_all", sql }); }
  async route(key) { return (await this._call({ op: "route", key })).shard; }
  execute(sql, { shard = 0, params = [] } = {}) { return this._call({ op: "execute", shard, sql, params }); }
  tx(statements, { shard = 0 } = {}) {
    const norm = statements.map((s) => (typeof s === "string" ? { sql: s } : s));
    return this._call({ op: "tx", shard, statements: norm });
  }
  executeAll(sql) { return this._call({ op: "execute_all", sql }); }
  info() { return this._call({ op: "info" }); }
  cluster() { return this._call({ op: "cluster" }); }
  stats() { return this._call({ op: "stats" }); }
  schema(shard) { return this._call({ op: "schema", shard }); }
  frames(shard) { return this._call({ op: "frames", shard }); }
  async listUsers() { return (await this._call({ op: "list_users" })).users; }
  createUser(name, secret, role) { return this._call({ op: "create_user", name, secret, role }); }
  dropUser(name) { return this._call({ op: "drop_user", name }); }
}
