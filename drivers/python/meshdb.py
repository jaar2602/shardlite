"""meshdb HTTP driver — pure standard library, streaming reads.

A thin client over the meshdb HTTP gateway (`meshdb serve --http ADDR`). No third-party
dependencies: it uses urllib. Queries stream — `query()` is a generator that yields one row
at a time, so a million-row result costs the driver almost nothing, matching the gateway.

    from meshdb import Client
    db = Client("http://localhost:4680", user="app", secret="s3cret")
    for row in db.query("SELECT id, v FROM t WHERE id > ?", params=[5]):
        print(row["id"], row["v"])
    db.execute("INSERT INTO t VALUES (?, ?)", params=[1, "a"])

Auth is sent as `Authorization: Bearer base64(user:secret)` — the programmatic scheme, which
does not trigger a browser login prompt. Over a plaintext gateway the credential is exposed;
run the gateway behind TLS on any untrusted network.
"""

import base64
import json
import urllib.error
import urllib.request


class MeshdbError(Exception):
    def __init__(self, status, message):
        super().__init__(f"HTTP {status}: {message}")
        self.status = status
        self.message = message


class Client:
    def __init__(self, base_url, user=None, secret=None, timeout=30):
        self.base = base_url.rstrip("/")
        self.timeout = timeout
        self._auth = None
        if user is not None and secret is not None:
            token = base64.b64encode(f"{user}:{secret}".encode()).decode()
            self._auth = f"Bearer {token}"

    # -- transport --

    def _open(self, method, path, body=None):
        headers = {}
        if self._auth:
            headers["Authorization"] = self._auth
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(self.base + path, data=data, method=method, headers=headers)
        try:
            return urllib.request.urlopen(req, timeout=self.timeout)
        except urllib.error.HTTPError as e:
            raw = e.read().decode(errors="replace")
            msg = raw
            try:
                msg = json.loads(raw).get("error", raw)
            except Exception:
                pass
            raise MeshdbError(e.code, msg) from None

    def _json(self, method, path, body=None):
        with self._open(method, path, body) as resp:
            return json.loads(resp.read())

    # -- reads --

    def query(self, sql, shard=0, params=None, consistency="linearizable"):
        """Stream a read. Yields a dict per row, lazily — the whole result is never held."""
        body = {"shard": shard, "sql": sql, "params": params or [], "consistency": consistency}
        resp = self._open("POST", "/v1/query", body)
        columns = None
        with resp:
            for raw in resp:  # HTTPResponse iterates lines as they arrive
                line = raw.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if isinstance(obj, dict) and "columns" in obj:
                    columns = obj["columns"]
                    continue
                if isinstance(obj, dict) and "error" in obj:
                    # An error after the 200 header, reported as a trailing object.
                    raise MeshdbError(200, obj["error"])
                yield dict(zip(columns, obj)) if columns else obj

    def query_all(self, sql):
        return self._json("POST", "/v1/query_all", {"sql": sql})

    def route(self, key):
        return self._json("POST", "/v1/route", {"key": key})["shard"]

    # -- writes --

    def execute(self, sql, shard=0, params=None):
        return self._json("POST", "/v1/execute", {"shard": shard, "sql": sql, "params": params or []})

    def tx(self, statements, shard=0):
        """Atomic, durable transaction. `statements` is a list of str, (sql, params), or dict."""
        norm = []
        for s in statements:
            if isinstance(s, str):
                norm.append({"sql": s})
            elif isinstance(s, dict):
                norm.append(s)
            else:
                norm.append({"sql": s[0], "params": list(s[1])})
        return self._json("POST", "/v1/tx", {"shard": shard, "statements": norm})

    def execute_all(self, sql):
        return self._json("POST", "/v1/execute_all", {"sql": sql})

    # -- introspection --

    def info(self):
        return self._json("GET", "/v1/info")

    def cluster(self):
        return self._json("GET", "/v1/cluster")

    def stats(self):
        return self._json("GET", "/v1/stats")

    def schema(self, shard):
        return self._json("GET", f"/v1/schema/{shard}")

    def frames(self, shard):
        return self._json("GET", f"/v1/frames/{shard}")

    # -- admin --

    def list_users(self):
        return self._json("GET", "/v1/users")["users"]

    def create_user(self, name, secret, role):
        self._open("POST", "/v1/users", {"name": name, "secret": secret, "role": role}).close()

    def drop_user(self, name):
        self._open("DELETE", f"/v1/users/{name}").close()


# -- Persistent TCP transport (JSON-over-TCP) --

import socket
import struct


class TcpClient:
    """A persistent-connection client over meshdb's JSON-over-TCP protocol.

    Lower per-request overhead than HTTP (no headers, one held socket). One request at a time
    per connection — not safe to share across threads. `query()` streams rows like the HTTP
    client. Auth (name/secret) is sent once at connect; the secret crosses the wire, so run
    over a trusted network or a TLS tunnel.

        db = TcpClient("127.0.0.1", 4620, user="app", secret="s3cret")
        for row in db.query("SELECT id, v FROM t"):
            ...
    """

    def __init__(self, host, port, user=None, secret=None, timeout=30):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._buf = self.sock.makefile("rb")
        if user is not None and secret is not None:
            r = self._call({"op": "auth", "name": user, "secret": secret})
            if not r.get("ok"):
                raise MeshdbError(401, "authentication failed")

    def close(self):
        try:
            self.sock.close()
        finally:
            self._buf.close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()

    def _send(self, frame):
        body = json.dumps(frame).encode()
        self.sock.sendall(struct.pack(">I", len(body)) + body)

    def _recv(self):
        header = self._buf.read(4)
        if len(header) < 4:
            raise MeshdbError(0, "connection closed")
        (n,) = struct.unpack(">I", header)
        return json.loads(self._buf.read(n))

    def _call(self, frame):
        """A bounded op: one request, one result frame."""
        self._send(frame)
        r = self._recv()
        if "error" in r:
            raise MeshdbError(r.get("status", 0), r["error"])
        return r["result"]

    # -- reads --

    def query(self, sql, shard=0, params=None, consistency="linearizable"):
        self._send({"op": "query", "shard": shard, "sql": sql,
                    "params": params or [], "consistency": consistency})
        columns = None
        while True:
            f = self._recv()
            if "columns" in f:
                columns = f["columns"]
            elif "row" in f:
                yield dict(zip(columns, f["row"])) if columns else f["row"]
            elif "end" in f:
                return
            elif "error" in f:
                raise MeshdbError(f.get("status", 200), f["error"])

    def query_all(self, sql):
        return self._call({"op": "query_all", "sql": sql})

    def route(self, key):
        return self._call({"op": "route", "key": key})["shard"]

    # -- writes --

    def execute(self, sql, shard=0, params=None):
        return self._call({"op": "execute", "shard": shard, "sql": sql, "params": params or []})

    def tx(self, statements, shard=0):
        norm = []
        for s in statements:
            if isinstance(s, str):
                norm.append({"sql": s})
            elif isinstance(s, dict):
                norm.append(s)
            else:
                norm.append({"sql": s[0], "params": list(s[1])})
        return self._call({"op": "tx", "shard": shard, "statements": norm})

    def execute_all(self, sql):
        return self._call({"op": "execute_all", "sql": sql})

    # -- introspection & admin --

    def info(self):
        return self._call({"op": "info"})

    def cluster(self):
        return self._call({"op": "cluster"})

    def stats(self):
        return self._call({"op": "stats"})

    def schema(self, shard):
        return self._call({"op": "schema", "shard": shard})

    def frames(self, shard):
        return self._call({"op": "frames", "shard": shard})

    def list_users(self):
        return self._call({"op": "list_users"})["users"]

    def create_user(self, name, secret, role):
        self._call({"op": "create_user", "name": name, "secret": secret, "role": role})

    def drop_user(self, name):
        self._call({"op": "drop_user", "name": name})
