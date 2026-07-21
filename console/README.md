# meshdb console

A standalone web app for managing and observing meshdb clusters — the console's own login and
state, managing many clusters the way a database client manages many connections. It is
deliberately **not** part of the database binary: keeping it out of the 150 MB / 0.33 CPU
database avoids adding a browser-facing surface and request load there, and a standalone backend
means its own login and no CORS (the browser only ever talks to the console backend).

See `../docs/console-plan.md` for the design and the confirmed scope.

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
over endpoints meshdb already exposes; the console adds no database-core changes. The one thing
the database does not provide — a *history* of stats for charts — the backend supplies by
sampling `/v1/stats` into a bounded in-memory ring.

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
  stores only ciphertext; the file alone grants no cluster access.
- Sessions are in-memory: a restart signs everyone out (nothing to invalidate on disk).
- Put the console behind TLS on any untrusted network. Console passwords and, on the way to a
  cluster, meshdb secrets cross these connections.

## Frontend development

`npm --prefix web run dev` serves the SPA with hot reload and proxies `/api` to a console backend
on `127.0.0.1:7100`, so run the backend alongside it.

## Verify

`../scripts/console_smoke.sh` builds everything, starts a meshdb gateway and the console, and
drives the whole API end to end (login, multi-user roles, encrypted-at-rest secrets, the streaming
proxy over a large result, and the embedded SPA).
