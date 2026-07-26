# shardlite

A high-availability, multi-write **SQLite** server. Issue arbitrary SQL and define your own
tables; shardlite spreads the data across many single-writer SQLite shards so that several
writers run concurrently — one per shard — without forking SQLite.

## Overview

SQLite serializes writes at the file level: one writer per database file, always. shardlite
does not fight that. Instead it splits the keyspace into many shards, each a separate SQLite
database with a single writer, and — in a cluster — spreads those shards across nodes so that
many nodes accept writes at once. Concurrency for writers comes from sharding, not from
locking tricks, and every shard stays an ordinary SQLite file you can open with the stock
`sqlite3` tool.

What it gives you:

- **Multi-write throughput** via sharding, with self-tuning group commit per shard.
- **A single logical database** — a client connected to any node can run SQL against any
  shard; writes are routed to the owning node and cross-shard reads are gathered from all of
  them.
- **Streaming reads** — large result sets stream row-by-row and never materialise, on the
  server or over the network gateways.
- **Optional edges**: authentication, TLS, an HTTP/JSON gateway, and a JSON-over-TCP gateway,
  each behind a build feature so a minimal deployment compiles none of them.

What it is **not**: there are no cross-shard transactions (a transaction is scoped to one
shard), a cross-shard read is not a consistent snapshot, and a hot single row is a hot single
shard — sharding does not help a workload concentrated on one key.

## Installation

Requires **Rust 1.94 or newer** (edition 2024; the bundled SQLite build needs a recent
toolchain). SQLite itself is vendored — no system libsqlite3 required.

```sh
git clone git@github.com:jaar2602/shardlite.git
cd shardlite
cargo build --release
# binary at target/release/shardlite
```

Or install it onto your PATH:

```sh
cargo install --path .
```

Optional features (off by default):

| Feature | Enables |
|---|---|
| `tls` | TLS transport for the server |
| `http` | HTTP/JSON gateway (`/v1/...`) |
| `json-tcp` | JSON-over-TCP gateway |
| `s3` | S3 archival |

```sh
cargo build --release --features tls,http,json-tcp
```

## Getting started

### 1. Create a database and run SQL

The shard count is fixed at creation and cannot be changed later (it determines how every key
routes), so you choose it up front. 16–64 is typical.

This is the legacy routing model and remains compatible. New clusters can instead start with one
active shard and grow through the experimental dynamic path:

```sh
# Node 1: one active shard; unused local files are created lazily.
shardlite init ./node-1 --listen 10.0.0.1:4600 --initial-shards 1
shardlite serve ./node-1

# Another host: join as non-voting storage, then serve.
shardlite join ./node-2 --seed 10.0.0.1:4600 \
  --node-id 2 --listen 10.0.0.2:4600
shardlite serve ./node-2
```

The leader then splits or transfers one shard at a time to use the new capacity. Dynamic mode is
experimental: online splits support declared text/integer/BLOB keys, composite-key and keyless
rowid tables, and require every current replica to durably acknowledge both shadow installs before
the routing epoch commits. Virtual and `WITHOUT ROWID` tables still fail closed pending differential
proof. See the [dynamic scaling plan](docs/dynamic-scaling-plan.md) and [backlog status](docs/backlog.md).

Dynamic topology crash recovery has a deterministic, process-level qualification suite. It kills
real server processes without running destructors, restarts the same data directories, waits for
automatic catalog/operation repair, and compares complete rows:

```sh
cargo test --features failpoints --test dynamic_crash -- --ignored --nocapture --test-threads=1
```

For the concurrent differential workload (split/transfer, reader validation, replica loss, and
coordinator restart), run:

```sh
cargo test --features failpoints --test dynamic_workload -- --ignored --nocapture --test-threads=1
```

The failpoint feature is excluded from normal production builds. See
[crash recovery qualification](docs/crash-recovery.md) for the coverage and remaining production
fault matrix.

```sh
# create a data directory with 16 shards and run one statement
shardlite ./data --shards 16 -c "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT) STRICT"

# subsequent statements just open the existing directory
shardlite ./data -c "INSERT INTO users VALUES (1, 'ada')"
shardlite ./data -c "SELECT * FROM users"
```

Run a whole file of statements (one per line):

```sh
shardlite ./data -f schema.sql
```

### 2. Interactive shell

```sh
shardlite ./data
```

```
shardlite:0> SELECT count(*) FROM users;
shardlite:0> .tables
shardlite:0> .stats
shardlite:0> .help
shardlite:0> .quit
```

### 3. Run it as a server

```sh
shardlite serve ./data --listen 127.0.0.1:4600
```

Add a users file to require authentication, TLS to encrypt, and gateways for non-Rust clients:

```sh
shardlite serve ./data \
  --listen 127.0.0.1:4600 \
  --users ./users \
  --tls-cert cert.pem --tls-key key.pem \
  --http 127.0.0.1:4680
```

See `shardlite serve --help` for the full flag set, including cluster mode
(`--node-id`, `--peers`) for spreading shards across hosts.

### 4. Connect from another language

With the HTTP or JSON-TCP gateway enabled, drivers for Python, JavaScript, Go and Rust live
under [`drivers/`](drivers/) — each dependency-light and streaming.

## License

[MIT](LICENSE) © 2026 jaar2602
