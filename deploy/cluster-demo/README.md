# meshdb cluster demo (3 nodes, containers)

Spins up **three meshdb nodes**, each in its own container, forming one cluster on a shared
Docker network. It demonstrates the point of the whole design: **a client connects to any node,
runs plain SQL, and never knows the data is sharded across all three.**

## What it shows

- 12 shards are spread across the 3 nodes automatically — placement round-robins by node id, so
  each node owns 4 shards (single-copy; this demo is placement-only, not data-replicated).
- `CREATE TABLE users (id TEXT PRIMARY KEY, …)` — the **primary key becomes the shard key**, with
  no extra declaration.
- Writes and reads are issued against **different nodes in rotation**, yet stay consistent:
  - an `INSERT` is routed to the node that owns the row's shard,
  - a point `SELECT … WHERE id = …` is routed to the one node that holds it,
  - a `SELECT count(*)` / `GROUP BY` is fanned out and merged across **all** nodes.

## Run it

Requires `docker` (with the compose plugin), plus `curl` and `python3` on the host for the demo
script.

```sh
cd deploy/cluster-demo
./demo.sh
```

The script builds the image, starts `node1`/`node2`/`node3`, waits for a leader to be elected and
shards to be placed, then runs the SQL walkthrough. Each node's HTTP gateway is published on the
host: node1 → `localhost:8081`, node2 → `8082`, node3 → `8083`.

Try it yourself against any node — the answer is the same everywhere:

```sh
curl -s localhost:8083/v1/run -d '{"sql":"SELECT count(*) FROM users"}'
```

## Web console

`./console.sh` starts the meshdb **console** (the web UI) pointed at this cluster. It brings the
cluster up if needed, launches the console backend on `localhost:7100`, and pre-registers the
cluster as a connection, so you just sign in and browse/query it:

```sh
./console.sh
# then open http://127.0.0.1:7100  (user: admin, pass: admin-demo-pass)
```

The console reaches the cluster over the same HTTP `/v1` edge — its connection test reports
`forwarding: true` and `shard_count: 12`, i.e. it sees the real 3-node cluster. Ctrl-C stops the
console; the cluster keeps running. (The console is built on first run and needs `npm` + `cargo`.)

Tear down:

```sh
docker compose down
```

## How the nodes find each other

Each node is started with (see `docker-compose.yml`):

```
meshdb serve /data --shards 12 \
  --listen 0.0.0.0:4600 --http 0.0.0.0:8080 --http-insecure \
  --node-id <N> --peers <the other two as id=host:4600>
```

- `--node-id` turns on cluster mode; `--peers` is the static list of the other members and their
  addresses (Docker DNS resolves `node2`, `node3`, …). There is **one port per node** (`4600`) for
  both client and intra-cluster traffic, by design.
- A node forwards any shard it does not own to that shard's owner; the owner runs it locally. This
  is the same routing the in-process test (`tests/cross_node.rs`) exercises, here over real TCP.

## Caveats (this is a demo)

- **Placement-only, no data HA.** Each shard lives on a single owner; there are no follower
  replicas and writes are acknowledged locally (no quorum wait). Lose a node and its shards are
  unavailable until it returns. Data-level HA (replicas + quorum ack) is a further step.
- **No security.** `--http-insecure` serves a plaintext gateway with no auth. For anything real,
  put TLS and `--users` (see `meshdb serve --help`) in front.
- Containers use ephemeral `/data`; a full teardown starts fresh.
