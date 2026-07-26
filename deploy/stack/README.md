# shardlite full-stack deployment

One command brings up the whole app on Docker — a **3-node HA cluster** and the **web console** —
with every piece of state on a **Docker volume**, so it survives restarts.

The packaged demo intentionally uses a legacy fixed-shard (12-shard) topology so it starts
deterministically on a clean Docker host. It is not the dynamic-scaling qualification environment:
`--shards` is a creation-time setting for these services. Exercise dynamic joins, splits, capacity
weights, and fenced movement with `shardlite init`/`shardlite join` directories instead.

```sh
cd deploy/stack
./shardlite-stack up
```

First run builds two images (the shardlite node and the console, frontend + backend), starts four
containers, waits for the console to come up, and registers the cluster as a console connection.
When it finishes it prints the console URL and the generated admin credentials.

Open **http://localhost:7100**, sign in as `admin` with the printed password, and the `shardlite`
connection is already there to browse, query, and point the AI assistant at.

## Commands

| Command | Does |
|---|---|
| `./shardlite-stack up` | Build (first run) and start everything; register the connection. |
| `./shardlite-stack down` | Stop the stack. **Keeps all data** on the volumes. |
| `./shardlite-stack restart` | Restart the running services. |
| `./shardlite-stack status` | Container and health status. |
| `./shardlite-stack logs [svc]` | Follow logs — all services, or one (`node1`/`node2`/`node3`/`console`). |
| `./shardlite-stack register` | Re-register the cluster connection (idempotent). |
| `./shardlite-stack destroy` | Stop **and delete all volumes + secrets** (asks for confirmation). |
| `./verify.sh` | Read-only smoke check for compose config, node/console health, and dynamic catalog policy. |

## What persists, and how

| Volume | Holds |
|---|---|
| `node1-data` / `node2-data` / `node3-data` | Each node's shard databases and Raft state. |
| `console-data` | Console users, saved connections, and the sealed AI settings. |

`down` + `up` keeps all of it. Only `destroy` removes the volumes.

### Secrets (`.env`, generated on first `up`)

`shardlite-stack` writes `SHARDLITE_CONSOLE_KEY` and `SHARDLITE_CONSOLE_ADMIN_PASSWORD` to `.env` (mode 600)
once, then reuses them. **`SHARDLITE_CONSOLE_KEY` must stay constant** — it encrypts the console's
stored secrets at rest, so a changed key makes saved connections and the AI key undecryptable. That
is why the key is pinned in a file rather than regenerated each run. `.env` is git-ignored; keep it
safe, and `destroy` deletes it.

## Ports

| Host | Service |
|---|---|
| `7100` | Console (web UI + API) |
| `8081` / `8082` / `8083` | Cluster nodes' HTTP gateway (node1/2/3) |

Inside the Docker network the console reaches the nodes at `node1:8080`, `node2:8080`, `node3:8080`
— which is why the registered connection uses those addresses, not the host ports.

## Notes

- This runs the nodes with `--http-insecure` (plaintext gateway) on a private Docker network, as a
  self-contained deployment. For anything exposed beyond the host, terminate TLS in front of the
  console and the nodes.
- To change the number of shards or nodes, edit `docker-compose.yml`. **Shard count is fixed at
  cluster creation** — changing `--shards` only takes effect on a fresh cluster (after `destroy`).
