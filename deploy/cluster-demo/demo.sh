#!/usr/bin/env bash
#
# Bring up the 3-node meshdb cluster and prove a client can run SQL against ANY node without
# knowing the data is sharded across all three. Every request below goes over the HTTP gateway
# to a DIFFERENT node in rotation, yet the answers are consistent — the nodes route to each other.
#
# Requires: docker (with the compose plugin), curl, python3.
set -euo pipefail
cd "$(dirname "$0")"

# Host ports mapped to node1/node2/node3's HTTP gateways (see docker-compose.yml).
PORTS=(8081 8082 8083)

run() { # <host-port> <sql>
  local body
  body=$(python3 -c 'import json,sys; print(json.dumps({"sql": sys.argv[1]}))' "$2")
  curl -s -X POST "http://127.0.0.1:$1/v1/run" -H 'content-type: application/json' -d "$body"
  echo
}

echo "==> building the image (once) and starting 3 nodes"
docker compose build
docker compose up -d

echo "==> waiting for the gateways to answer"
for _ in $(seq 1 90); do
  curl -sf "http://127.0.0.1:${PORTS[0]}/v1/health" >/dev/null 2>&1 && break
  sleep 1
done

echo "==> waiting for the cluster to elect a leader and place shards"
sleep 6

echo
echo "# CREATE TABLE via node 1 — its PRIMARY KEY becomes the shard key, no extra step"
run "${PORTS[0]}" "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT) STRICT"

echo
echo "# insert 90 rows, rotating which node we talk to (node 1, 2, 3, 1, 2, 3, ...)"
for i in $(seq 1 90); do
  run "${PORTS[$(( i % 3 ))]}" "INSERT INTO users (id, name) VALUES ('user-$i', 'name-$i')" >/dev/null
done

echo
echo "# every node reports all 90 rows — a cross-shard read gathered from all three:"
for i in 0 1 2; do
  printf "  node %s: " "$(( i + 1 ))"
  run "${PORTS[$i]}" "SELECT count(*) FROM users"
done

echo
echo "# a point lookup asked of node 3 — routed to whichever node owns the key's shard:"
run "${PORTS[2]}" "SELECT name FROM users WHERE id = 'user-42'"

echo
echo "# a grouped aggregate, two-phase-merged across nodes:"
run "${PORTS[1]}" "SELECT substr(id, 1, 6) AS bucket, count(*) FROM users GROUP BY substr(id, 1, 6)"

echo
echo "cluster is live. Poke it directly, e.g.:"
echo "  curl -s localhost:8082/v1/run -d '{\"sql\":\"SELECT count(*) FROM users\"}'"
echo "Tear it down with:  docker compose -f \"$(pwd)/docker-compose.yml\" down"
