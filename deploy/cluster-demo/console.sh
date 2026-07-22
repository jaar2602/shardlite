#!/usr/bin/env bash
#
# Start the meshdb **console** (the web UI) pointed at the 3-node Docker cluster from this demo.
# It brings the cluster up if needed, launches the console backend, and pre-registers the cluster
# as a connection so you can log in and immediately browse/query it — no manual setup.
#
# Requires: docker (compose plugin), curl, python3. The console binary is built on first run
# (needs npm + cargo) unless one already exists.
set -euo pipefail
cd "$(dirname "$0")"

CONSOLE_DIR=../../console
CONSOLE_ADDR=127.0.0.1:7100
CONSOLE_KEY="demo-master-key"          # encrypts stored secrets (demo value)
ADMIN_USER=admin
ADMIN_PASS=admin-demo-pass
DATA_DIR="$PWD/console-data"           # fresh each run for a predictable demo
NODE_URLS='["http://127.0.0.1:8081","http://127.0.0.1:8082","http://127.0.0.1:8083"]'

# --- 1. cluster up ---------------------------------------------------------------------------
if ! curl -sf "http://127.0.0.1:8081/v1/health" >/dev/null 2>&1; then
  echo "==> cluster not responding; starting it (docker compose)"
  docker compose build
  docker compose up -d
  for _ in $(seq 1 90); do
    curl -sf "http://127.0.0.1:8081/v1/health" >/dev/null 2>&1 && break
    sleep 1
  done
  echo "==> giving the cluster a moment to elect a leader and place shards"
  sleep 6
else
  echo "==> cluster already up on :8081-:8083"
fi

# --- 2. console binary -----------------------------------------------------------------------
BIN="$CONSOLE_DIR/server/target/release/meshdb-console"
[ -x "$BIN" ] || BIN="$CONSOLE_DIR/server/target/debug/meshdb-console"
if [ ! -x "$BIN" ]; then
  echo "==> no console binary found; building it (frontend + backend)"
  ( cd "$CONSOLE_DIR" && ./build.sh )
  BIN="$CONSOLE_DIR/server/target/debug/meshdb-console"
fi

# --- 3. start the console backend ------------------------------------------------------------
rm -rf "$DATA_DIR"; mkdir -p "$DATA_DIR"
echo "==> starting console at http://$CONSOLE_ADDR"
MESHDB_CONSOLE_KEY="$CONSOLE_KEY" \
MESHDB_CONSOLE_ADMIN="$ADMIN_USER" \
MESHDB_CONSOLE_ADMIN_PASSWORD="$ADMIN_PASS" \
  "$BIN" --listen "$CONSOLE_ADDR" --data "$DATA_DIR" &
CONSOLE_PID=$!
cleanup(){ kill "$CONSOLE_PID" 2>/dev/null || true; }
trap cleanup EXIT

for _ in $(seq 1 60); do
  curl -sf "http://$CONSOLE_ADDR/api/health" >/dev/null 2>&1 && break
  curl -s "http://$CONSOLE_ADDR/" >/dev/null 2>&1 && break
  sleep 0.5
done

# --- 4. log in and pre-register the cluster connection ---------------------------------------
JAR=$(mktemp)
LOGIN=$(curl -s -c "$JAR" -X POST "http://$CONSOLE_ADDR/api/login" \
  -H 'content-type: application/json' \
  -d "{\"username\":\"$ADMIN_USER\",\"password\":\"$ADMIN_PASS\"}")
CSRF=$(printf '%s' "$LOGIN" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("csrf_token",""))' 2>/dev/null || true)

if [ -n "$CSRF" ]; then
  echo "==> registering the Docker cluster as a console connection"
  curl -s -b "$JAR" -X POST "http://$CONSOLE_ADDR/api/connections" \
    -H 'content-type: application/json' -H "X-CSRF-Token: $CSRF" \
    -d "{\"name\":\"docker-cluster\",\"seeds\":$NODE_URLS,\"allow_insecure_http\":true}" \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); print("   ->", json.dumps(d)[:200])' 2>/dev/null \
    || echo "   (connection POST returned non-JSON; add it in the UI if it is missing)"
else
  echo "==> could not log in automatically; add the connection in the UI (details below)"
fi

cat <<EOF

============================================================
 meshdb console is running:  http://$CONSOLE_ADDR
   sign in:   user  $ADMIN_USER
              pass  $ADMIN_PASS
   connection "docker-cluster" points at the 3 nodes:
     http://127.0.0.1:8081  http://127.0.0.1:8082  http://127.0.0.1:8083
============================================================

Press Ctrl-C to stop the console (the Docker cluster keeps running;
tear it down with:  docker compose -f "$PWD/docker-compose.yml" down).
EOF

wait "$CONSOLE_PID"
