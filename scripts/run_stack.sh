#!/usr/bin/env bash
#
# Run a local meshdb gateway and the web console together, wired up so the console already has a
# "local" connection pointing at the gateway. Builds whatever is missing, then supervises both;
# Ctrl-C stops both cleanly.
#
# Everything is env-overridable (defaults shown):
#   MESHDB_STACK_DIR=.stack          where both keep their data
#   MESHDB_HTTP=127.0.0.1:4680       gateway HTTP /v1 edge (what the console talks to)
#   MESHDB_NATIVE=127.0.0.1:4620     gateway native TCP edge
#   MESHDB_SHARDS=4                  shard count when the data dir is first created
#   CONSOLE_ADDR=127.0.0.1:7100      console web address
#   CONSOLE_ADMIN=admin / CONSOLE_ADMIN_PASSWORD=admin    bootstrap console login
#   MESHDB_CONSOLE_KEY=<dev default> master passphrase encrypting stored connection secrets
#
# Auth: by default the local gateway runs WITHOUT meshdb auth (fine for localhost). Set both
# MESHDB_USER and MESHDB_SECRET to run it WITH auth — the script provisions that admin user,
# starts the gateway with auth, and seeds the console connection with the (sealed) credential.
set -uo pipefail
cd "$(dirname "$0")/.."

STACK_DIR="${MESHDB_STACK_DIR:-.stack}"
MESHDB_HTTP="${MESHDB_HTTP:-127.0.0.1:4680}"
MESHDB_NATIVE="${MESHDB_NATIVE:-127.0.0.1:4620}"
SHARDS="${MESHDB_SHARDS:-4}"
CONSOLE_ADDR="${CONSOLE_ADDR:-127.0.0.1:7100}"
CONSOLE_ADMIN="${CONSOLE_ADMIN:-admin}"
CONSOLE_ADMIN_PASSWORD="${CONSOLE_ADMIN_PASSWORD:-admin}"
export MESHDB_CONSOLE_KEY="${MESHDB_CONSOLE_KEY:-dev-master-key-change-me}"
MESHDB_USER="${MESHDB_USER:-}"
MESHDB_SECRET="${MESHDB_SECRET:-}"

MESHDB_BIN=target/debug/meshdb
CONSOLE_BIN=console/server/target/debug/meshdb-console
DB_DIR="$STACK_DIR/db"
CONSOLE_DATA="$STACK_DIR/console"
USERS_FILE="$STACK_DIR/meshdb-users.json"

MDB="" CON=""
cleanup() {
  trap - INT TERM EXIT   # disarm so this runs once, not again on the EXIT that follows
  echo
  echo "stopping…"
  # Signal both the tracked PIDs and any direct child (belt and suspenders — a re-exec or an
  # intermediate shell can make the tracked PID stale), then SIGKILL whatever is still up.
  kill "$CON" "$MDB" 2>/dev/null
  pkill -P $$ 2>/dev/null
  for _ in $(seq 1 15); do
    { [ -n "$CON" ] && kill -0 "$CON" 2>/dev/null; } || { [ -n "$MDB" ] && kill -0 "$MDB" 2>/dev/null; } || break
    sleep 0.2
  done
  kill -9 "$CON" "$MDB" 2>/dev/null
  pkill -9 -P $$ 2>/dev/null
  wait 2>/dev/null
}
trap cleanup INT TERM EXIT

# --- build what's missing ---
if [ ! -x "$MESHDB_BIN" ] || ! "$MESHDB_BIN" serve --help >/dev/null 2>&1; then
  echo "building meshdb (--features http)…"
  cargo build --features http || { echo "meshdb build failed"; exit 1; }
fi
if [ ! -x "$CONSOLE_BIN" ]; then
  echo "building console (frontend + backend)…"
  ( cd console && ./build.sh ) || { echo "console build failed — see console/README.md"; exit 1; }
fi

mkdir -p "$STACK_DIR"

# --- provision meshdb auth if requested ---
SERVE_AUTH_ARGS=()
if [ -n "$MESHDB_USER" ] && [ -n "$MESHDB_SECRET" ]; then
  if [ ! -f "$USERS_FILE" ]; then
    "$MESHDB_BIN" user add "$MESHDB_USER" "$MESHDB_SECRET" --role admin --users "$USERS_FILE" >/dev/null
  fi
  # auth without TLS is refused unless explicitly acknowledged — fine on localhost.
  SERVE_AUTH_ARGS=(--users "$USERS_FILE" --http-insecure)
  AUTH_NOTE="on (user: $MESHDB_USER)"
else
  AUTH_NOTE="off (local, no meshdb auth)"
fi

# --- initialise the data dir on first run (shard count is fixed at creation) ---
if [ ! -e "$DB_DIR" ]; then
  echo "initialising meshdb data dir ($SHARDS shards)…"
  "$MESHDB_BIN" "$DB_DIR" --shards "$SHARDS" -c "SELECT 1" >/dev/null || { echo "init failed"; exit 1; }
fi

# --- start meshdb ---
"$MESHDB_BIN" serve "$DB_DIR" --listen "$MESHDB_NATIVE" --http "$MESHDB_HTTP" "${SERVE_AUTH_ARGS[@]}" &
MDB=$!

# --- start the console ---
MESHDB_CONSOLE_ADMIN="$CONSOLE_ADMIN" \
MESHDB_CONSOLE_ADMIN_PASSWORD="$CONSOLE_ADMIN_PASSWORD" \
  "$CONSOLE_BIN" --listen "$CONSOLE_ADDR" --data "$CONSOLE_DATA" &
CON=$!

# --- wait for both to accept connections ---
wait_up() { for _ in $(seq 1 50); do curl -s -o /dev/null "$1" && return 0; sleep 0.2; done; return 1; }
wait_up "http://$MESHDB_HTTP/v1/info" || { echo "meshdb did not come up"; exit 1; }
wait_up "http://$CONSOLE_ADDR/api/me" || { echo "console did not come up"; exit 1; }

# --- seed a "local" connection in the console so it's ready to use ---
JAR="$STACK_DIR/.seed-cookies"
curl -s -c "$JAR" -X POST "http://$CONSOLE_ADDR/api/login" \
  -d "{\"username\":\"$CONSOLE_ADMIN\",\"password\":\"$CONSOLE_ADMIN_PASSWORD\"}" >/dev/null
SEED="{\"name\":\"local\",\"url\":\"http://$MESHDB_HTTP\",\"replace\":true"
[ -n "$MESHDB_USER" ] && SEED="$SEED,\"meshdb_user\":\"$MESHDB_USER\",\"meshdb_secret\":\"$MESHDB_SECRET\""
SEED="$SEED}"
curl -s -b "$JAR" -X POST "http://$CONSOLE_ADDR/api/connections" -d "$SEED" >/dev/null
rm -f "$JAR"

cat <<BANNER

  meshdb stack is up
  ──────────────────────────────────────────────
  console      http://$CONSOLE_ADDR   (log in: $CONSOLE_ADMIN / $CONSOLE_ADMIN_PASSWORD)
  gateway HTTP http://$MESHDB_HTTP    (meshdb auth: $AUTH_NOTE)
  gateway TCP  $MESHDB_NATIVE
  data         $STACK_DIR/
  ──────────────────────────────────────────────
  A "local" connection is already configured in the console.
  Press Ctrl-C to stop both.

BANNER

# Exit (and trigger cleanup) as soon as either process dies.
wait -n
