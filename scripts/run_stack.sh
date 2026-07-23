#!/usr/bin/env bash
#
# Run a local shardlite gateway and the web console together, wired up so the console already has a
# "local" connection pointing at the gateway. Builds whatever is missing, then supervises both;
# Ctrl-C stops both cleanly.
#
# Everything is env-overridable (defaults shown):
#   SHARDLITE_STACK_DIR=.stack          where both keep their data
#   SHARDLITE_HTTP=127.0.0.1:4680       gateway HTTP /v1 edge (what the console talks to)
#   SHARDLITE_NATIVE=127.0.0.1:4620     gateway native TCP edge
#   SHARDLITE_SHARDS=4                  shard count when the data dir is first created
#   CONSOLE_ADDR=127.0.0.1:7100      console web address
#   CONSOLE_ADMIN=admin / CONSOLE_ADMIN_PASSWORD=admin    bootstrap console login
#   SHARDLITE_CONSOLE_KEY=<dev default> master passphrase encrypting stored connection secrets
#
# Auth: by default the local gateway runs WITHOUT shardlite auth (fine for localhost). Set both
# SHARDLITE_USER and SHARDLITE_SECRET to run it WITH auth — the script provisions that admin user,
# starts the gateway with auth, and seeds the console connection with the (sealed) credential.
set -uo pipefail
cd "$(dirname "$0")/.."

STACK_DIR="${SHARDLITE_STACK_DIR:-.stack}"
SHARDLITE_HTTP="${SHARDLITE_HTTP:-127.0.0.1:4680}"
SHARDLITE_NATIVE="${SHARDLITE_NATIVE:-127.0.0.1:4620}"
SHARDS="${SHARDLITE_SHARDS:-4}"
CONSOLE_ADDR="${CONSOLE_ADDR:-127.0.0.1:7100}"
CONSOLE_ADMIN="${CONSOLE_ADMIN:-admin}"
CONSOLE_ADMIN_PASSWORD="${CONSOLE_ADMIN_PASSWORD:-admin}"
export SHARDLITE_CONSOLE_KEY="${SHARDLITE_CONSOLE_KEY:-dev-master-key-change-me}"
SHARDLITE_USER="${SHARDLITE_USER:-}"
SHARDLITE_SECRET="${SHARDLITE_SECRET:-}"

SHARDLITE_BIN=target/debug/shardlite
CONSOLE_BIN=console/server/target/debug/shardlite-console
DB_DIR="$STACK_DIR/db"
CONSOLE_DATA="$STACK_DIR/console"
USERS_FILE="$STACK_DIR/shardlite-users.json"

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
if [ ! -x "$SHARDLITE_BIN" ] || ! "$SHARDLITE_BIN" serve --help >/dev/null 2>&1; then
  echo "building shardlite (--features http)…"
  cargo build --features http || { echo "shardlite build failed"; exit 1; }
fi
if [ ! -x "$CONSOLE_BIN" ]; then
  echo "building console (frontend + backend)…"
  ( cd console && ./build.sh ) || { echo "console build failed — see console/README.md"; exit 1; }
fi

mkdir -p "$STACK_DIR"

# --- provision shardlite auth if requested ---
SERVE_AUTH_ARGS=()
if [ -n "$SHARDLITE_USER" ] && [ -n "$SHARDLITE_SECRET" ]; then
  if [ ! -f "$USERS_FILE" ]; then
    "$SHARDLITE_BIN" user add "$SHARDLITE_USER" "$SHARDLITE_SECRET" --role admin --users "$USERS_FILE" >/dev/null
  fi
  # auth without TLS is refused unless explicitly acknowledged — fine on localhost.
  SERVE_AUTH_ARGS=(--users "$USERS_FILE" --http-insecure)
  AUTH_NOTE="on (user: $SHARDLITE_USER)"
else
  AUTH_NOTE="off (local, no shardlite auth)"
fi

# --- initialise the data dir on first run (shard count is fixed at creation) ---
if [ ! -e "$DB_DIR" ]; then
  echo "initialising shardlite data dir ($SHARDS shards)…"
  "$SHARDLITE_BIN" "$DB_DIR" --shards "$SHARDS" -c "SELECT 1" >/dev/null || { echo "init failed"; exit 1; }
fi

# --- start shardlite ---
"$SHARDLITE_BIN" serve "$DB_DIR" --listen "$SHARDLITE_NATIVE" --http "$SHARDLITE_HTTP" "${SERVE_AUTH_ARGS[@]}" &
MDB=$!

# --- start the console ---
SHARDLITE_CONSOLE_ADMIN="$CONSOLE_ADMIN" \
SHARDLITE_CONSOLE_ADMIN_PASSWORD="$CONSOLE_ADMIN_PASSWORD" \
  "$CONSOLE_BIN" --listen "$CONSOLE_ADDR" --data "$CONSOLE_DATA" &
CON=$!

# --- wait for both to accept connections ---
wait_up() { for _ in $(seq 1 50); do curl -s -o /dev/null "$1" && return 0; sleep 0.2; done; return 1; }
wait_up "http://$SHARDLITE_HTTP/v1/info" || { echo "shardlite did not come up"; exit 1; }
wait_up "http://$CONSOLE_ADDR/api/me" || { echo "console did not come up"; exit 1; }

# --- seed a "local" connection in the console so it's ready to use ---
JAR="$STACK_DIR/.seed-cookies"
curl -s -c "$JAR" -X POST "http://$CONSOLE_ADDR/api/login" \
  -d "{\"username\":\"$CONSOLE_ADMIN\",\"password\":\"$CONSOLE_ADMIN_PASSWORD\"}" >/dev/null
SEED="{\"name\":\"local\",\"url\":\"http://$SHARDLITE_HTTP\",\"replace\":true"
[ -n "$SHARDLITE_USER" ] && SEED="$SEED,\"shardlite_user\":\"$SHARDLITE_USER\",\"shardlite_secret\":\"$SHARDLITE_SECRET\""
SEED="$SEED}"
curl -s -b "$JAR" -X POST "http://$CONSOLE_ADDR/api/connections" -d "$SEED" >/dev/null
rm -f "$JAR"

cat <<BANNER

  shardlite stack is up
  ──────────────────────────────────────────────
  console      http://$CONSOLE_ADDR   (log in: $CONSOLE_ADMIN / $CONSOLE_ADMIN_PASSWORD)
  gateway HTTP http://$SHARDLITE_HTTP    (shardlite auth: $AUTH_NOTE)
  gateway TCP  $SHARDLITE_NATIVE
  data         $STACK_DIR/
  ──────────────────────────────────────────────
  A "local" connection is already configured in the console.
  Press Ctrl-C to stop both.

BANNER

# Exit (and trigger cleanup) as soon as either process dies.
wait -n
