#!/usr/bin/env bash
#
# End-to-end check of the meshdb console: builds it, starts a meshdb HTTP gateway and the console,
# and drives the whole API — login, multi-user roles, encrypted-at-rest secrets, the streaming
# proxy over a large result, and the embedded SPA. Skips the build if the binaries already exist.
set -uo pipefail
cd "$(dirname "$0")/.."

MESHDB=target/debug/meshdb
CONSOLE=console/server/target/debug/meshdb-console
PASS=0 FAIL=0
check() { # check "name" "expected substring" "actual"
  if echo "$3" | grep -qF "$2"; then echo "  ok   $1"; PASS=$((PASS+1));
  else echo "  FAIL $1 — wanted '$2', got: $3"; FAIL=$((FAIL+1)); fi
}

# --- build if needed ---
[ -x "$MESHDB" ] || cargo build --features http >/dev/null 2>&1
if [ ! -x "$CONSOLE" ]; then
  echo "building console (frontend + backend)…"
  ( cd console && ./build.sh ) >/dev/null 2>&1 || { echo "console build failed — run console/build.sh"; exit 1; }
fi

W=$(mktemp -d); U="$W/users.json"; P=$(( (RANDOM%2000)+43000 )); CP=$((P+3)); J="$W/jar"; JV="$W/jv"
MDB="" CON=""
cleanup() { [ -n "$MDB" ] && kill "$MDB" 2>/dev/null; [ -n "$CON" ] && kill "$CON" 2>/dev/null; rm -rf "$W"; }
trap cleanup EXIT

# --- a meshdb cluster with an admin user and 60k rows ---
"$MESHDB" user add app app-secret --role admin --users "$U" >/dev/null
"$MESHDB" "$W/data" --shards 1 -c "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT" >/dev/null
"$MESHDB" "$W/data" -c "WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x<60000) INSERT INTO t SELECT x,'v'||x FROM s" >/dev/null
"$MESHDB" serve "$W/data" --listen "127.0.0.1:$((P+1))" --http "127.0.0.1:$P" --http-insecure --users "$U" >/dev/null 2>&1 &
MDB=$!

# --- the console ---
export MESHDB_CONSOLE_KEY="master-passphrase"
export MESHDB_CONSOLE_ADMIN="root" MESHDB_CONSOLE_ADMIN_PASSWORD="rootpw"
"$CONSOLE" --listen "127.0.0.1:$CP" --data "$W/cdata" >/dev/null 2>&1 &
CON=$!
sleep 1

check "embedded SPA served"        "meshdb console" "$(curl -s http://127.0.0.1:$CP/)"
check "SPA fallback for route"     "200"            "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$CP/c/local/query)"
check "login rejects wrong pw"     "401"            "$(curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:$CP/api/login -d '{"username":"root","password":"no"}')"
curl -s -c "$J" -X POST http://127.0.0.1:$CP/api/login -d '{"username":"root","password":"rootpw"}' >/dev/null
check "me returns admin"           '"role":"admin"' "$(curl -s -b "$J" http://127.0.0.1:$CP/api/me)"

curl -s -b "$J" -X POST http://127.0.0.1:$CP/api/connections \
  -d "{\"name\":\"local\",\"url\":\"http://127.0.0.1:$P\",\"meshdb_user\":\"app\",\"meshdb_secret\":\"app-secret\"}" >/dev/null
check "connection list hides secret" '"meshdb_user":"app"' "$(curl -s -b "$J" http://127.0.0.1:$CP/api/connections)"
check "secret encrypted at rest"   "secret-absent"  "$(grep -q app-secret "$W/cdata/connections.json" && echo LEAK || echo secret-absent)"

STREAM=$(curl -s -b "$J" -X POST http://127.0.0.1:$CP/api/connections/local/query -d '{"sql":"SELECT id FROM t ORDER BY id"}' | wc -l)
check "streaming proxy: 60000 rows + header" "60001" "$STREAM"

check "proxy tx atomic"            '"rows_affected":2' "$(curl -s -b "$J" -X POST http://127.0.0.1:$CP/api/connections/local/tx -d '{"shard":0,"statements":[{"sql":"INSERT INTO t VALUES (100001,'\''a'\'')"},{"sql":"INSERT INTO t VALUES (100002,'\''b'\'')"}]}')"
check "proxy frames report"        '"wal":true'     "$(curl -s -b "$J" http://127.0.0.1:$CP/api/connections/local/frames/0)"
check "proxy meshdb users"         '"app"'          "$(curl -s -b "$J" http://127.0.0.1:$CP/api/connections/local/users)"

# multi-user roles: a 'user' may read connections but not administer.
curl -s -b "$J" -X POST http://127.0.0.1:$CP/api/console-users -d '{"username":"viewer","password":"vp","role":"user"}' >/dev/null
curl -s -c "$JV" -X POST http://127.0.0.1:$CP/api/login -d '{"username":"viewer","password":"vp"}' >/dev/null
check "user can list connections"  "200"            "$(curl -s -b "$JV" -o /dev/null -w '%{http_code}' http://127.0.0.1:$CP/api/connections)"
check "user blocked from admin op" "403"            "$(curl -s -b "$JV" -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:$CP/api/connections -d '{"name":"x","url":"http://h:1"}')"
check "unauthenticated blocked"    "401"            "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$CP/api/connections/local/info)"

echo
echo "console smoke: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
