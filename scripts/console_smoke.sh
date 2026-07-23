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

# --- build the exact feature set and embedded SPA under test (incremental when unchanged) ---
cargo build --features http --bin meshdb >/dev/null 2>&1 || { echo "meshdb HTTP build failed"; exit 1; }
echo "building console (frontend + backend)…"
( cd console && ./build.sh ) >/dev/null 2>&1 || { echo "console build failed — run console/build.sh"; exit 1; }

W=$(mktemp -d); U="$W/users.json"; P=$(( (RANDOM%2000)+43000 )); CP=$((P+3)); J="$W/jar"; JV="$W/jv"
MDB="" CON=""
cleanup() { [ -n "$MDB" ] && kill "$MDB" 2>/dev/null; [ -n "$CON" ] && kill "$CON" 2>/dev/null; rm -rf "$W"; }
trap cleanup EXIT

# --- a meshdb cluster with an admin user and 60k rows ---
"$MESHDB" user add app app-secret --role admin --users "$U" >/dev/null
"$MESHDB" "$W/data" --shards 1 -c "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT" >/dev/null
"$MESHDB" "$W/data" -c "WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x<60000) INSERT INTO t SELECT x,'v'||x FROM s" >/dev/null
"$MESHDB" serve "$W/data" --listen "127.0.0.1:$((P+1))" --http "127.0.0.1:$P" --http-insecure --users "$U" >"$W/meshdb.log" 2>&1 &
MDB=$!

for _ in $(seq 1 40); do
  curl -fsS -u app:app-secret "http://127.0.0.1:$P/v1/info" >/dev/null 2>&1 && break
  if ! kill -0 "$MDB" 2>/dev/null; then
    echo "meshdb fixture exited during startup:"
    sed -n '1,120p' "$W/meshdb.log"
    exit 1
  fi
  sleep 0.1
done
if ! curl -fsS -u app:app-secret "http://127.0.0.1:$P/v1/info" >/dev/null 2>&1; then
  echo "meshdb fixture did not become ready:"
  sed -n '1,120p' "$W/meshdb.log"
  exit 1
fi

# --- the console ---
export MESHDB_CONSOLE_KEY="master-passphrase"
export MESHDB_CONSOLE_ADMIN="root" MESHDB_CONSOLE_ADMIN_PASSWORD="rootpw"
"$CONSOLE" --listen "127.0.0.1:$CP" --data "$W/cdata" >/dev/null 2>&1 &
CON=$!
sleep 1

check "embedded SPA served"        "meshdb console" "$(curl -s http://127.0.0.1:$CP/)"
check "SPA fallback for route"     "200"            "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$CP/c/local/query)"
check "login rejects wrong pw"     "401"            "$(curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:$CP/api/login -d '{"username":"root","password":"no"}')"
LOGIN=$(curl -s -c "$J" -X POST http://127.0.0.1:$CP/api/login -d '{"username":"root","password":"rootpw"}')
CSRF=$(printf '%s' "$LOGIN" | sed -n 's/.*"csrf_token":"\([^"]*\)".*/\1/p')
check "me returns admin"           '"role":"admin"' "$(curl -s -b "$J" http://127.0.0.1:$CP/api/me)"
check "console health"             '"ok":true'      "$(curl -s http://127.0.0.1:$CP/healthz)"
check "mutation requires CSRF"     "403"             "$(curl -s -b "$J" -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:$CP/api/connections -d '{"name":"blocked","url":"http://host:1"}')"

curl -s -b "$J" -H "X-CSRF-Token: $CSRF" -X POST http://127.0.0.1:$CP/api/connections \
  -d "{\"name\":\"local\",\"url\":\"http://127.0.0.1:$P\",\"meshdb_user\":\"app\",\"meshdb_secret\":\"app-secret\",\"allow_insecure_http\":true}" >/dev/null
check "connection list hides secret" '"meshdb_user":"app"' "$(curl -s -b "$J" http://127.0.0.1:$CP/api/connections)"
check "secret encrypted at rest"   "secret-absent"  "$(grep -q app-secret "$W/cdata/connections.json" && echo LEAK || echo secret-absent)"
check "connection test"            '"ok":true'      "$(curl -s -b "$J" -H "X-CSRF-Token: $CSRF" -X POST http://127.0.0.1:$CP/api/connections/local/test)"
check "profile migrates to seeds"  '"seeds":["http://127.0.0.1:' "$(curl -s -b "$J" http://127.0.0.1:$CP/api/connections)"
check "proxy meta contract"        '"api_version":1' "$(curl -s -b "$J" http://127.0.0.1:$CP/api/connections/local/meta)"
check "proxy health contract"      '"status":"healthy"' "$(curl -s -b "$J" http://127.0.0.1:$CP/api/connections/local/health)"
check "proxy topology contract"    '"observed_at_ms":' "$(curl -s -b "$J" http://127.0.0.1:$CP/api/connections/local/topology)"
check "proxy shard inventory"      '"local_role":"primary"' "$(curl -s -b "$J" http://127.0.0.1:$CP/api/connections/local/shards)"

OBS=""
for _ in $(seq 1 40); do
  OBS=$(curl -s -b "$J" http://127.0.0.1:$CP/api/connections/local/observation)
  echo "$OBS" | grep -q '"last_success_ms":[0-9]' && break
  sleep 0.25
done
check "bounded collector observation" '"status":"healthy"' "$OBS"
check "fleet aggregates cluster"       '"name":"local"' "$(curl -s -b "$J" http://127.0.0.1:$CP/api/fleet)"
check "aggregated shard inventory"      '"state":"available"' "$(curl -s -b "$J" http://127.0.0.1:$CP/api/connections/local/shard-inventory)"

STREAM=$(curl -s -b "$J" -H "X-CSRF-Token: $CSRF" -X POST http://127.0.0.1:$CP/api/connections/local/query -d '{"sql":"SELECT id FROM t ORDER BY id"}' | wc -l)
check "streaming proxy: 60000 rows + header" "60001" "$STREAM"
curl -s -b "$J" -D "$W/download-headers" -o "$W/download.ndjson" -X POST \
  --data-urlencode "csrf=$CSRF" \
  --data-urlencode 'payload={"shard":0,"sql":"SELECT id FROM t ORDER BY id LIMIT 2"}' \
  http://127.0.0.1:$CP/api/connections/local/query-download
check "streaming query download"    "3"              "$(wc -l < "$W/download.ndjson")"
check "download disposition"        "attachment"     "$(tr -d '\r' < "$W/download-headers")"

curl -s -b "$J" -o "$W/download.csv" -X POST \
  --data-urlencode "csrf=$CSRF" \
  --data-urlencode 'payload={"shard":0,"sql":"SELECT id,v FROM t ORDER BY id LIMIT 5","format":"csv","max_rows":2}' \
  http://127.0.0.1:$CP/api/connections/local/query-download
check "bounded streaming CSV export" "3"              "$(wc -l < "$W/download.csv")"
check "CSV export header"            "id,v"           "$(head -n 1 "$W/download.csv")"
check "route by key"                 '"shard":0'      "$(curl -s -b "$J" -H "X-CSRF-Token: $CSRF" -X POST http://127.0.0.1:$CP/api/connections/local/route -d '{"key":"customer:42"}')"
check "at-least-LSN query"           '"columns"'      "$(curl -s -b "$J" -H "X-CSRF-Token: $CSRF" -X POST http://127.0.0.1:$CP/api/connections/local/query -d '{"sql":"SELECT 1","consistency":{"at_least_lsn":0}}')"
check "typed blob parameter"         '"00AFFF"'       "$(curl -s -b "$J" -H "X-CSRF-Token: $CSRF" -X POST http://127.0.0.1:$CP/api/connections/local/query -d '{"sql":"SELECT hex(?)","params":[{"blob_hex":"00afff"}]}')"
check "explain query plan"            '"detail"'       "$(curl -s -b "$J" -H "X-CSRF-Token: $CSRF" -X POST http://127.0.0.1:$CP/api/connections/local/query -d '{"sql":"EXPLAIN QUERY PLAN SELECT * FROM t WHERE id = ?","params":[1]}')"
check "rich schema table metadata"    '"id"'           "$(curl -s -b "$J" -H "X-CSRF-Token: $CSRF" -X POST http://127.0.0.1:$CP/api/connections/local/query -d '{"sql":"SELECT name FROM pragma_table_xinfo(?) ORDER BY cid","params":["t"]}')"
check "fan-out query"                '"columns"'      "$(curl -s -b "$J" -H "X-CSRF-Token: $CSRF" -X POST http://127.0.0.1:$CP/api/connections/local/query_all -d '{"sql":"SELECT COUNT(*) FROM t"}')"

check "proxy tx atomic"            '"rows_affected":2' "$(curl -s -b "$J" -H "X-CSRF-Token: $CSRF" -X POST http://127.0.0.1:$CP/api/connections/local/tx -d '{"shard":0,"statements":[{"sql":"INSERT INTO t VALUES (100001,'\''a'\'')"},{"sql":"INSERT INTO t VALUES (100002,'\''b'\'')"}]}')"
check "MeshDB all-shard primitive" '"ok":true'        "$(curl -s -u app:app-secret -X POST http://127.0.0.1:$P/v1/execute_all -d '{"sql":"ALTER TABLE t ADD COLUMN phase2 INTEGER"}')"
check "direct console rollout blocked" "404"           "$(curl -s -b "$J" -H "X-CSRF-Token: $CSRF" -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:$CP/api/connections/local/execute_all -d '{"sql":"CREATE TABLE bypass(x)"}')"

ROLL_SQL='CREATE INDEX phase3_t_v ON t(v)'
PREFLIGHT=$(curl -s -b "$J" -H "X-CSRF-Token: $CSRF" -X POST http://127.0.0.1:$CP/api/operations/preflight -d "{\"connection\":\"local\",\"sql\":\"$ROLL_SQL\"}")
check "durable rollout preflight"  '"sql_fingerprint"' "$PREFLIGHT"
PREFLIGHT_TOKEN=$(printf '%s' "$PREFLIGHT" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
EXPECTED_VERSIONS=$(printf '%s' "$PREFLIGHT" | sed -n 's/.*"versions":\(\[[^]]*\]\).*/\1/p')
OPERATION=$(curl -s -b "$J" -H "X-CSRF-Token: $CSRF" -X POST http://127.0.0.1:$CP/api/operations \
  -d "{\"connection\":\"local\",\"sql\":\"$ROLL_SQL\",\"idempotency_key\":\"smoke-phase3-rollout\",\"preflight_token\":\"$PREFLIGHT_TOKEN\",\"expected_versions\":$EXPECTED_VERSIONS}")
OPERATION_ID=$(printf '%s' "$OPERATION" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
check "durable rollout submitted"  '"kind":"schema_rollout"' "$OPERATION"
OPERATION_DUP=$(curl -s -b "$J" -H "X-CSRF-Token: $CSRF" -X POST http://127.0.0.1:$CP/api/operations \
  -d "{\"connection\":\"local\",\"sql\":\"$ROLL_SQL\",\"idempotency_key\":\"smoke-phase3-rollout\",\"preflight_token\":\"$PREFLIGHT_TOKEN\",\"expected_versions\":$EXPECTED_VERSIONS}")
check "operation retry is idempotent" "\"id\":\"$OPERATION_ID\"" "$OPERATION_DUP"
OPERATION_RESULT=""
for _ in $(seq 1 40); do
  OPERATION_RESULT=$(curl -s -b "$J" http://127.0.0.1:$CP/api/operations/$OPERATION_ID)
  echo "$OPERATION_RESULT" | grep -q '"status":"succeeded"' && break
  sleep 0.1
done
check "durable rollout completed"  '"status":"succeeded"' "$OPERATION_RESULT"
check "durable per-shard outcome"  '"ok":true' "$OPERATION_RESULT"
check "operation appears in history" "$OPERATION_ID" "$(curl -s -b "$J" http://127.0.0.1:$CP/api/operations)"
check "operation journal persisted" "$OPERATION_ID" "$(grep -F "$OPERATION_ID" "$W/cdata/operations.json")"

STALE_SQL='CREATE TABLE phase3_stale_guard(x INTEGER)'
STALE_PREFLIGHT=$(curl -s -b "$J" -H "X-CSRF-Token: $CSRF" -X POST http://127.0.0.1:$CP/api/operations/preflight -d "{\"connection\":\"local\",\"sql\":\"$STALE_SQL\"}")
STALE_TOKEN=$(printf '%s' "$STALE_PREFLIGHT" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
STALE_VERSIONS=$(printf '%s' "$STALE_PREFLIGHT" | sed -n 's/.*"versions":\(\[[^]]*\]\).*/\1/p')
curl -s -u app:app-secret -X POST http://127.0.0.1:$P/v1/execute_all -d '{"sql":"CREATE TABLE phase3_version_bump(x INTEGER)"}' >/dev/null
STALE_OPERATION=$(curl -s -b "$J" -H "X-CSRF-Token: $CSRF" -X POST http://127.0.0.1:$CP/api/operations \
  -d "{\"connection\":\"local\",\"sql\":\"$STALE_SQL\",\"idempotency_key\":\"smoke-stale-preflight\",\"preflight_token\":\"$STALE_TOKEN\",\"expected_versions\":$STALE_VERSIONS}")
STALE_ID=$(printf '%s' "$STALE_OPERATION" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
STALE_RESULT=""
for _ in $(seq 1 40); do
  STALE_RESULT=$(curl -s -b "$J" http://127.0.0.1:$CP/api/operations/$STALE_ID)
  echo "$STALE_RESULT" | grep -q '"status":"failed"' && break
  sleep 0.1
done
check "changed preflight blocks rollout" '"status":"failed"' "$STALE_RESULT"
check "stale approval explains refusal"  "schema changed after approval" "$STALE_RESULT"
check "stale rollout never reached MeshDB" '[0]' "$(curl -s -b "$J" -H "X-CSRF-Token: $CSRF" -X POST http://127.0.0.1:$CP/api/connections/local/query -d '{"sql":"SELECT COUNT(*) FROM sqlite_schema WHERE name = '\''phase3_stale_guard'\''"}')"
check "proxy frames report"        '"wal":true'     "$(curl -s -b "$J" http://127.0.0.1:$CP/api/connections/local/frames/0)"
check "proxy meshdb users"         '"app"'          "$(curl -s -b "$J" http://127.0.0.1:$CP/api/connections/local/users)"

# Multi-user roles: a viewer may observe and query, but cannot write or administer.
curl -s -b "$J" -H "X-CSRF-Token: $CSRF" -X POST http://127.0.0.1:$CP/api/console-users -d '{"username":"viewer","password":"vp","role":"viewer"}' >/dev/null
VLOGIN=$(curl -s -c "$JV" -X POST http://127.0.0.1:$CP/api/login -d '{"username":"viewer","password":"vp"}')
VCSRF=$(printf '%s' "$VLOGIN" | sed -n 's/.*"csrf_token":"\([^"]*\)".*/\1/p')
check "user can list connections"  "200"            "$(curl -s -b "$JV" -o /dev/null -w '%{http_code}' http://127.0.0.1:$CP/api/connections)"
check "viewer can query"            "200"            "$(curl -s -b "$JV" -H "X-CSRF-Token: $VCSRF" -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:$CP/api/connections/local/query -d '{"sql":"SELECT 1"}')"
check "viewer blocked from operations" "403"          "$(curl -s -b "$JV" -o /dev/null -w '%{http_code}' http://127.0.0.1:$CP/api/operations)"
check "viewer blocked from writes" "403"            "$(curl -s -b "$JV" -H "X-CSRF-Token: $VCSRF" -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:$CP/api/connections/local/execute -d '{"sql":"CREATE TABLE denied(x)"}')"
check "user blocked from admin op" "403"            "$(curl -s -b "$JV" -H "X-CSRF-Token: $VCSRF" -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:$CP/api/connections -d '{"name":"x","url":"http://h:1"}')"
check "audit records write"         '"meshdb.tx.post"' "$(curl -s -b "$J" http://127.0.0.1:$CP/api/audit)"
check "audit records operation lifecycle" '"operation.schema_rollout.complete"' "$(curl -s -b "$J" http://127.0.0.1:$CP/api/audit)"
check "unauthenticated blocked"    "401"            "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$CP/api/connections/local/info)"

echo
echo "console smoke: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
