#!/usr/bin/env bash
#
# Start a meshdb HTTP gateway and exercise each driver against it. Skips a language whose
# runtime is absent rather than failing. Requires the binary built with --features http.
set -uo pipefail
cd "$(dirname "$0")/.."

BIN=target/debug/meshdb
if ! "$BIN" serve --help >/dev/null 2>&1; then
  cargo build --features http >/dev/null 2>&1 || { echo "build failed"; exit 1; }
fi

W=$(mktemp -d); P=$(( (RANDOM % 2000) + 34000 ))
cleanup() { [ -n "${SRV:-}" ] && kill "$SRV" 2>/dev/null; rm -rf "$W"; }
trap cleanup EXIT

"$BIN" "$W/data" --shards 1 -c "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT" >/dev/null 2>&1
"$BIN" "$W/data" -c "WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x<20000) INSERT INTO t SELECT x, 'v'||x FROM s" >/dev/null 2>&1
"$BIN" serve "$W/data" --listen "127.0.0.1:$((P+1))" --http "127.0.0.1:$P" >/dev/null 2>&1 &
SRV=$!
sleep 1
if ! curl -s "http://127.0.0.1:$P/v1/info" 2>/dev/null | grep -q shard_count; then
  echo "gateway did not start on :$P — build with --features http (cargo build --features http)"
  exit 1
fi

# A driver passes if it streams all 20000 rows and reports it.
EXPECT="streamed rows: 20000"
FAIL=0

if command -v python3 >/dev/null 2>&1; then
  OUT=$(MESHDB_PORT=$P python3 drivers/python/driver_check.py 2>&1)
  echo "$OUT" | grep -q "$EXPECT" && echo "  python  ok" || { echo "  python  FAIL"; echo "$OUT"; FAIL=1; }
else echo "  python  skipped (absent)"; fi

if command -v node >/dev/null 2>&1; then
  OUT=$(MESHDB_PORT=$P node drivers/javascript/driver_check.mjs 2>&1)
  echo "$OUT" | grep -q "$EXPECT" && echo "  node    ok" || { echo "  node    FAIL"; echo "$OUT"; FAIL=1; }
else echo "  node    skipped (absent)"; fi

if command -v cargo >/dev/null 2>&1; then
  OUT=$(cd drivers/rust && MESHDB_PORT=$P cargo run --quiet --example demo 2>&1)
  echo "$OUT" | grep -q "$EXPECT" && echo "  rust    ok" || { echo "  rust    FAIL"; echo "$OUT"; FAIL=1; }
else echo "  rust    skipped (absent)"; fi

if command -v go >/dev/null 2>&1; then
  OUT=$(cd drivers/go && MESHDB_PORT=$P go run ./example 2>&1)
  echo "$OUT" | grep -q "alice" && echo "  go      ok" || { echo "  go      FAIL"; echo "$OUT"; FAIL=1; }
else echo "  go      skipped (no toolchain)"; fi

echo
[ "$FAIL" -eq 0 ] && echo "drivers: all present languages passed" || echo "drivers: FAILURES above"
exit $FAIL
