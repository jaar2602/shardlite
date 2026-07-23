#!/usr/bin/env bash
#
# Start a shardlite gateway on BOTH transports (HTTP and JSON/TCP) and exercise each driver against
# it over both. Skips a language whose runtime is absent rather than failing. Requires the binary
# built with --features http,json-tcp.
set -uo pipefail
cd "$(dirname "$0")/.."

BIN=target/debug/shardlite
if ! "$BIN" serve --help >/dev/null 2>&1; then
  cargo build --features http,json-tcp >/dev/null 2>&1 || { echo "build failed"; exit 1; }
fi

W=$(mktemp -d); P=$(( (RANDOM % 2000) + 34000 )); TP=$((P+2))
cleanup() { [ -n "${SRV:-}" ] && kill "$SRV" 2>/dev/null; rm -rf "$W"; }
trap cleanup EXIT

"$BIN" "$W/data" --shards 1 -c "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT) STRICT" >/dev/null 2>&1
"$BIN" "$W/data" -c "WITH RECURSIVE s(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM s WHERE x<20000) INSERT INTO t SELECT x, 'v'||x FROM s" >/dev/null 2>&1
"$BIN" serve "$W/data" --listen "127.0.0.1:$((P+1))" --http "127.0.0.1:$P" --json-tcp "127.0.0.1:$TP" >/dev/null 2>&1 &
SRV=$!
sleep 1
if ! curl -s "http://127.0.0.1:$P/v1/info" 2>/dev/null | grep -q shard_count; then
  echo "gateway did not start on :$P — build with --features http,json-tcp"
  exit 1
fi

# A driver passes if it streams all 20000 rows over HTTP and over JSON/TCP and reports both.
EXPECT_HTTP="streamed rows: 20000"
EXPECT_TCP="tcp streamed rows: 20000"
FAIL=0

# ok NAME OUTPUT — pass only if both transports streamed the full result.
ok() {
  local name="$1" out="$2"
  if echo "$out" | grep -q "$EXPECT_HTTP" && echo "$out" | grep -q "$EXPECT_TCP"; then
    echo "  $name  ok (http + tcp)"
  else
    echo "  $name  FAIL"; echo "$out"; FAIL=1
  fi
}

if command -v python3 >/dev/null 2>&1; then
  ok "python" "$(SHARDLITE_PORT=$P SHARDLITE_TCP_PORT=$TP python3 drivers/python/driver_check.py 2>&1)"
else echo "  python  skipped (absent)"; fi

if command -v node >/dev/null 2>&1; then
  ok "node  " "$(SHARDLITE_PORT=$P SHARDLITE_TCP_PORT=$TP node drivers/javascript/driver_check.mjs 2>&1)"
else echo "  node    skipped (absent)"; fi

if command -v cargo >/dev/null 2>&1; then
  # The Rust driver reaches TCP via the native client, not JSON/TCP; HTTP only here.
  OUT=$(cd drivers/rust && SHARDLITE_PORT=$P cargo run --quiet --example demo 2>&1)
  echo "$OUT" | grep -q "$EXPECT_HTTP" && echo "  rust    ok (http)" || { echo "  rust    FAIL"; echo "$OUT"; FAIL=1; }
else echo "  rust    skipped (absent)"; fi

if command -v go >/dev/null 2>&1; then
  OUT=$(cd drivers/go && SHARDLITE_PORT=$P SHARDLITE_TCP_PORT=$TP go run ./example 2>&1)
  echo "$OUT" | grep -q "alice" && echo "$OUT" | grep -q "$EXPECT_TCP" \
    && echo "  go      ok (http + tcp)" || { echo "  go      FAIL"; echo "$OUT"; FAIL=1; }
else echo "  go      skipped (no toolchain)"; fi

echo
[ "$FAIL" -eq 0 ] && echo "drivers: all present languages passed" || echo "drivers: FAILURES above"
exit $FAIL
