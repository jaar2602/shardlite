#!/usr/bin/env bash
#
# End-to-end test of the `meshdb user` and `meshdb serve --users` CLI: offline
# provisioning, a running server, and runtime user creation that persists.
set -uo pipefail
cd "$(dirname "$0")/.."
BIN=target/debug/meshdb
[ -x "$BIN" ] || cargo build >/dev/null 2>&1
WORK=$(mktemp -d)
PORT=$(( (RANDOM % 2000) + 15000 ))
SERVER=""
cleanup() { [ -n "$SERVER" ] && kill "$SERVER" 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); printf '  \033[32mok\033[0m   %s\n' "$1"; }
bad()  { FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$1"; }
check(){ if eval "$2"; then ok "$1"; else bad "$1"; fi; }

U="$WORK/users.db"

$BIN user add admin s3cret --role admin --users "$U" >/dev/null
check "offline admin created"          "grep -q '^user admin admin' '$U'"
check "secret is NOT in the file"      "! grep -q 's3cret' '$U'"

$BIN user add alice alice-pw --role write --users "$U" >/dev/null
check "offline list shows both users"  "[ \$($BIN user list --users '$U' | wc -l) -eq 2 ]"

# cluster role is refused even offline? No — offline file editing is a deploy-time act and
# may set cluster. The runtime refusal is what matters; assert that below.
$BIN serve "$WORK/data" --shards 2 --listen "127.0.0.1:$PORT" --users "$U" >/dev/null 2>&1 &
SERVER=$!
sleep 1

$BIN user add bob bob-pw --role read --server "127.0.0.1:$PORT" --as admin --admin-secret s3cret >/dev/null
check "runtime user created over the wire" "$BIN user list --server 127.0.0.1:$PORT --as admin --admin-secret s3cret | grep -q '^bob'"
check "runtime change persisted to file"   "grep -q '^user bob read' '$U'"

# runtime cluster grant refused
if $BIN user add evil evil-pw --role cluster --server "127.0.0.1:$PORT" --as admin --admin-secret s3cret >/dev/null 2>&1; then
  bad "runtime cluster grant refused"
else
  ok "runtime cluster grant refused"
fi

# a non-admin cannot manage users
if $BIN user list --server "127.0.0.1:$PORT" --as alice --admin-secret alice-pw >/dev/null 2>&1; then
  bad "non-admin cannot list users"
else
  ok "non-admin cannot list users"
fi

# wrong admin secret is refused
if $BIN user list --server "127.0.0.1:$PORT" --as admin --admin-secret WRONG >/dev/null 2>&1; then
  bad "wrong admin secret refused"
else
  ok "wrong admin secret refused"
fi

echo
echo "passed $PASS, failed $FAIL"
[ "$FAIL" -eq 0 ]
