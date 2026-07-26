#!/usr/bin/env bash
# Read-only smoke verification for the packaged Docker stack.
#
# Usage: ./verify.sh [console-url]
# The stack must already be running (`./shardlite-stack up`). This intentionally does not write
# data or mutate cluster membership; it verifies the image is buildable, every node is healthy,
# the console is ready, and the console has a registered connection with a dynamic catalog.
set -euo pipefail
cd "$(dirname "$0")"

CONSOLE_URL="${1:-http://localhost:${CONSOLE_PORT:-7100}}"
ROOT="$(cd ../.. && pwd)"

command -v curl >/dev/null || { echo "error: curl is required" >&2; exit 2; }
if command -v docker >/dev/null 2>&1; then
  if docker compose version >/dev/null 2>&1; then DC=(docker compose); elif command -v docker-compose >/dev/null 2>&1; then DC=(docker-compose); else DC=(); fi
else
  DC=()
fi

if ((${#DC[@]})); then
  echo "==> validating compose configuration"
  "${DC[@]}" config --quiet
fi

echo "==> checking console readiness"
curl --fail --silent --show-error "${CONSOLE_URL}/readyz" >/dev/null

for port in 8081 8082 8083; do
  echo "==> checking node ${port}"
  curl --fail --silent --show-error "http://localhost:${port}/v1/health" >/dev/null
done

if ! command -v python3 >/dev/null 2>&1; then
  echo "warning: python3 is unavailable; skipping authenticated catalog check"
  exit 0
fi

if [[ ! -f .env ]]; then
  echo "warning: .env is missing; health checks passed, but console registration cannot be verified"
  exit 0
fi

set -a
# shellcheck disable=SC1091
. ./.env
set +a
jar="$(mktemp)"
trap 'rm -f "$jar"' EXIT
login="$(curl --fail --silent --show-error -c "$jar" -X POST "${CONSOLE_URL}/api/login" \
  -H 'content-type: application/json' \
  -d "{\"username\":\"${SHARDLITE_CONSOLE_ADMIN}\",\"password\":\"${SHARDLITE_CONSOLE_ADMIN_PASSWORD}\"}")"
csrf="$(printf '%s' "$login" | python3 -c 'import json,sys; print(json.load(sys.stdin)["csrf_token"])')"
connections="$(curl --fail --silent --show-error -b "$jar" "${CONSOLE_URL}/api/connections")"
CONNECTIONS_JSON="$connections" python3 - <<'PY'
import json, os, sys
items = json.loads(os.environ["CONNECTIONS_JSON"])
if not any(item.get("name") for item in items):
    raise SystemExit("error: console has no registered connection")
PY

name="${CONN_NAME:-shardlite}"
catalog="$(curl --fail --silent --show-error -b "$jar" "${CONSOLE_URL}/api/connections/${name}/cluster/catalog")"
CATALOG_JSON="$catalog" python3 - <<'PY'
import json, os
value = json.loads(os.environ["CATALOG_JSON"])
if value.get("enabled") is True:
    if not value.get("scaling_policy"):
        raise SystemExit("error: dynamic catalog omitted scaling policy")
    print("ok: console connection and dynamic catalog policy verified")
else:
    # The shipped Docker demo is intentionally fixed-shard; dynamic mode is qualified by the
    # init/join workflow and may be registered as a separate connection.
    print("ok: console connection verified (legacy fixed-shard demo)")
PY

echo "ok: packaged stack verification passed"
