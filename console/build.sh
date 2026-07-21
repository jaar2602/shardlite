#!/usr/bin/env bash
# Build the console as one self-contained binary: frontend first (produces web/dist), then the
# backend (which embeds web/dist). Pass --release for an optimized build.
set -euo pipefail
cd "$(dirname "$0")"

PROFILE="${1:-}"

echo "==> building frontend (web/dist)"
if [ -d web/node_modules ]; then
  npm --prefix web run build
else
  npm --prefix web ci && npm --prefix web run build
fi

echo "==> building backend (embeds web/dist)"
if [ "$PROFILE" = "--release" ]; then
  cargo build --manifest-path server/Cargo.toml --release
  echo "==> built: server/target/release/meshdb-console"
else
  cargo build --manifest-path server/Cargo.toml
  echo "==> built: server/target/debug/meshdb-console"
fi
