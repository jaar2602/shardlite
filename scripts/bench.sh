#!/usr/bin/env bash
#
# Benchmarks and the memory check, under the floor profile.
#
#   ./scripts/bench.sh
#
# Runs in a 1 CPU / 1 GB container, because numbers from a many-core host are
# meaningless for a design targeting one shared CPU — and because this host's /tmp
# completes an fsync in 0.3us, which makes the entire group-commit result vanish.
# Measured: host /tmp ~0.3us per fsync, container overlay ~1.5ms. Only the second is
# a real durability cost.

set -uo pipefail
cd "$(dirname "$0")/.."

IMAGE=${IMAGE:-fedora:latest}
CPUS=${CPUS:-1}
MEM=${MEM:-1g}
MEM_LIMIT=${MEM_LIMIT:-150m}

command -v docker >/dev/null || { echo "docker required"; exit 1; }

echo "building release binaries..."
cargo build --release --bins --benches --quiet || exit 1
BENCH=$(ls target/release/deps/write_throughput-* 2>/dev/null | grep -v '\.d$' | head -1)
[[ -n "$BENCH" ]] || { echo "bench binary not found"; exit 1; }

echo
echo "############ write throughput — ${CPUS} CPU, ${MEM} ############"
docker run --rm --cpus="$CPUS" --memory="$MEM" -v "$PWD":/w -w /w "$IMAGE" "$BENCH"

echo
echo "############ peak RSS — ${CPUS} CPU, HARD ${MEM_LIMIT} cap ############"
echo "-- 256 MB database across 64 shards"
docker run --rm --cpus="$CPUS" --memory="$MEM_LIMIT" -v "$PWD":/w -w /w "$IMAGE" \
    ./target/release/memcheck /tmp/mc 64 4 150 || exit 1

echo
echo "-- 1.3 GB database across 64 shards (RSS must not track data size)"
docker run --rm --cpus="$CPUS" --memory="$MEM_LIMIT" -v "$PWD":/w -w /w "$IMAGE" \
    ./target/release/memcheck /tmp/mc 64 16 150 || exit 1
