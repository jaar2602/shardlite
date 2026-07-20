#!/usr/bin/env bash
#
# Repeatedly run the test suite and keep the evidence when something fails.
#
#   ./scripts/stress.sh              # 20 sequential runs
#   ./scripts/stress.sh 50           # 50 sequential runs
#   ./scripts/stress.sh 10 4         # 10 rounds of 4 suites running at once
#
# Exists because an intermittent failure was once seen and then lost: the loop that spotted
# it printed "FAILED" and discarded the output, so there was nothing left to diagnose. A
# check that detects a problem without capturing it turns a bug into a rumour.
#
# Every failing run's full output is kept. The point is not to go green; it is to leave
# something readable behind when it does not.

set -uo pipefail
cd "$(dirname "$0")/.."

ROUNDS=${1:-20}
CONCURRENCY=${2:-1}
OUT=${STRESS_DIR:-$(mktemp -d)}
mkdir -p "$OUT"

echo "running $ROUNDS rounds x $CONCURRENCY concurrent suites"
echo "output kept in: $OUT"
echo

# Build first, so compile time is not counted as a run and a build break is not reported as
# a test failure.
if ! cargo test --no-run > "$OUT/build.log" 2>&1; then
    echo "BUILD FAILED — see $OUT/build.log"
    grep -E "^error" -A6 "$OUT/build.log" | head -40
    exit 1
fi

FAILED=0
TOTAL=0

for round in $(seq 1 "$ROUNDS"); do
    pids=()
    for slot in $(seq 1 "$CONCURRENCY"); do
        log="$OUT/round${round}_slot${slot}.log"
        cargo test > "$log" 2>&1 &
        pids+=($!)
    done
    wait "${pids[@]}" 2>/dev/null

    for slot in $(seq 1 "$CONCURRENCY"); do
        log="$OUT/round${round}_slot${slot}.log"
        TOTAL=$((TOTAL + 1))

        # Test failures are checked first, and deliberately so: `cargo test` prints
        # "error: test failed, to rerun pass ..." for a failed assertion, which looks like a
        # build error to any pattern matching a leading "error". Checking builds first
        # classified every assertion failure as a toolchain problem and printed none of the
        # assertions — found by feeding this script a deliberately broken test.
        if grep -qE "^test .* FAILED" "$log"; then
            FAILED=$((FAILED + 1))
            echo "round $round slot $slot: TEST FAILURE  -> $log"
            grep -E "^test .* FAILED" "$log" | sed 's/^/    /'
            # The assertion message is the part worth reading; print it with context.
            grep -E "panicked at" -A4 "$log" | sed 's/^/    /' | head -24
            echo
            continue
        fi

        # A genuine compile break. Narrower than "starts with error" for the reason above.
        if grep -qE "^error\[E[0-9]+\]|^error: could not compile" "$log"; then
            FAILED=$((FAILED + 1))
            echo "round $round slot $slot: BUILD ERROR  -> $log"
            grep -E "^error" -A5 "$log" | head -12
            continue
        fi

        # Neither, yet cargo was unhappy: a timeout, a signal, a crashed harness. Worth
        # surfacing rather than passing over, because it is the shape of the failure that
        # went undiagnosed once already.
        if ! grep -qE "^test result: ok" "$log"; then
            FAILED=$((FAILED + 1))
            echo "round $round slot $slot: SUITE DID NOT COMPLETE  -> $log"
            tail -15 "$log" | sed 's/^/    /'
            echo
            continue
        fi

        rm -f "$log"
    done
    printf "."
done

echo
echo
if [ "$FAILED" -eq 0 ]; then
    echo "$TOTAL suite runs, no failures"
    rmdir "$OUT" 2>/dev/null
    exit 0
fi

echo "$TOTAL suite runs, $FAILED with failures"
echo "full output for each failure is in $OUT"
exit 1
