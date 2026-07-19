#!/usr/bin/env bash
#
# End-to-end smoke test for the meshdb CLI.
#
#   ./scripts/smoke.sh
#
# Builds the binary, drives it against a throwaway database, and asserts on the output.
# Exits non-zero on the first failure.

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=target/debug/meshdb
DB_DIR=$(mktemp -d)
DB="$DB_DIR/shard_0.db"
trap 'rm -rf "$DB_DIR"' EXIT

PASS=0
FAIL=0

pass() { printf '  \033[32mok\033[0m   %s\n' "$1"; PASS=$((PASS + 1)); }
fail() {
    printf '  \033[31mFAIL\033[0m %s\n' "$1"
    printf '       expected: %s\n' "$2"
    printf '       actual:   %s\n' "$3"
    FAIL=$((FAIL + 1))
}

# assert_contains <description> <expected-substring> <command...>
assert_contains() {
    local desc="$1" expected="$2"; shift 2
    local actual
    actual=$("$@" 2>&1)
    if [[ "$actual" == *"$expected"* ]]; then pass "$desc"; else fail "$desc" "$expected" "$actual"; fi
}

# assert_fails <description> <expected-substring> <command...>
assert_fails() {
    local desc="$1" expected="$2"; shift 2
    local actual rc
    actual=$("$@" 2>&1); rc=$?
    if [[ $rc -eq 0 ]]; then
        fail "$desc" "non-zero exit + '$expected'" "exit 0: $actual"
    elif [[ "$actual" == *"$expected"* ]]; then
        pass "$desc"
    else
        fail "$desc" "$expected" "$actual"
    fi
}

sql() { "$BIN" "$DB" -c "$1"; }

echo "building..."
cargo build --quiet 2>&1 | tail -5
[[ -x "$BIN" ]] || { echo "build failed: $BIN not found"; exit 1; }
echo "database: $DB"
echo

echo "schema and writes"
assert_contains "create table"      "ok"    "$BIN" "$DB" -c "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER) STRICT"
assert_contains "insert"            "1 row affected" "$BIN" "$DB" -c "INSERT INTO users VALUES (1, 'ada', 36)"
assert_contains "insert reports rowid" "last_insert_rowid=2" "$BIN" "$DB" -c "INSERT INTO users VALUES (2, 'grace', 45)"
assert_contains "update"            "1 row affected" "$BIN" "$DB" -c "UPDATE users SET age = 37 WHERE id = 1"

echo
echo "reads"
assert_contains "select returns data"   "grace"  "$BIN" "$DB" -c "SELECT name FROM users ORDER BY id"
assert_contains "select shows row count" "(2 rows)" "$BIN" "$DB" -c "SELECT * FROM users"
assert_contains "aggregate"             "82"     "$BIN" "$DB" -c "SELECT sum(age) FROM users"
assert_contains "update took effect"    "37"     "$BIN" "$DB" -c "SELECT age FROM users WHERE id = 1"

echo
echo "rejected statements (deterministic — a result, not a crash)"
assert_fails "duplicate primary key" "rejected"      "$BIN" "$DB" -c "INSERT INTO users VALUES (1, 'dup', 1)"
assert_fails "not-null violation"    "rejected"      "$BIN" "$DB" -c "INSERT INTO users VALUES (9, NULL, 1)"
assert_fails "unknown table"         "no such table" "$BIN" "$DB" -c "SELECT * FROM nope"
assert_fails "syntax error"          "rejected"      "$BIN" "$DB" -c "THIS IS NOT SQL"

echo
echo "unsupported statements (guarded, with a reason)"
# BEGIN is the important one: SQLite reports it as read-only, so without an explicit
# guard it would route to a reader, appear to succeed, and silently do nothing.
assert_fails "BEGIN rejected"   "writer owns transaction boundaries" "$BIN" "$DB" -c "BEGIN"
assert_fails "COMMIT rejected"  "unsupported"                        "$BIN" "$DB" -c "COMMIT"
assert_fails "VACUUM rejected"  "cannot run inside a transaction"    "$BIN" "$DB" -c "VACUUM"
assert_fails "ATTACH rejected"  "no atomic commit across"            "$BIN" "$DB" -c "ATTACH DATABASE 'x.db' AS x"

echo
echo "configuration is live"
assert_contains "WAL mode active"     "wal" "$BIN" "$DB" -c "PRAGMA journal_mode"
assert_contains "sqlite version"      "3."  "$BIN" "$DB" -c "SELECT sqlite_version()"

echo
echo "batch from a file"
cat > "$DB_DIR/batch.sql" <<'EOF'
-- comments and blank lines are skipped

INSERT INTO users VALUES (3, 'alan', 41)
INSERT INTO users VALUES (4, 'edsger', 52)
EOF
assert_contains "run -f" "ok" "$BIN" "$DB" -f "$DB_DIR/batch.sql"
assert_contains "file inserts landed" "(4 rows)" "$BIN" "$DB" -c "SELECT * FROM users"

echo
echo "interactive shell"
assert_contains "repl runs sql"    "ada"      bash -c "printf 'SELECT name FROM users WHERE id=1\n.quit\n' | $BIN $DB"
assert_contains "repl .tables"     "users"    bash -c "printf '.tables\n.quit\n' | $BIN $DB"
assert_contains "repl .stats writer" "mean_batch" bash -c "printf '.stats\n.quit\n' | $BIN $DB"
assert_contains "repl .stats reader" "threads=2"  bash -c "printf '.stats\n.quit\n' | $BIN $DB"
# Reads must be served by the pool, not by the writer thread.
assert_contains "select counted as a reader query" "queries=1" \
    bash -c "printf 'SELECT 1\n.stats\n.quit\n' | $BIN $DB"

echo
printf 'passed %d, failed %d\n' "$PASS" "$FAIL"
[[ $FAIL -eq 0 ]]
