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
DB="$DB_DIR/data"
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

# Shard count is immutable, so the CLI refuses to guess one. Create explicitly.
create() { "$BIN" "$1" --shards "$2" -c "SELECT 1" >/dev/null 2>&1; }

echo "building..."
cargo build --quiet 2>&1 | tail -5
[[ -x "$BIN" ]] || { echo "build failed: $BIN not found"; exit 1; }
echo "data dir: $DB"
echo

echo "shard count must be chosen explicitly"
assert_fails "new directory without --shards is refused" "--shards N is required" "$BIN" "$DB" -c "SELECT 1"
create "$DB" 1
assert_contains "created with --shards 1" "shard_count=1" cat "$DB/meshdb.manifest"
assert_fails "--shards on an existing directory that disagrees" "cannot change that" \
    "$BIN" "$DB" --shards 4 -c "SELECT 1"
assert_contains "opening an existing directory needs no --shards" "1" "$BIN" "$DB" -c "SELECT 1"
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
assert_fails "VACUUM points at the maintenance path" "Use the maintenance path" "$BIN" "$DB" -c "VACUUM"
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
assert_contains "repl .info"         "shard_count" bash -c "printf '.info\n.quit\n' | $BIN $DB"
assert_contains "repl .vacuum"       "ok"          bash -c "printf '.vacuum\n.quit\n' | $BIN $DB"
assert_contains "repl .stats wal"    "wal:"       bash -c "printf '.stats\n.quit\n' | $BIN $DB"
# Reads must be served by the pool, not by the writer thread.
assert_contains "select counted as a reader query" "queries=1" \
    bash -c "printf 'SELECT 1\n.stats\n.quit\n' | $BIN $DB"

echo
echo "manifest guards the immutable shard count"
SH_DIR=$(mktemp -d); trap 'rm -rf "$DB_DIR" "$SH_DIR"' EXIT
"$BIN" "$SH_DIR/multi" --shards 8 -c "CREATE TABLE t (id INTEGER PRIMARY KEY) STRICT" >/dev/null 2>&1
assert_contains "manifest records shard count" "shard_count=8" cat "$SH_DIR/multi/meshdb.manifest"
assert_fails "reopening with a different count is refused" "cannot change that" \
    "$BIN" "$SH_DIR/multi" --shards 4 -c "SELECT 1"
assert_contains "reopening with the same count works" "1" "$BIN" "$SH_DIR/multi" --shards 8 -c "SELECT 1"
# DDL must reach every shard, not just the one being targeted.
assert_contains "ddl fans out to all shards" "applied to 8 shards" \
    "$BIN" "$SH_DIR/multi" --shards 8 -c "CREATE TABLE fanout (a INTEGER) STRICT"

echo
echo "cross-shard reads"
Q="$SH_DIR/multi"
"$BIN" "$Q" -c "CREATE TABLE u (k TEXT PRIMARY KEY, n INTEGER) STRICT" >/dev/null 2>&1
for i in 0 1 2 3 4 5 6 7; do
    "$BIN" "$Q" -c "INSERT INTO u VALUES ('k$i', $i)" >/dev/null 2>&1
done
# All 8 rows land on shard 0 via the CLI, but the fan-out must still see them all.
assert_contains "count fans out"  "8"  "$BIN" "$Q" -c "SELECT count(*) FROM u"
assert_contains "sum fans out"    "28" "$BIN" "$Q" -c "SELECT sum(n) FROM u"
assert_contains "order by merges" "k0" "$BIN" "$Q" -c "SELECT k FROM u ORDER BY k LIMIT 1"
# A shape that cannot be combined must be refused, not answered from one shard.
assert_fails "AVG refused across shards"      "AVG"      "$BIN" "$Q" -c "SELECT avg(n) FROM u"
assert_fails "GROUP BY refused across shards" "GROUP BY" "$BIN" "$Q" -c "SELECT n, count(*) FROM u GROUP BY n"

echo
printf 'passed %d, failed %d\n' "$PASS" "$FAIL"
[[ $FAIL -eq 0 ]]
