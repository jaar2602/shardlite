#!/usr/bin/env bash
#
# Load a dataset into a running shardlite stack, so the console has something to show.
#
#   ./ingest.sh                    default dataset (~26k rows)
#   ./ingest.sh --rows 100000      more rows
#   ./ingest.sh --workers 12       more concurrent writers
#   ./ingest.sh --fresh            drop the tables first
#   ./ingest.sh --node 8082        talk to one node only (default: rotate across all three)
#   ./ingest.sh --prefix demo_     name the tables demo_customers, demo_orders, … so the load
#                                  sits alongside data that is already in the volume
#
# The stack must already be up (./shardlite-stack up). Nothing here names a shard: every statement
# is ordinary SQL against one logical database, and the cluster routes it.
#
# The schema deliberately passes shardlite's DDL constraint gate — each table's shard key is its
# single-column PRIMARY KEY, and there are no UNIQUE or FOREIGN KEY constraints off that key,
# because those cannot be enforced across independent SQLite files. See docs/shard-alignment-plan.md.
set -uo pipefail
cd "$(dirname "$0")"

ROWS=26000
WORKERS=8
FRESH=0
NODES="8081 8082 8083"
PREFIX=""

while [ $# -gt 0 ]; do
  case "$1" in
    --rows)    ROWS="$2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    --node)    NODES="$2"; shift 2 ;;
    --fresh)   FRESH=1; shift ;;
    --prefix)  PREFIX="$2"; shift 2 ;;
    --help|-h) sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown option: $1 (try --help)" >&2; exit 2 ;;
  esac
done

command -v curl >/dev/null   || { echo "error: curl is required" >&2; exit 2; }
command -v python3 >/dev/null || { echo "error: python3 is required" >&2; exit 2; }

first_node="${NODES%% *}"
if ! curl -sf -m 3 "http://127.0.0.1:$first_node/v1/health" >/dev/null 2>&1; then
  echo "error: no shardlite node answering on http://127.0.0.1:$first_node" >&2
  echo "       start the stack first:  ./shardlite-stack up" >&2
  exit 1
fi

ROWS=$ROWS WORKERS=$WORKERS FRESH=$FRESH NODES="$NODES" PREFIX="$PREFIX" python3 <<'PY'
import json, os, sys, threading, time, urllib.request

GREEN, RED, DIM, BOLD, OFF = "\033[32m", "\033[31m", "\033[2m", "\033[1m", "\033[0m"
ROWS = int(os.environ["ROWS"])
WORKERS = int(os.environ["WORKERS"])
FRESH = os.environ["FRESH"] == "1"
PREFIX = os.environ.get("PREFIX", "")
PORTS = os.environ["NODES"].split()
URLS = [f"http://127.0.0.1:{p}" for p in PORTS]


def post(url, path, payload, timeout=120):
    req = urllib.request.Request(
        url + path, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {"error": body[:200]}
    except Exception as e:                       # noqa: BLE001 — surface it, don't abort mid-load
        return {"error": f"{type(e).__name__}: {e}"}


def run(sql, url=None):
    return post(url or URLS[0], "/v1/run", {"sql": sql})


def query(sql, url=None):
    return post(url or URLS[0], "/v1/query_all", {"sql": sql})


def fail(msg):
    print(f"  {RED}error{OFF} {msg}")
    sys.exit(1)


# ---------------------------------------------------------------- schema
# Every table is sharded by its own INTEGER PRIMARY KEY. Note what is deliberately absent:
# no UNIQUE on a non-key column and no FOREIGN KEY — the DDL gate refuses both on a sharded
# table, because neither can be enforced once rows live in different files.
T_CUST, T_ORD, T_EVT = f"{PREFIX}customers", f"{PREFIX}orders", f"{PREFIX}events"
TABLES = {
    T_CUST: (f"CREATE TABLE {T_CUST} (id INTEGER PRIMARY KEY, name TEXT, city TEXT, tier INTEGER)",
             ["id", "name", "city", "tier"]),
    T_ORD:  (f"CREATE TABLE {T_ORD} (id INTEGER PRIMARY KEY, customer_id INTEGER, item TEXT, amount INTEGER, day INTEGER)",
             ["id", "customer_id", "item", "amount", "day"]),
    T_EVT:  (f"CREATE TABLE {T_EVT} (id INTEGER PRIMARY KEY, kind TEXT, day INTEGER)",
             ["id", "kind", "day"]),
}


def blocked_by(table):
    """Tables holding a FOREIGN KEY to `table` — what stops a DROP when foreign_keys is ON."""
    reply = query("SELECT name FROM sqlite_master WHERE type='table' AND sql LIKE "
                  f"'%REFERENCES {table}%'")
    # The fan-out returns one row per shard, so the same table name comes back N times.
    return sorted({r[0] for r in reply.get("rows", [])})


def columns_of(table):
    reply = query(f"SELECT * FROM {table} LIMIT 0")
    return [c.lower() for c in reply.get("columns", [])]

print(f"\n{BOLD}Target{OFF}")
info = post(URLS[0], "/v1/info", {}) if False else None
try:
    with urllib.request.urlopen(URLS[0] + "/v1/info", timeout=10) as r:
        info = json.loads(r.read().decode())
except Exception as e:                            # noqa: BLE001
    fail(f"cannot read {URLS[0]}/v1/info: {e}")
shard_count = info.get("shard_count", 0)
print(f"  {len(URLS)} node(s) {DIM}{' '.join(URLS)}{OFF}")
print(f"  {shard_count} shards in one logical database")

print(f"\n{BOLD}Schema{OFF}")
if FRESH:
    # Reverse order so our own tables go before anything of ours that might reference them.
    for name in reversed(list(TABLES)):
        reply = run(f"DROP TABLE IF EXISTS {name}")
        if "error" in reply:
            holders = blocked_by(name)
            print(f"  {RED}could not drop {name}{OFF}: {reply['error'][:100]}")
            if holders:
                print(f"      {DIM}a FOREIGN KEY in {', '.join(holders)} points at it; "
                      f"drop those first{OFF}")
            fail("--fresh could not clear the tables")
    print(f"  {DIM}dropped existing tables (--fresh){OFF}")

for name, (ddl, want) in TABLES.items():
    reply = run(ddl)
    existed = "error" in reply and "already exists" in json.dumps(reply)
    if "error" in reply and not existed:
        fail(f"creating {name}: {reply['error'][:160]}")

    # An existing table is only usable if it is the table we expect. Accepting "already exists"
    # without this check is how a leftover table with the same name silently swallows the load
    # and every INSERT fails with "no such column".
    have = columns_of(name)
    missing = [c for c in want if c not in have]
    if missing:
        print(f"  {RED}✗{OFF}   {name} exists with a different schema "
              f"{DIM}(missing: {', '.join(missing)}){OFF}")
        print(f"      {DIM}it has: {', '.join(have) or '?'}{OFF}")
        holders = blocked_by(name)
        if holders:
            print(f"      {DIM}a FOREIGN KEY in {', '.join(holders)} blocks dropping it{OFF}")
        print(f"      {DIM}fix: re-run with --prefix demo_ to use separate tables,{OFF}")
        print(f"      {DIM}     or ./shardlite-stack destroy to start from an empty volume{OFF}")
        fail(f"{name} is not the table this script expects")
    print(f"  {GREEN}ok{OFF}   {name}{DIM}{'  (existing, schema matches)' if existed else ''}{OFF}")

# ---------------------------------------------------------------- volume plan
customers = max(1, ROWS // 6)
orders = max(1, (ROWS * 3) // 6)
events = max(1, ROWS - customers - orders)
CITIES = ["london", "lisbon", "osaka", "lima", "oslo", "lagos"]
ITEMS = ["book", "cable", "desk", "lamp", "mug", "pen", "chair"]
KINDS = ["login", "view", "click", "purchase", "logout"]
BATCH = 200

work = []
for lo in range(1, customers + 1, BATCH):
    work.append((T_CUST, lo, min(lo + BATCH - 1, customers)))
for lo in range(1, orders + 1, BATCH):
    work.append((T_ORD, lo, min(lo + BATCH - 1, orders)))
for lo in range(1, events + 1, BATCH):
    work.append((T_EVT, lo, min(lo + BATCH - 1, events)))

done = [0]
errors = []
lock = threading.Lock()


def rows_for(table, lo, hi):
    if table == T_CUST:
        return ",".join(
            f"({i}, 'cust-{i}', '{CITIES[i % len(CITIES)]}', {i % 4})" for i in range(lo, hi + 1))
    if table == T_ORD:
        return ",".join(
            f"({i}, {1 + (i % max(1, customers))}, '{ITEMS[i % len(ITEMS)]}', {10 + (i % 90)}, {i % 30})"
            for i in range(lo, hi + 1))
    return ",".join(
        f"({i}, '{KINDS[i % len(KINDS)]}', {i % 30})" for i in range(lo, hi + 1))


COLUMNS = {
    T_CUST: "(id, name, city, tier)",
    T_ORD: "(id, customer_id, item, amount, day)",
    T_EVT: "(id, kind, day)",
}


def worker(index):
    # Rotate nodes: any node accepts any write and routes it to the shard's owner.
    url = URLS[index % len(URLS)]
    while True:
        with lock:
            if not work:
                return
            table, lo, hi = work.pop()
        reply = run(f"INSERT INTO {table} {COLUMNS[table]} VALUES {rows_for(table, lo, hi)}", url)
        with lock:
            if "error" in reply:
                errors.append(f"{table} {lo}-{hi}: {reply['error'][:120]}")
            else:
                done[0] += hi - lo + 1


total = customers + orders + events
print(f"\n{BOLD}Ingest{OFF}")
print(f"  {DIM}{total} rows in batches of {BATCH}, {WORKERS} concurrent writers, "
      f"rotating across {len(URLS)} node(s){OFF}")

start = time.time()
threads = [threading.Thread(target=worker, args=(w,), daemon=True) for w in range(WORKERS)]
for t in threads:
    t.start()
while any(t.is_alive() for t in threads):
    time.sleep(0.5)
    with lock:
        n = done[0]
    pct = (n * 100) // total if total else 100
    sys.stdout.write(f"\r  {n}/{total} rows ({pct}%)   ")
    sys.stdout.flush()
for t in threads:
    t.join()
elapsed = time.time() - start
sys.stdout.write("\r" + " " * 44 + "\r\n")

if errors:
    print(f"  {RED}{len(errors)} batch error(s){OFF}")
    for e in errors[:5]:
        print(f"      {e}")
rate = done[0] / elapsed if elapsed else 0
print(f"  {GREEN}ok{OFF}   {done[0]} rows in {elapsed:.1f}s {DIM}(~{rate:.0f} rows/s){OFF}")

# ---------------------------------------------------------------- verify
print(f"\n{BOLD}What landed{OFF}")
for name in TABLES:
    reply = query(f"SELECT COUNT(*) FROM {name}")
    n = reply.get("rows", [[0]])[0][0] if "rows" in reply else "?"
    print(f"  {name:<10} {n}")

print(f"\n{BOLD}Spread across shards{OFF}")
counts = []
for s in range(shard_count):
    try:
        req = urllib.request.Request(
            URLS[0] + "/v1/query",
            data=json.dumps({"shard": s, "sql": f"SELECT COUNT(*) FROM {T_ORD}"}).encode(),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as r:
            lines = [l for l in r.read().decode().splitlines() if l]
        counts.append(json.loads(lines[1])[0] if len(lines) > 1 else 0)
    except Exception:                              # noqa: BLE001
        counts.append(0)
print(f"  {T_ORD} per shard: {' '.join(str(c) for c in counts)}")
if counts and max(counts) and sum(counts):
    skew = max(counts) / (sum(counts) / len(counts))
    print(f"  {DIM}{len(counts)} single-writer SQLite files, one logical table "
          f"(max/mean {skew:.2f}){OFF}")

print(f"\n{BOLD}Try these — in the console, or with curl{OFF}")
for sql in [
    f"SELECT COUNT(*) FROM {T_ORD}",
    f"SELECT city, COUNT(*) FROM {T_CUST} GROUP BY city",
    f"SELECT item, SUM(amount) FROM {T_ORD} GROUP BY item",
    f"SELECT id, amount FROM {T_ORD} ORDER BY amount DESC, id LIMIT 10",
    f"SELECT name FROM {T_CUST} WHERE id = 42",
]:
    print(f"  {DIM}{sql}{OFF}")
print()
PY
