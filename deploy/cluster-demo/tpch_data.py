#!/usr/bin/env python3
"""Generate a small, deterministic TPC-H-shaped dataset for the meshdb cluster demo.

This is NOT official TPC-H (no dbgen, no scale factor) — it is the same table shapes and the same
query shapes (Q1, Q6, plus a join and a grouped aggregate), scaled down to a few hundred rows so it
loads over HTTP in seconds and the whole thing fits a demo.

Design choices that matter for meshdb:
  * Every table has a SINGLE-column primary key, which meshdb makes the shard key automatically — no
    extra declaration, exactly like the users-table demo.
  * Money is INTEGER cents and discount is an INTEGER percent, with the discounted price
    precomputed. So every aggregate in Q1/Q6 is an integer sum, and a cross-shard merged answer is
    EXACTLY equal to a single-shard one — no floating-point drift to explain away.

Output: SQL statements, one per line, CREATE TABLEs first then INSERTs, on stdout. The same stream
loads into meshdb (over /v1/run) and into stock sqlite3 (the ground-truth check).
"""
import random

SEED = 424242
random.seed(SEED)

REGIONS = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]
NATIONS = [
    ("ALGERIA", 0), ("ARGENTINA", 1), ("BRAZIL", 1), ("CANADA", 1), ("EGYPT", 4),
    ("ETHIOPIA", 0), ("FRANCE", 3), ("GERMANY", 3), ("INDIA", 2), ("INDONESIA", 2),
    ("IRAN", 4), ("IRAQ", 4), ("JAPAN", 2), ("JORDAN", 4), ("KENYA", 0),
    ("MOROCCO", 0), ("MOZAMBIQUE", 0), ("PERU", 1), ("CHINA", 2), ("ROMANIA", 3),
    ("SAUDI ARABIA", 4), ("VIETNAM", 2), ("RUSSIA", 3), ("UNITED KINGDOM", 3),
    ("UNITED STATES", 1),
]
SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]
PRIORITIES = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]

N_CUSTOMERS = 40
N_ORDERS = 80


def q(s):
    return "'" + s.replace("'", "''") + "'"


def emit(stmt):
    print(stmt)


# --- schema (single-column PKs = shard keys) ---
emit("CREATE TABLE region (r_regionkey INTEGER PRIMARY KEY, r_name TEXT) STRICT")
emit("CREATE TABLE nation (n_nationkey INTEGER PRIMARY KEY, n_name TEXT, n_regionkey INTEGER) STRICT")
emit(
    "CREATE TABLE customer (c_custkey INTEGER PRIMARY KEY, c_name TEXT, c_nationkey INTEGER, "
    "c_acctbal_cents INTEGER, c_mktsegment TEXT) STRICT"
)
emit(
    "CREATE TABLE orders (o_orderkey INTEGER PRIMARY KEY, o_custkey INTEGER, o_orderstatus TEXT, "
    "o_totalprice_cents INTEGER, o_orderdate TEXT, o_orderpriority TEXT) STRICT"
)
emit(
    "CREATE TABLE lineitem (l_key INTEGER PRIMARY KEY, l_orderkey INTEGER, l_partkey INTEGER, "
    "l_suppkey INTEGER, l_quantity INTEGER, l_extendedprice_cents INTEGER, l_discount_pct INTEGER, "
    "l_disc_price_cents INTEGER, l_tax_pct INTEGER, l_returnflag TEXT, l_linestatus TEXT, "
    "l_shipdate TEXT) STRICT"
)

# meshdb routes an INSERT by its shard key, so every INSERT must LIST its columns — an unlisted
# `INSERT ... VALUES (...)` is refused because the key's position cannot be assumed. We spell the
# columns out everywhere (which is good SQL hygiene regardless).

# --- region / nation (canonical) ---
for k, name in enumerate(REGIONS):
    emit(f"INSERT INTO region (r_regionkey, r_name) VALUES ({k}, {q(name)})")
for k, (name, rk) in enumerate(NATIONS):
    emit(f"INSERT INTO nation (n_nationkey, n_name, n_regionkey) VALUES ({k}, {q(name)}, {rk})")

# --- customers ---
for c in range(1, N_CUSTOMERS + 1):
    nk = random.randint(0, len(NATIONS) - 1)
    bal = random.randint(-99999, 999999)  # cents
    seg = random.choice(SEGMENTS)
    emit(
        "INSERT INTO customer (c_custkey, c_name, c_nationkey, c_acctbal_cents, c_mktsegment) "
        f"VALUES ({c}, {q(f'Customer#{c:04d}')}, {nk}, {bal}, {q(seg)})"
    )

# --- orders + lineitems ---
lkey = 0
for o in range(1, N_ORDERS + 1):
    cust = random.randint(1, N_CUSTOMERS)
    n_lines = random.randint(1, 4)
    # order date across ~7 years so Q1's shipdate cutoff and Q6's date range both bite
    year = random.randint(1992, 1998)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    odate = f"{year}-{month:02d}-{day:02d}"
    status = random.choice(["O", "F", "P"])
    prio = random.choice(PRIORITIES)
    order_total = 0
    lines = []
    for _ in range(n_lines):
        lkey += 1
        qty = random.randint(1, 50)
        price = qty * random.randint(90000, 110000)  # cents; ~$900-1100 per unit
        disc = random.randint(0, 10)  # percent
        disc_price = price * (100 - disc) // 100  # exact integer
        tax = random.randint(0, 8)
        rflag = random.choice(["A", "N", "R"])
        lstatus = "O" if year >= 1995 else "F"
        # ship a little after the order
        sy, sm, sd = year, month, day
        sdate = f"{sy}-{sm:02d}-{sd:02d}"
        order_total += disc_price
        lines.append(
            (lkey, o, random.randint(1, 200), random.randint(1, 20), qty, price, disc,
             disc_price, tax, rflag, lstatus, sdate)
        )
    emit(
        "INSERT INTO orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice_cents, "
        "o_orderdate, o_orderpriority) "
        f"VALUES ({o}, {cust}, {q(status)}, {order_total}, {q(odate)}, {q(prio)})"
    )
    for ln in lines:
        emit(
            "INSERT INTO lineitem (l_key, l_orderkey, l_partkey, l_suppkey, l_quantity, "
            "l_extendedprice_cents, l_discount_pct, l_disc_price_cents, l_tax_pct, l_returnflag, "
            "l_linestatus, l_shipdate) VALUES ("
            f"{ln[0]}, {ln[1]}, {ln[2]}, {ln[3]}, {ln[4]}, {ln[5]}, {ln[6]}, {ln[7]}, {ln[8]}, "
            f"{q(ln[9])}, {q(ln[10])}, {q(ln[11])})"
        )
