"""Run against a live gateway: MESHDB_PORT=NNNN python3 example.py"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from meshdb import Client

db = Client(f"http://127.0.0.1:{os.environ.get('MESHDB_PORT', '4680')}")
db.execute_all("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT) STRICT")
db.tx([("INSERT INTO t VALUES (?, ?)", [1, "alice"]),
       ("INSERT INTO t VALUES (?, ?)", [2, "bob"])])
for row in db.query("SELECT id, v FROM t ORDER BY id"):
    print(row["id"], row["v"])
print("info:", db.info())
