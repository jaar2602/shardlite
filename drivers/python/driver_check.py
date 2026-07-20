import os, sys; sys.path.insert(0, os.path.dirname(__file__))
from meshdb import Client
db = Client(f"http://127.0.0.1:{os.environ['MESHDB_PORT']}")
n = sum(1 for _ in db.query("SELECT id FROM t ORDER BY id"))
print(f"streamed rows: {n}")
