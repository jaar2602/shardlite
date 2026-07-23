import os, sys; sys.path.insert(0, os.path.dirname(__file__))
from shardlite import Client, TcpClient

db = Client(f"http://127.0.0.1:{os.environ['SHARDLITE_PORT']}")
n = sum(1 for _ in db.query("SELECT id FROM t ORDER BY id"))
print(f"streamed rows: {n}")

tcp_port = os.environ.get("SHARDLITE_TCP_PORT")
if tcp_port:
    tc = TcpClient("127.0.0.1", int(tcp_port))
    m = sum(1 for _ in tc.query("SELECT id FROM t ORDER BY id"))
    tc.close()
    print(f"tcp streamed rows: {m}")
