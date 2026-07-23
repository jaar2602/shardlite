// Run against a live gateway: SHARDLITE_PORT=NNNN node example.mjs
import { Client } from "./shardlite.mjs";
const db = new Client(`http://127.0.0.1:${process.env.SHARDLITE_PORT ?? "4680"}`);
await db.executeAll("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT) STRICT");
await db.tx([{ sql: "INSERT INTO t VALUES (?, ?)", params: [1, "alice"] },
             { sql: "INSERT INTO t VALUES (?, ?)", params: [2, "bob"] }]);
for await (const row of db.query("SELECT id, v FROM t ORDER BY id"))
  console.log(row.id, row.v);
console.log("info:", await db.info());
