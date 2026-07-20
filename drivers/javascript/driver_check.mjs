import { Client } from "./meshdb.mjs";
const db = new Client(`http://127.0.0.1:${process.env.MESHDB_PORT}`);
let n = 0; for await (const _ of db.query("SELECT id FROM t ORDER BY id")) n++;
console.log(`streamed rows: ${n}`);
