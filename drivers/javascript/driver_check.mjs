import { Client, TcpClient } from "./meshdb.mjs";

const db = new Client(`http://127.0.0.1:${process.env.MESHDB_PORT}`);
let n = 0; for await (const _ of db.query("SELECT id FROM t ORDER BY id")) n++;
console.log(`streamed rows: ${n}`);

const tcpPort = process.env.MESHDB_TCP_PORT;
if (tcpPort) {
  const tc = await TcpClient.connect("127.0.0.1", Number(tcpPort));
  let m = 0; for await (const _ of tc.query("SELECT id FROM t ORDER BY id")) m++;
  tc.close();
  console.log(`tcp streamed rows: ${m}`);
}
