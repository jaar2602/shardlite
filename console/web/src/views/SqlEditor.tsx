import { useState } from "react";
import * as api from "../lib/api";
import { Banner, Button, DataTable, Select, Spinner, TextInput } from "../components/ui";

// Whether a statement is a read (streams a grid) or a write/DDL (returns a count). A crude but
// reliable check on the leading keyword — the same distinction the gateway itself draws.
function isRead(sql: string): boolean {
  const head = sql.trimStart().slice(0, 6).toLowerCase();
  return head.startsWith("select") || head.startsWith("with") || head.startsWith("pragma") || head.startsWith("explain");
}

export default function SqlEditor({ name }: { name: string }) {
  const c = api.conn(name);
  const [sql, setSql] = useState("SELECT 1");
  const [shard, setShard] = useState(0);
  const [consistency, setConsistency] = useState("linearizable");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<api.QueryResult | null>(null);
  const [changed, setChanged] = useState<string | null>(null);
  const [elapsed, setElapsed] = useState<number | null>(null);

  const run = async () => {
    setBusy(true);
    setError(null);
    setResult(null);
    setChanged(null);
    const started = performance.now();
    try {
      if (isRead(sql)) {
        const r = await c.query(sql, { shard, consistency });
        setResult(r);
      } else {
        const r = await c.execute(sql, shard);
        setChanged(`rows affected: ${r.rows_affected} · last insert rowid: ${r.last_insert_rowid}`);
      }
      setElapsed(Math.round(performance.now() - started));
    } catch (e) {
      setError(e instanceof Error ? e.message : "query failed");
    } finally {
      setBusy(false);
    }
  };

  const onKey = (e: React.KeyboardEvent) => {
    if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
      e.preventDefault();
      void run();
    }
  };

  return (
    <div className="p-6 space-y-4">
      <textarea
        className="w-full h-40 bg-carbon-field border border-carbon-border p-3 font-mono text-sm text-carbon-text outline-none focus:border-carbon-blue resize-y"
        value={sql}
        onChange={(e) => setSql(e.target.value)}
        onKeyDown={onKey}
        spellCheck={false}
      />
      <div className="flex items-end gap-4">
        <div className="w-28">
          <TextInput
            label="Shard"
            type="number"
            min={0}
            value={shard}
            onChange={(e) => setShard(Number(e.target.value))}
          />
        </div>
        <div className="w-44">
          <Select label="Consistency" value={consistency} onChange={(e) => setConsistency(e.target.value)}>
            <option value="linearizable">linearizable</option>
            <option value="stale">stale</option>
          </Select>
        </div>
        <Button onClick={() => void run()} disabled={busy}>
          {busy ? "Running…" : "Run  (⌘/Ctrl+Enter)"}
        </Button>
        {elapsed !== null && !busy && <span className="text-carbon-text-3 text-xs">{elapsed} ms</span>}
      </div>

      {busy && <Spinner label="Streaming results…" />}
      {error && <Banner tone="error">{error}</Banner>}
      {changed && <Banner tone="success">{changed}</Banner>}

      {result && (
        <div className="space-y-2">
          <div className="text-carbon-text-3 text-xs">
            {result.rows.length} row{result.rows.length === 1 ? "" : "s"}
            {result.truncated && " (display capped — the full result still streamed through the console)"}
          </div>
          <DataTable
            columns={result.columns.length ? result.columns : ["(no columns)"]}
            empty="No rows"
            rows={result.rows.map((r) => r.map((cell) => formatCell(cell)))}
          />
        </div>
      )}
    </div>
  );
}

function formatCell(v: unknown): string {
  if (v === null) return "NULL";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}
