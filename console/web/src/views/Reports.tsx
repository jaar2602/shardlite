import { useEffect, useState } from "react";
import { useAuth } from "../auth";
import * as api from "../lib/api";
import { Banner, Button, Card, DataTable, EmptyState, Page, PageHeader, Select, Spinner, Tag, TextInput } from "../components/ui";

const VIZ_OPTIONS = ["table", "bar", "line", "number"] as const;

function vizTone(viz: string): "gray" | "blue" | "green" | "yellow" {
  return viz === "number" ? "green" : viz === "bar" || viz === "line" ? "blue" : "gray";
}

function time(ms: number): string {
  return ms ? new Date(ms).toLocaleString() : "never";
}

// Cell formatting mirrors the SQL editor: NULL is explicit, objects are JSON, everything else is a string.
function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

type Form = { name: string; description: string; connection: string; sql: string; viz: string };
const emptyForm: Form = { name: "", description: "", connection: "", sql: "", viz: "table" };

export default function Reports() {
  const { me } = useAuth();
  const canWrite = api.permits(me?.role, "write");
  const [list, setList] = useState<api.Report[] | null>(null);
  const [connections, setConnections] = useState<api.Connection[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);
  const [editing, setEditing] = useState<string | null>(null);
  const [form, setForm] = useState<Form>(emptyForm);
  const [running, setRunning] = useState<string | null>(null);
  const [results, setResults] = useState<Record<string, { ok: boolean; message?: string; data?: api.MaterializedQueryResult; viz: string }>>({});

  const load = async () => {
    try {
      const [reports, conns] = await Promise.all([api.reports.list(), api.connections.list().catch(() => [])]);
      setList(reports);
      setConnections(conns);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to load");
    }
  };
  useEffect(() => {
    void load();
  }, []);

  const startAdd = () => {
    setForm(emptyForm);
    setEditing(null);
    setAdding(true);
  };

  const startEdit = (report: api.Report) => {
    setForm({
      name: report.name,
      description: report.description,
      connection: report.connection ?? "",
      sql: report.sql,
      viz: report.viz || "table",
    });
    setEditing(report.id);
    setAdding(true);
  };

  const cancelEdit = () => {
    setAdding(false);
    setEditing(null);
    setForm(emptyForm);
  };

  const save = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    const body: api.ReportInput = {
      name: form.name,
      description: form.description,
      connection: form.connection || null,
      sql: form.sql,
      viz: form.viz,
    };
    try {
      if (editing) await api.reports.update(editing, body);
      else await api.reports.create(body);
      cancelEdit();
      void load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to save report");
    }
  };

  const run = async (report: api.Report) => {
    if (!report.connection) {
      setResults((r) => ({ ...r, [report.id]: { ok: false, message: "This report has no connection; edit it to pick one.", viz: report.viz } }));
      return;
    }
    setRunning(report.id);
    try {
      const data = await api.conn(report.connection).queryAll(report.sql);
      setResults((r) => ({ ...r, [report.id]: { ok: true, data, viz: report.viz } }));
    } catch (e) {
      setResults((r) => ({ ...r, [report.id]: { ok: false, message: e instanceof Error ? e.message : "query failed", viz: report.viz } }));
    } finally {
      setRunning(null);
    }
  };

  const remove = async (report: api.Report) => {
    if (!confirm(`Delete report "${report.name}"?`)) return;
    try {
      await api.reports.remove(report.id);
      setResults((r) => {
        const next = { ...r };
        delete next[report.id];
        return next;
      });
      void load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to delete report");
    }
  };

  return (
    <Page>
      <PageHeader
        eyebrow="Saved queries / visualizations"
        title="Reports"
        description="Save a SQL query against a database and render it as a table, chart, or single number. Run any report on demand."
        actions={canWrite && <Button onClick={adding ? cancelEdit : startAdd}>{adding ? "Close form" : "New report"}</Button>}
      />

      {error && <Banner tone="error">{error}</Banner>}

      {adding && canWrite && (
        <Card title={editing ? "Edit report" : "New report"}>
          <form onSubmit={save} className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            <TextInput label="Name" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} placeholder="Daily signups" required />
            <TextInput label="Description (optional)" value={form.description} onChange={(e) => setForm({ ...form, description: e.target.value })} placeholder="What this report shows" />
            <Select label="Connection (optional)" value={form.connection} onChange={(e) => setForm({ ...form, connection: e.target.value })}>
              <option value="">— none —</option>
              {connections.map((c) => <option key={c.name} value={c.name}>{c.name}</option>)}
            </Select>
            <Select label="Visualization" value={form.viz} onChange={(e) => setForm({ ...form, viz: e.target.value })}>
              {VIZ_OPTIONS.map((v) => <option key={v} value={v}>{v}</option>)}
            </Select>
            <label className="text-sm text-carbon-text sm:col-span-2">
              <span className="mb-1 block text-xs uppercase tracking-wide text-carbon-text-2">SQL</span>
              <textarea
                className="h-32 w-full resize-y border-b border-carbon-border bg-carbon-layer px-3 py-2 font-mono text-xs outline-none focus:border-carbon-blue"
                value={form.sql}
                onChange={(e) => setForm({ ...form, sql: e.target.value })}
                placeholder="SELECT status, COUNT(*) FROM orders GROUP BY status"
                required
              />
            </label>
            <div className="sm:col-span-2 flex gap-2">
              <Button type="submit">{editing ? "Update report" : "Save report"}</Button>
              <Button type="button" variant="ghost" onClick={cancelEdit}>Cancel</Button>
            </div>
          </form>
        </Card>
      )}

      {list === null ? (
        <Spinner label="Loading reports…" />
      ) : list.length === 0 ? (
        <EmptyState title="No reports yet" description={canWrite ? "Create a report to save a query and its visualization." : "No reports have been created yet."} action={canWrite && <Button onClick={startAdd}>New report</Button>} />
      ) : (
        <div className="grid gap-2 xl:grid-cols-2">
          {list.map((report) => (
            <ReportRecord
              key={report.id}
              report={report}
              canWrite={canWrite}
              running={running === report.id}
              result={results[report.id]}
              onRun={() => void run(report)}
              onEdit={() => startEdit(report)}
              onRemove={() => void remove(report)}
            />
          ))}
        </div>
      )}
    </Page>
  );
}

function ReportRecord({ report, canWrite, running, result, onRun, onEdit, onRemove }: {
  report: api.Report;
  canWrite: boolean;
  running: boolean;
  result?: { ok: boolean; message?: string; data?: api.MaterializedQueryResult; viz: string };
  onRun: () => void;
  onEdit: () => void;
  onRemove: () => void;
}) {
  return (
    <article className="border border-carbon-border border-l-4 border-l-carbon-blue bg-carbon-layer">
      <div className="p-3">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <h2 className="truncate text-lg font-semibold">{report.name}</h2>
            {report.description && <p className="mt-1 text-sm text-carbon-text-3">{report.description}</p>}
          </div>
          <Tag tone={vizTone(report.viz)}>{report.viz || "table"}</Tag>
        </div>
        <dl className="mt-3 grid grid-cols-3 gap-2 border-y border-carbon-border py-2.5 text-xs">
          <div><dt className="font-mono text-[9px] uppercase tracking-wider text-carbon-text-3">Connection</dt><dd className="mt-1 truncate font-mono">{report.connection ?? "none"}</dd></div>
          <div><dt className="font-mono text-[9px] uppercase tracking-wider text-carbon-text-3">Author</dt><dd className="mt-1 truncate font-mono">{report.created_by}</dd></div>
          <div><dt className="font-mono text-[9px] uppercase tracking-wider text-carbon-text-3">Updated</dt><dd className="mt-1 truncate font-mono">{time(report.updated_at)}</dd></div>
        </dl>
        <pre className="mt-2 overflow-x-auto whitespace-pre-wrap break-words font-mono text-xs text-carbon-text-2">{report.sql}</pre>
        {result && (
          <div className="mt-3 border-t border-carbon-border pt-3">
            {result.ok && result.data ? <ReportViz viz={result.viz} data={result.data} /> : <Banner tone="error">{result.message ?? "query failed"}</Banner>}
          </div>
        )}
      </div>
      <div className="flex flex-wrap items-center gap-1 border-t border-carbon-border p-2">
        <Button variant="ghost" disabled={running} onClick={onRun}>{running ? "Running…" : "Run"}</Button>
        {canWrite && <><Button className="ml-auto" variant="ghost" onClick={onEdit}>Edit</Button><Button variant="ghost" className="text-carbon-red" onClick={onRemove}>Delete</Button></>}
      </div>
    </article>
  );
}

// Render a query result according to the report's chosen visualization.
export function ReportViz({ viz, data }: { viz: string; data: api.MaterializedQueryResult }) {
  if (data.rows.length === 0) return <div className="px-4 py-6 text-center text-sm text-carbon-text-3">No rows</div>;

  if (viz === "number") {
    return <div className="font-mono text-4xl leading-none text-carbon-blue">{formatCell(data.rows[0][0])}</div>;
  }
  if (viz === "bar" || viz === "line") {
    return <Chart kind={viz} data={data} />;
  }
  // Default: a table.
  return <DataTable columns={data.columns} rows={data.rows.map((row) => row.map(formatCell))} />;
}

// The label is the first column; the value is the last column that parses as a number.
function seriesFrom(data: api.MaterializedQueryResult): { labels: string[]; values: number[] } | null {
  const first = data.rows[0];
  let valueCol = -1;
  for (let i = first.length - 1; i >= 0; i--) {
    if (Number.isFinite(Number(first[i]))) { valueCol = i; break; }
  }
  if (valueCol < 0) return null;
  const labels = data.rows.map((row) => formatCell(row[0]));
  const values = data.rows.map((row) => Number(row[valueCol]) || 0);
  return { labels, values };
}

// A minimal inline SVG bar/line chart — no charting library, matching the Sparkline approach.
function Chart({ kind, data }: { kind: "bar" | "line"; data: api.MaterializedQueryResult }) {
  const series = seriesFrom(data);
  if (!series) return <DataTable columns={data.columns} rows={data.rows.map((row) => row.map(formatCell))} />;
  const { labels, values } = series;
  const width = 480;
  const height = 160;
  const pad = 24;
  const max = Math.max(...values, 1);
  const min = Math.min(...values, 0);
  const span = max - min || 1;
  const y = (v: number) => height - pad - ((v - min) / span) * (height - 2 * pad);
  const step = values.length > 1 ? (width - 2 * pad) / (values.length - 1) : 0;
  const barW = values.length > 0 ? Math.max(2, (width - 2 * pad) / values.length - 4) : 0;

  return (
    <div>
      <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`${kind} chart`} className="w-full">
        <line x1={pad} y1={height - pad} x2={width - pad} y2={height - pad} stroke="#525252" strokeWidth="1" />
        {kind === "bar"
          ? values.map((v, i) => {
              const x = pad + (i * (width - 2 * pad)) / values.length + 2;
              const top = y(v);
              return <rect key={i} x={x} y={top} width={barW} height={height - pad - top} fill="#0f62fe" />;
            })
          : <polyline
              points={values.map((v, i) => `${(pad + i * step).toFixed(1)},${y(v).toFixed(1)}`).join(" ")}
              fill="none"
              stroke="#0f62fe"
              strokeWidth="1.5"
            />}
      </svg>
      <div className="mt-1 flex justify-between font-mono text-[10px] text-carbon-text-3">
        <span className="truncate">{labels[0]}</span>
        {labels.length > 1 && <span className="truncate">{labels[labels.length - 1]}</span>}
      </div>
    </div>
  );
}
