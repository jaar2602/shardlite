import { useEffect, useState } from "react";
import { useAuth } from "../auth";
import * as api from "../lib/api";
import { Banner, Button, Card, EmptyState, Page, PageHeader, Spinner, Tag, TextInput } from "../components/ui";
import { ReportViz } from "./Reports";

function time(ms: number): string {
  return ms ? new Date(ms).toLocaleString() : "never";
}

// New tiles are stacked in a single column: full width, four rows tall, one under the next.
function tileFor(reportId: string, index: number): api.Tile {
  return { report_id: reportId, x: 0, y: index * 4, w: 12, h: 4 };
}

type Form = { name: string; description: string; tiles: string[] };
const emptyForm: Form = { name: "", description: "", tiles: [] };

export default function Dashboards() {
  const { me } = useAuth();
  const canWrite = api.permits(me?.role, "write");
  const [list, setList] = useState<api.Dashboard[] | null>(null);
  const [reports, setReports] = useState<api.Report[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [viewing, setViewing] = useState<api.Dashboard | null>(null);
  const [adding, setAdding] = useState(false);
  const [editing, setEditing] = useState<string | null>(null);
  const [form, setForm] = useState<Form>(emptyForm);

  const load = async () => {
    try {
      const [dashboards, reportList] = await Promise.all([api.dashboards.list(), api.reports.list().catch(() => [])]);
      setList(dashboards);
      setReports(reportList);
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
    setViewing(null);
  };

  const startEdit = (dashboard: api.Dashboard) => {
    setForm({ name: dashboard.name, description: dashboard.description, tiles: dashboard.tiles.map((t) => t.report_id) });
    setEditing(dashboard.id);
    setAdding(true);
    setViewing(null);
  };

  const cancelEdit = () => {
    setAdding(false);
    setEditing(null);
    setForm(emptyForm);
  };

  const toggleTile = (reportId: string) => {
    setForm((f) => ({ ...f, tiles: f.tiles.includes(reportId) ? f.tiles.filter((id) => id !== reportId) : [...f.tiles, reportId] }));
  };

  const save = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    const body: api.DashboardInput = {
      name: form.name,
      description: form.description,
      tiles: form.tiles.map((reportId, index) => tileFor(reportId, index)),
    };
    try {
      if (editing) await api.dashboards.update(editing, body);
      else await api.dashboards.create(body);
      cancelEdit();
      void load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to save dashboard");
    }
  };

  const remove = async (dashboard: api.Dashboard) => {
    if (!confirm(`Delete dashboard "${dashboard.name}"?`)) return;
    try {
      await api.dashboards.remove(dashboard.id);
      if (viewing?.id === dashboard.id) setViewing(null);
      void load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to delete dashboard");
    }
  };

  if (viewing) {
    return <DashboardView dashboard={viewing} reports={reports} canWrite={canWrite} onBack={() => setViewing(null)} onEdit={() => startEdit(viewing)} onRemove={() => void remove(viewing)} />;
  }

  return (
    <Page>
      <PageHeader
        eyebrow="Composed reports"
        title="Dashboards"
        description="Group saved reports onto a single page. Open a dashboard to run every tile against its database."
        actions={canWrite && <Button onClick={adding ? cancelEdit : startAdd}>{adding ? "Close form" : "New dashboard"}</Button>}
      />

      {error && <Banner tone="error">{error}</Banner>}

      {adding && canWrite && (
        <Card title={editing ? "Edit dashboard" : "New dashboard"}>
          <form onSubmit={save} className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            <TextInput label="Name" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} placeholder="Operations overview" required />
            <TextInput label="Description (optional)" value={form.description} onChange={(e) => setForm({ ...form, description: e.target.value })} placeholder="What this dashboard shows" />
            <div className="sm:col-span-2">
              <div className="mb-1 text-xs uppercase tracking-wide text-carbon-text-2">Tiles</div>
              {reports.length === 0 ? (
                <p className="text-sm text-carbon-text-3">No reports exist yet. Create reports first, then add them here.</p>
              ) : (
                <div className="grid gap-1 sm:grid-cols-2">
                  {reports.map((report) => (
                    <label key={report.id} className="flex items-center gap-2 border border-carbon-border bg-carbon-layer px-3 py-2 text-sm text-carbon-text">
                      <input type="checkbox" checked={form.tiles.includes(report.id)} onChange={() => toggleTile(report.id)} />
                      <span className="min-w-0 flex-1 truncate">{report.name}</span>
                      <Tag tone="gray">{report.viz || "table"}</Tag>
                    </label>
                  ))}
                </div>
              )}
              <p className="mt-1 text-xs text-carbon-text-3">{form.tiles.length} tile{form.tiles.length === 1 ? "" : "s"} selected.</p>
            </div>
            <div className="sm:col-span-2 flex gap-2">
              <Button type="submit">{editing ? "Update dashboard" : "Save dashboard"}</Button>
              <Button type="button" variant="ghost" onClick={cancelEdit}>Cancel</Button>
            </div>
          </form>
        </Card>
      )}

      {list === null ? (
        <Spinner label="Loading dashboards…" />
      ) : list.length === 0 ? (
        <EmptyState title="No dashboards yet" description={canWrite ? "Create a dashboard to group reports onto one page." : "No dashboards have been created yet."} action={canWrite && <Button onClick={startAdd}>New dashboard</Button>} />
      ) : (
        <div className="grid gap-2 lg:grid-cols-2 2xl:grid-cols-3">
          {list.map((dashboard) => (
            <article key={dashboard.id} className="border border-carbon-border border-l-4 border-l-carbon-blue bg-carbon-layer">
              <div className="p-3">
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <h2 className="truncate text-lg font-semibold">{dashboard.name}</h2>
                    {dashboard.description && <p className="mt-1 text-sm text-carbon-text-3">{dashboard.description}</p>}
                  </div>
                  <Tag tone="blue">{dashboard.tiles.length} tile{dashboard.tiles.length === 1 ? "" : "s"}</Tag>
                </div>
                <dl className="mt-3 grid grid-cols-2 gap-2 border-t border-carbon-border pt-2.5 text-xs">
                  <div><dt className="font-mono text-[9px] uppercase tracking-wider text-carbon-text-3">Author</dt><dd className="mt-1 truncate font-mono">{dashboard.created_by}</dd></div>
                  <div><dt className="font-mono text-[9px] uppercase tracking-wider text-carbon-text-3">Updated</dt><dd className="mt-1 truncate font-mono">{time(dashboard.updated_at)}</dd></div>
                </dl>
              </div>
              <div className="flex flex-wrap items-center gap-1 border-t border-carbon-border p-2">
                <Button variant="ghost" onClick={() => setViewing(dashboard)}>Open</Button>
                {canWrite && <><Button className="ml-auto" variant="ghost" onClick={() => startEdit(dashboard)}>Edit</Button><Button variant="ghost" className="text-carbon-red" onClick={() => void remove(dashboard)}>Delete</Button></>}
              </div>
            </article>
          ))}
        </div>
      )}
    </Page>
  );
}

function DashboardView({ dashboard, reports, canWrite, onBack, onEdit, onRemove }: {
  dashboard: api.Dashboard;
  reports: api.Report[];
  canWrite: boolean;
  onBack: () => void;
  onEdit: () => void;
  onRemove: () => void;
}) {
  return (
    <Page>
      <PageHeader
        eyebrow="Dashboard"
        title={dashboard.name}
        description={dashboard.description || undefined}
        actions={<>
          <Button variant="ghost" onClick={onBack}>Back</Button>
          {canWrite && <><Button variant="secondary" onClick={onEdit}>Edit</Button><Button variant="ghost" className="text-carbon-red" onClick={onRemove}>Delete</Button></>}
        </>}
      />
      {dashboard.tiles.length === 0 ? (
        <EmptyState title="This dashboard has no tiles" description={canWrite ? "Edit the dashboard to add reports." : "No reports have been added yet."} />
      ) : (
        <div className="grid gap-2 xl:grid-cols-2">
          {dashboard.tiles.map((tile, index) => (
            <DashboardTile key={`${tile.report_id}-${index}`} tile={tile} known={reports.find((r) => r.id === tile.report_id)} />
          ))}
        </div>
      )}
    </Page>
  );
}

function DashboardTile({ tile, known }: { tile: api.Tile; known?: api.Report }) {
  const [report, setReport] = useState<api.Report | null>(known ?? null);
  const [data, setData] = useState<api.MaterializedQueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      setError(null);
      try {
        const r = await api.reports.get(tile.report_id);
        if (cancelled) return;
        setReport(r);
        if (!r.connection) {
          setError("Report has no connection.");
          return;
        }
        const result = await api.conn(r.connection).queryAll(r.sql);
        if (!cancelled) setData(result);
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : "failed to run tile");
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, [tile.report_id]);

  const viz = tile.viz || report?.viz || "table";
  return (
    <Card title={report?.name ?? "Report"} actions={report && <Tag tone="gray">{viz}</Tag>}>
      {loading ? <Spinner label="Running…" /> : error ? <Banner tone="error">{error}</Banner> : data ? <ReportViz viz={viz} data={data} /> : <div className="text-sm text-carbon-text-3">No result</div>}
    </Card>
  );
}
