import { useCallback, useEffect, useMemo, useState } from "react";
import { useAuth } from "../auth";
import * as api from "../lib/api";
import { Banner, Button, Card, Page, PageHeader, Spinner, StatCard, Tag, TextInput } from "../components/ui";

export default function ShardInventory({ name }: { name: string }) {
  const { me } = useAuth();
  const canOperate = api.permits(me?.role, "operate");
  const [inventory, setInventory] = useState<api.ShardInventory | null>(null);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [wal, setWal] = useState<Record<string, unknown> | null>(null);
  const [walBusy, setWalBusy] = useState(false);
  const [maintenance, setMaintenance] = useState<string | null>(null);
  const [maintenanceBusy, setMaintenanceBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");
  const [scrollTop, setScrollTop] = useState(0);
  const load = useCallback(async () => {
    try {
      const next = await api.conn(name).shardInventory();
      setInventory(next);
      setSelectedId((current) => current != null && next.rows.some((row) => row.id === current) ? current : next.rows[0]?.id ?? null);
      setError(null);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Storage diagnostics could not be loaded.");
    }
  }, [name]);
  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(), 5000);
    return () => window.clearInterval(timer);
  }, [load]);

  const rows = inventory?.rows ?? [];
  const filtered = useMemo(() => rows.filter((row) =>
    !filter || String(row.id).includes(filter) || (row.owner ?? "").toLowerCase().includes(filter.toLowerCase()) || row.state.includes(filter.toLowerCase()),
  ), [filter, rows]);
  const selected = rows.find((row) => row.id === selectedId);
  const unavailable = rows.filter((row) => row.state === "unavailable").length;
  const lagging = rows.filter((row) => (row.max_lag ?? 0) > 0).length;
  const rowHeight = 42;
  const viewport = 470;
  const start = Math.max(0, Math.floor(scrollTop / rowHeight) - 5);
  const visible = filtered.slice(start, start + Math.ceil(viewport / rowHeight) + 10);

  const inspectWal = async () => {
    if (!selected) return;
    setWalBusy(true);
    setError(null);
    try { setWal(await api.conn(name).frames(selected.id)); }
    catch (caught) { setError(caught instanceof Error ? caught.message : "WAL evidence could not be loaded."); }
    finally { setWalBusy(false); }
  };

  const maintain = async (label: string, action: () => Promise<unknown>, confirmMessage?: string) => {
    if (confirmMessage && !confirm(confirmMessage)) return;
    setMaintenanceBusy(true);
    setError(null);
    setMaintenance(null);
    try { setMaintenance(`${label}: ${JSON.stringify(await action())}`); }
    catch (caught) { setError(caught instanceof Error ? caught.message : `${label} failed.`); }
    finally { setMaintenanceBusy(false); }
  };

  if (!inventory) return <div className="p-6">{error ? <Banner tone="error">{error}</Banner> : <Spinner label="Loading storage diagnostics…" />}</div>;
  return <Page>
    <PageHeader eyebrow="Diagnostics / storage internals" title="Storage internals" description="Operator-only placement and replication evidence. Normal database work never requires selecting a storage unit." actions={<><span className="font-mono text-xs text-carbon-text-3">updates every 5s</span><Button variant="secondary" onClick={() => void load()}>Refresh now</Button></>} />
    <Banner tone="info">This view exposes physical implementation details for incident diagnosis. Use the SQL editor and Schema pages for normal database work.</Banner>
    {error && <Banner tone="error">{error}</Banner>}
    <div className="grid grid-cols-2 gap-px bg-carbon-border md:grid-cols-4">
      <StatCard label="Storage units" value={rows.length} />
      <StatCard label="Unavailable" value={unavailable} tone={unavailable ? "red" : "green"} />
      <StatCard label="Replication behind" value={lagging} tone={lagging ? "yellow" : "green"} />
      <StatCard label="Observed replicas" value={rows.reduce((sum, row) => sum + row.replicas.length, 0)} />
    </div>
    <div className="grid items-start gap-3 2xl:grid-cols-[minmax(680px,1fr)_22rem]">
      <section>
        <div className="mb-3 flex flex-wrap items-end justify-between gap-3"><div className="w-full max-w-sm"><TextInput label="Find a storage unit" placeholder="Unit ID, owner, or state" value={filter} onChange={(event) => { setFilter(event.target.value); setScrollTop(0); }} /></div><span className="font-mono text-xs text-carbon-text-3">showing {filtered.length} of {rows.length}</span></div>
        <div className="overflow-x-auto border border-carbon-border text-xs">
          <div className="min-w-[860px]">
            <div className="grid grid-cols-[80px_1fr_90px_110px_1.2fr_90px_110px] bg-carbon-layer2 px-3 py-2.5 font-mono text-[10px] uppercase tracking-wider text-carbon-text-2"><span>Unit</span><span>Primary node</span><span>Epoch</span><span>Primary LSN</span><span>Replicas</span><span>Max lag</span><span>State</span></div>
            <div className="relative overflow-y-auto" style={{ height: viewport }} onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}>
              {filtered.length === 0 && <div className="absolute inset-0 grid place-items-center text-sm text-carbon-text-3">No storage units match “{filter}”.</div>}
              <div style={{ height: filtered.length * rowHeight, position: "relative" }}>
                {visible.map((row, index) => <button key={row.id} onClick={() => { setSelectedId(row.id); setWal(null); setMaintenance(null); }} className={`absolute left-0 right-0 grid grid-cols-[80px_1fr_90px_110px_1.2fr_90px_110px] items-center border-t border-carbon-border px-3 text-left font-mono text-carbon-text hover:bg-carbon-layer2/60 ${selectedId === row.id ? "bg-carbon-layer2" : ""}`} style={{ top: (start + index) * rowHeight, height: rowHeight }}>
                  <span>{row.id}</span><span>{row.owner ?? row.primary_node ?? "unknown"}</span><span>{row.epoch}</span><span>{row.primary_lsn}</span>
                  <span className="truncate" title={row.replicas.map((replica) => `${replica.node}: e${replica.epoch}/lsn${replica.lsn}`).join(", ")}>{row.replicas.length ? row.replicas.map((replica) => `${replica.node}:${replica.lsn}`).join(", ") : "none observed"}</span>
                  <span>{row.max_lag ?? "—"}</span><span><Tag tone={row.state === "available" ? "green" : row.state === "unavailable" || row.state === "conflict" ? "red" : "yellow"}>{row.state}</Tag></span>
                </button>)}
              </div>
            </div>
          </div>
        </div>
      </section>
      <Card title={selected ? `Storage unit ${selected.id}` : "Storage unit"}>
        {!selected ? <p className="text-sm text-carbon-text-3">No storage evidence is available.</p> : <div className="space-y-3 text-xs">
          <Diagnostic label="Primary node" value={selected.owner ?? selected.primary_node ?? "unknown"} />
          <Diagnostic label="Replication state" value={selected.state} />
          <Diagnostic label="Evidence sources" value={selected.evidence} />
          <Button variant="secondary" disabled={walBusy} onClick={() => void inspectWal()}>{walBusy ? "Reading WAL…" : "Inspect WAL"}</Button>
          {wal && <pre className="max-h-80 overflow-auto border-t border-carbon-border pt-3 font-mono text-[11px] leading-5 text-carbon-text-2">{JSON.stringify(wal, null, 2)}</pre>}
          {canOperate && <div className="space-y-2 border-t border-carbon-border pt-3">
            <div className="text-[10px] uppercase tracking-wider text-carbon-text-3">Maintenance</div>
            <div className="flex gap-2">
              <Button variant="secondary" disabled={maintenanceBusy} onClick={() => void maintain("Vacuum", () => api.conn(name).vacuum(selected.id), `Vacuum storage unit ${selected.id}?`)}>Vacuum</Button>
              <Button variant="secondary" disabled={maintenanceBusy} onClick={() => void maintain("Checkpoint", () => api.conn(name).checkpoint(selected.id))}>Checkpoint</Button>
            </div>
            {maintenance && <pre className="max-h-40 overflow-auto font-mono text-[11px] leading-5 text-carbon-text-2">{maintenance}</pre>}
          </div>}
        </div>}
      </Card>
    </div>
  </Page>;
}

function Diagnostic({ label, value }: { label: string; value: React.ReactNode }) {
  return <div className="flex items-start justify-between gap-4 border-b border-carbon-border pb-2"><span className="text-carbon-text-3">{label}</span><span className="text-right font-mono">{value}</span></div>;
}
