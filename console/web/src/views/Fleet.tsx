import { useCallback, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import * as api from "../lib/api";
import { Banner, Button, EmptyState, Page, PageHeader, Spinner, StatCard, Tag } from "../components/ui";

const tone = (status: api.ClusterHealthStatus): "green" | "yellow" | "red" =>
  status === "healthy" ? "green" : status === "degraded" ? "yellow" : "red";

function age(timestamp?: number | null): string {
  if (!timestamp) return "never";
  const seconds = Math.max(0, Math.round((Date.now() - timestamp) / 1000));
  return seconds < 60 ? `${seconds}s ago` : `${Math.floor(seconds / 60)}m ago`;
}

export default function Fleet() {
  const nav = useNavigate();
  const [clusters, setClusters] = useState<api.FleetSummary[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      setClusters(await api.fleet.list());
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to load fleet");
    }
  }, []);

  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(), 5000);
    return () => window.clearInterval(timer);
  }, [load]);

  if (!clusters) return <div className="p-8"><Spinner label="Loading fleet observations…" /></div>;
  const healthy = clusters.filter((cluster) => cluster.status === "healthy").length;
  const degraded = clusters.filter((cluster) => cluster.status === "degraded").length;
  const unavailable = clusters.filter((cluster) => cluster.status === "unavailable").length;

  return (
    <Page>
      <PageHeader
        eyebrow="Databases / live evidence"
        title="Every database, one signal"
        description="Health comes from timestamped node observations. Missing, stale, or conflicting evidence is never reported as healthy."
        actions={<><span className="font-mono text-xs text-carbon-text-3">updates every 5s</span><Button variant="secondary" onClick={() => void load()}>Refresh now</Button></>}
      />
      {error && <Banner tone="error">Refresh failed; showing retained evidence. {error}</Banner>}
      <div className="grid grid-cols-2 gap-px bg-carbon-border md:grid-cols-4">
        <StatCard label="Clusters" value={clusters.length} />
        <StatCard label="Healthy" value={healthy} tone="green" />
        <StatCard label="Degraded" value={degraded} tone={degraded ? "yellow" : undefined} />
        <StatCard label="Unavailable" value={unavailable} tone="red" />
      </div>
      <section aria-labelledby="fleet-clusters">
        <div className="mb-3 flex items-center justify-between"><h2 id="fleet-clusters" className="font-mono text-xs uppercase tracking-wider text-carbon-text-2">Database records</h2><span className="text-xs text-carbon-text-3">Select a record to inspect it</span></div>
        {clusters.length === 0 ? <EmptyState title="No databases connected" description="Add a ShardLite connection to begin collecting fleet health and topology evidence." action={<Button onClick={() => nav("/connections")}>Add a connection</Button>} /> : <div className="grid gap-2 lg:grid-cols-2 2xl:grid-cols-3">
          {clusters.map((cluster) => <ClusterRecord key={cluster.name} cluster={cluster} onOpen={() => nav(`/c/${encodeURIComponent(cluster.name)}/overview`)} />)}
        </div>}
      </section>
    </Page>
  );
}

function ClusterRecord({ cluster, onOpen }: { cluster: api.FleetSummary; onOpen: () => void }) {
  const rail = cluster.status === "healthy" ? "border-l-carbon-green" : cluster.status === "degraded" ? "border-l-carbon-yellow" : "border-l-carbon-red";
  return <article className={`border border-carbon-border border-l-4 bg-carbon-layer p-3 transition-colors hover:bg-carbon-layer2/40 ${rail}`}>
    <div className="flex items-start justify-between gap-4">
      <div className="min-w-0"><div className="truncate text-lg font-semibold text-carbon-text">{cluster.name}</div><div className="mt-1 font-mono text-[10px] uppercase tracking-wider text-carbon-text-3">evidence {age(cluster.last_success_ms)}</div></div>
      <Tag tone={tone(cluster.status)}>{cluster.status}{cluster.stale ? " · stale" : ""}</Tag>
    </div>
    <dl className="mt-3 grid grid-cols-3 gap-2 border-y border-carbon-border py-2.5">
      <RecordValue label="Nodes" value={`${cluster.reachable_nodes}/${cluster.node_count || cluster.seeds}`} />
      <RecordValue label="Leader" value={cluster.leader ?? "unknown"} />
      <RecordValue label="Version" value={cluster.versions.join(", ") || "unknown"} />
    </dl>
    <div className="mt-3 flex items-start justify-between gap-4 text-xs"><span className={`line-clamp-2 ${cluster.issues.length ? "text-carbon-yellow" : "text-carbon-text-3"}`}>{cluster.issues.join("; ") || "No active issues"}</span><button className="shrink-0 text-carbon-blue hover:underline focus-visible:outline focus-visible:outline-2 focus-visible:outline-carbon-blue" onClick={onOpen}>Open database →</button></div>
  </article>;
}

function RecordValue({ label, value }: { label: string; value: string }) {
  return <div className="min-w-0"><dt className="font-mono text-[9px] uppercase tracking-wider text-carbon-text-3">{label}</dt><dd className="mt-1 truncate font-mono text-xs text-carbon-text" title={value}>{value}</dd></div>;
}
