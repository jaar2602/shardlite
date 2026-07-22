import { useCallback, useEffect, useState } from "react";
import * as api from "../lib/api";
import { Banner, Button, DataTable, Page, PageHeader, Spinner, StatCard, Tag } from "../components/ui";

const tone = (status: api.ClusterHealthStatus): "green" | "yellow" | "red" =>
  status === "healthy" ? "green" : status === "degraded" ? "yellow" : "red";

function time(value?: number | null): string {
  return value ? new Date(value).toLocaleTimeString() : "never";
}

export default function Overview({ name }: { name: string }) {
  const [observation, setObservation] = useState<api.ClusterObservation | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      setObservation(await api.conn(name).observation());
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to load cluster observation");
    }
  }, [name]);

  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(), 5000);
    return () => window.clearInterval(timer);
  }, [load]);

  if (!observation) {
    return <div className="p-6">{error ? <Banner tone="error">{error}</Banner> : <Spinner label="Waiting for observations…" />}</div>;
  }

  return (
    <Page>
      <PageHeader
        eyebrow="Database / current evidence"
        title={name}
        status={<Tag tone={tone(observation.status)}>{observation.status}</Tag>}
        description={`Observed through ${observation.seeds} database endpoint${observation.seeds === 1 ? "" : "s"}. Last successful evidence ${time(observation.last_success_ms)}.`}
        actions={<><span className="font-mono text-xs text-carbon-text-3">updates every 5s</span><Button variant="secondary" onClick={() => void load()}>Refresh now</Button></>}
      />
      {error && <Banner tone="error">Refresh failed; showing retained evidence. {error}</Banner>}
      {observation.issues.length > 0 && (
        <Banner tone="error">{observation.issues.join(" · ")}</Banner>
      )}
      <div className="grid grid-cols-2 gap-px bg-carbon-border lg:grid-cols-5">
        <StatCard label="Reachable endpoints" value={`${observation.reachable_nodes}/${observation.seeds}`} tone={observation.reachable_nodes === observation.seeds ? "green" : "red"} />
        <StatCard label="Observed nodes" value={observation.node_count} />
        <StatCard label="Availability" value={observation.status} tone={tone(observation.status)} />
        <StatCard label="Leader" value={observation.leader ?? "unknown"} />
        <StatCard label="Versions" value={observation.versions.join(", ") || "unknown"} />
      </div>
      <section aria-labelledby="node-evidence">
        <div className="mb-3 flex items-end justify-between gap-4"><div><h2 id="node-evidence" className="text-base font-semibold">Node evidence</h2><p className="mt-1 text-xs text-carbon-text-3">Each row is one configured endpoint, with no placement selection required.</p></div><span className="font-mono text-xs text-carbon-text-3">{observation.nodes.length} records</span></div>
        <DataTable columns={["Endpoint", "Node", "Health", "Role", "Term", "Leader", "Latency", "Last success", "Evidence"]} rows={observation.nodes.map((node) => [
          <span className={node.seed === observation.preferred_seed ? "text-carbon-blue" : ""}>
            {node.seed}{node.seed === observation.preferred_seed ? " · active" : ""}
          </span>,
          node.meta?.node == null ? "unknown" : String(node.meta.node),
          <Tag tone={node.error ? "red" : node.health?.status === "healthy" ? "green" : "yellow"}>
            {node.error ? "unreachable" : node.health?.status ?? "unknown"}
          </Tag>,
          node.topology?.role ?? node.health?.role ?? "unknown",
          node.topology?.term ?? node.health?.term ?? "—",
          node.topology?.leader == null ? "unknown" : String(node.topology.leader),
          node.latency_ms == null ? "—" : `${node.latency_ms} ms`,
          time(node.last_success_ms),
          <span className="max-w-80 whitespace-normal" title={node.error ?? "successful observation"}>
            {node.error ?? (node.health?.derived ? "legacy contract (derived)" : "direct contracts")}
          </span>,
        ])} />
      </section>
    </Page>
  );
}
