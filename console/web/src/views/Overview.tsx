import { useCallback, useEffect, useState } from "react";
import * as api from "../lib/api";
import { Banner, Button, DataTable, Page, PageHeader, Spinner, StatCard, Tag } from "../components/ui";

const tone = (status: api.ClusterHealthStatus): "green" | "yellow" | "red" =>
  status === "healthy" ? "green" : status === "degraded" ? "yellow" : "red";

function time(value?: number | null): string {
  return value ? new Date(value).toLocaleTimeString() : "never";
}

// Wall-clock ms → a short "3m ago" phrase; 0/absent means the reshuffle never happened.
function relativeTime(ms: number): string {
  if (!ms) return "never";
  const delta = Date.now() - ms;
  if (delta < 0) return "just now";
  const secs = Math.floor(delta / 1000);
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}

// Flatten nested stats into dotted numeric leaves, matching the Stats view's reader. Cluster
// counters (placement_changes, stepped_down) are read from the console's metrics history, which is
// loosely typed, so pull them defensively wherever they appear.
function flatten(obj: unknown, prefix = ""): Record<string, number> {
  const out: Record<string, number> = {};
  if (obj && typeof obj === "object") {
    for (const [k, v] of Object.entries(obj as Record<string, unknown>)) {
      const key = prefix ? `${prefix}.${k}` : k;
      if (typeof v === "number") out[key] = v;
      else if (v && typeof v === "object") Object.assign(out, flatten(v, key));
    }
  }
  return out;
}

function leaf(flat: Record<string, number>, name: string): number {
  for (const [k, v] of Object.entries(flat)) {
    if (k === name || k.endsWith(`.${name}`)) return v;
  }
  return 0;
}

// How much reshuffling happened recently: the delta of placement_changes / stepped_down over the
// samples inside the trailing 5-minute window, taken per source (each counter is per-node) and
// reduced to the worst node. Returns null when there is not enough history to judge.
const CHURN_WINDOW_MS = 5 * 60 * 1000;
function recentChurn(samples: api.MetricSample[] | null): { handovers: number; stepdowns: number; warning: boolean } | null {
  if (!samples) return null;
  const now = Date.now();
  const bySource = new Map<string, api.MetricSample[]>();
  for (const sample of samples) {
    if (now - sample.t > CHURN_WINDOW_MS) continue;
    const key = sample.source ?? "";
    bySource.set(key, [...(bySource.get(key) ?? []), sample]);
  }
  let handovers = 0;
  let stepdowns = 0;
  let seen = false;
  for (const series of bySource.values()) {
    if (series.length < 2) continue;
    seen = true;
    const first = flatten(series[0].stats);
    const last = flatten(series[series.length - 1].stats);
    handovers = Math.max(handovers, leaf(last, "placement_changes") - leaf(first, "placement_changes"));
    stepdowns = Math.max(stepdowns, leaf(last, "stepped_down") - leaf(first, "stepped_down"));
  }
  if (!seen) return null;
  handovers = Math.max(0, handovers);
  stepdowns = Math.max(0, stepdowns);
  return { handovers, stepdowns, warning: handovers >= 3 || stepdowns >= 2 };
}

export default function Overview({ name }: { name: string }) {
  const [observation, setObservation] = useState<api.ClusterObservation | null>(null);
  const [cluster, setCluster] = useState<api.ClusterInfo | null>(null);
  const [samples, setSamples] = useState<api.MetricSample[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const c = api.conn(name);
      // Observation is the page's primary evidence; the cluster snapshot and metrics history feed the
      // stability card and must degrade to null (not crash the page) when unavailable.
      const [obs, clusterInfo, metricSamples] = await Promise.all([
        c.observation(),
        c.cluster().catch(() => null),
        c.metrics().catch(() => null),
      ]);
      setObservation(obs);
      setCluster(clusterInfo);
      setSamples(metricSamples);
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
      {cluster?.clustered && (() => {
        const stats = (cluster.stats ?? {}) as Record<string, unknown>;
        const placementChanges = Number(stats.placement_changes ?? 0);
        const lastChangeMs = Number(stats.last_change_ms ?? 0);
        const electionsStarted = Number(stats.elections_started ?? 0);
        const steppedDown = Number(stats.stepped_down ?? 0);
        const handoverFailed = Number(stats.handover_failed ?? 0);
        const churn = recentChurn(samples);
        return (
          <section aria-labelledby="cluster-stability">
            <div className="mb-3 flex items-end justify-between gap-4">
              <div>
                <h2 id="cluster-stability" className="text-base font-semibold">Cluster stability</h2>
                <p className="mt-1 text-xs text-carbon-text-3">Leadership and placement churn for the connected node.</p>
              </div>
              {churn == null ? (
                <Tag tone="gray">collecting history…</Tag>
              ) : churn.warning ? (
                <Tag tone="red">reshuffling frequently</Tag>
              ) : (
                <Tag tone="green">stable</Tag>
              )}
            </div>
            {churn?.warning && (
              <Banner tone="error">cluster is reshuffling frequently — check for flapping links or an unstable leader ({churn.handovers} handover{churn.handovers === 1 ? "" : "s"} and {churn.stepdowns} step-down{churn.stepdowns === 1 ? "" : "s"} in the last 5 minutes).</Banner>
            )}
            <div className="grid grid-cols-2 gap-px bg-carbon-border lg:grid-cols-5">
              <StatCard label="Leader" value={cluster.leader == null ? "unknown" : String(cluster.leader)} detail={cluster.term == null ? undefined : `term ${cluster.term}`} />
              <StatCard label="Shard handovers" value={placementChanges} detail={`last change ${relativeTime(lastChangeMs)}`} tone={churn?.warning ? "yellow" : undefined} />
              <StatCard label="Elections started" value={electionsStarted} />
              <StatCard label="Step-downs" value={steppedDown} tone={churn?.warning ? "yellow" : undefined} />
              <StatCard label="Handover failures" value={handoverFailed} tone={handoverFailed > 0 ? "red" : undefined} />
            </div>
          </section>
        );
      })()}
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
