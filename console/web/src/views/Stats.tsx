import { useEffect, useMemo, useRef, useState } from "react";
import * as api from "../lib/api";
import { Banner, Button, Card, EmptyState, Page, PageHeader, Sparkline, Spinner, Tag, TextInput } from "../components/ui";
import ClusterTopologyPanel from "../components/ClusterTopologyPanel";
import {
  backpressureIndicator,
  batchingIndicator,
  durabilityIndicator,
  errorIndicator,
  flatten,
  formatBytes,
  formatRate,
  stabilityIndicator,
  vitalsFor,
  type Indicator,
  type NodeVitals,
  type Tone,
} from "../lib/health";

// Observability, not a counter dump.
//
// This page used to print every flattened counter as a sparkline. That is the raw material, but a
// reading of `writer.requests: 41293` answers no question an operator actually has: is it serving
// traffic, is it keeping up, is anything degrading. So the counters become rates and a small set of
// health indicators first, mapped onto the cluster so a problem has a location, with the full list
// kept underneath for when you already know which counter you want.

const TONE_TEXT: Record<Tone, string> = {
  green: "text-carbon-green",
  yellow: "text-carbon-yellow",
  red: "text-carbon-red",
  gray: "text-carbon-text-3",
};

const TONE_TAG: Record<Tone, "green" | "yellow" | "red" | "gray"> = {
  green: "green",
  yellow: "yellow",
  red: "red",
  gray: "gray",
};

function IndicatorCard({ indicator }: { indicator: Indicator }) {
  return (
    <div className="min-w-0 bg-carbon-layer p-4" title={indicator.note}>
      <div className="mb-1 truncate font-mono text-[10px] uppercase tracking-wider text-carbon-text-3">
        {indicator.label}
      </div>
      <div className={`truncate text-lg font-semibold ${TONE_TEXT[indicator.tone]}`}>{indicator.value}</div>
      <p className="mt-1 text-[11px] leading-4 text-carbon-text-3">{indicator.note}</p>
    </div>
  );
}

export default function Stats({ name }: { name: string }) {
  const c = api.conn(name);
  const [samples, setSamples] = useState<api.MetricSample[] | null>(null);
  const [inventory, setInventory] = useState<api.ShardInventory | null>(null);
  const [unresolved, setUnresolved] = useState<api.UnresolvedTransaction[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");
  const [showRaw, setShowRaw] = useState(false);
  const timer = useRef<number | null>(null);

  const load = async () => {
    try {
      const [metrics, inv, txns] = await Promise.all([
        c.metrics(),
        c.shardInventory().catch(() => null),
        c.transactions().catch(() => null),
      ]);
      setSamples(metrics);
      setInventory(inv);
      setUnresolved(txns?.unresolved ?? []);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to load metrics");
    }
  };

  useEffect(() => {
    void load();
    timer.current = window.setInterval(load, 5000);
    return () => {
      if (timer.current) window.clearInterval(timer.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [name]);

  const bySource = useMemo(() => {
    const map = new Map<string, api.MetricSample[]>();
    for (const sample of samples ?? []) {
      const source = sample.source || "legacy/default endpoint";
      map.set(source, [...(map.get(source) ?? []), sample]);
    }
    return map;
  }, [samples]);

  const vitals = useMemo<NodeVitals[]>(
    () => Array.from(bySource, ([source, list]) => vitalsFor(source, list)),
    [bySource],
  );

  // The topology panel keys by node id; samples key by source address. Match on a node id appearing
  // in the source string, which is how the console labels them.
  const vitalsByNode = useMemo(() => {
    const map = new Map<string, NodeVitals>();
    for (const entry of vitals) {
      map.set(entry.source, entry);
      const digits = entry.source.match(/node\s*(\d+)|\b(\d+)\b/);
      if (digits) map.set(digits[1] ?? digits[2], entry);
    }
    return map;
  }, [vitals]);

  if (error && !samples)
    return (
      <Page>
        <PageHeader eyebrow="Telemetry / sampled locally" title="Health & metrics" description="Live state of every reachable node." />
        <Banner tone="error">Metrics could not be loaded. {error}</Banner>
      </Page>
    );
  if (!samples) return <Page><Spinner label="Loading metrics…" /></Page>;
  if (samples.length === 0)
    return (
      <Page>
        <PageHeader eyebrow="Telemetry / sampled locally" title="Health & metrics" description="Live state of every reachable node." />
        <EmptyState title="Collecting the first sample" description="The console polls metrics every five seconds. Trends appear after the first node responds." />
      </Page>
    );

  const rows = inventory?.rows ?? [];
  const worstLag = rows.reduce((worst, row) => Math.max(worst, row.max_lag ?? 0), 0);
  const degraded = rows.filter((row) => row.state !== "available");
  const totalWrites = vitals.reduce((sum, v) => sum + v.writesPerSecond, 0);
  const totalReads = vitals.reduce((sum, v) => sum + v.readsPerSecond, 0);
  const minutes = Math.max(1, Math.round((samples.length * 5) / 60));

  const indicators: Indicator[] = [
    {
      label: "Throughput",
      value: `${formatRate(totalWrites)} w · ${formatRate(totalReads)} r`,
      tone: totalWrites + totalReads > 0 ? "green" : "gray",
      note: "Writes and reads per second across the cluster, measured across the sampling window.",
    },
    batchingIndicator(vitals),
    {
      label: "Replication",
      value: rows.length === 0 ? "unknown" : degraded.length ? `${degraded.length} degraded` : worstLag > 0 ? `${worstLag} behind` : "in sync",
      tone: rows.length === 0 ? "gray" : degraded.length > 0 ? "red" : worstLag > 0 ? "yellow" : "green",
      note:
        degraded.length > 0
          ? `Shards ${degraded.map((row) => row.id).join(", ")} are not available.`
          : "Worst replica lag across every shard — lag is what would be lost if a primary died now.",
    },
    backpressureIndicator(vitals),
    durabilityIndicator(vitals),
    {
      label: "In-doubt writes",
      value: unresolved.length === 0 ? "none" : `${unresolved.length}`,
      tone: unresolved.length === 0 ? "green" : "red",
      note:
        unresolved.length === 0
          ? "No cross-shard transaction is mid-flight."
          : "A multi-shard write may be visible on some shards and not others until recovery finishes.",
    },
    errorIndicator(vitals),
    stabilityIndicator(vitals),
  ];

  const worstTone: Tone = indicators.some((i) => i.tone === "red")
    ? "red"
    : indicators.some((i) => i.tone === "yellow")
      ? "yellow"
      : "green";

  const counterCount = Array.from(bySource.values()).reduce(
    (n, list) => Math.max(n, Object.keys(flatten(list[list.length - 1].stats)).length),
    0,
  );

  return (
    <Page>
      <PageHeader
        eyebrow="Telemetry / sampled locally"
        title="Health & metrics"
        description={`Derived from ${samples.length} samples over roughly ${minutes} minute${minutes === 1 ? "" : "s"}. History is collected by the console, not stored by shardlite.`}
        status={<Tag tone={TONE_TAG[worstTone]}>{worstTone === "green" ? "healthy" : worstTone === "yellow" ? "degraded" : "needs attention"}</Tag>}
        actions={<><span className="font-mono text-xs text-carbon-text-3">updates every 5s</span><Button variant="secondary" onClick={() => void load()}>Refresh now</Button></>}
      />
      {error && <Banner tone="error">Refresh failed; showing the last collected samples. {error}</Banner>}

      {/* What is wrong, before where. Each reading carries the threshold that makes it that colour,
          because a number without one is not actionable. */}
      <div className="grid grid-cols-2 gap-px bg-carbon-border md:grid-cols-4">
        {indicators.map((indicator) => <IndicatorCard key={indicator.label} indicator={indicator} />)}
      </div>

      {/* Then where. The same map as the Topology page, carrying this page's numbers. */}
      <Card title="Per-node load">
        <ClusterTopologyPanel
          name={name}
          caption={(nodeId) => {
            const v = vitalsByNode.get(nodeId);
            if (!v) return "no samples reaching this node";
            return `${formatRate(v.writesPerSecond)} w · ${formatRate(v.readsPerSecond)} r · batch ${v.meanBatch.toFixed(1)}`;
          }}
          annotateShard={(shard) => {
            const row = rows.find((entry) => entry.id === shard);
            if (!row) return null;
            return {
              tone: row.state !== "available" ? "red" : (row.max_lag ?? 0) > 0 ? "yellow" : "green",
              label: (row.max_lag ?? 0) > 0 ? `+${row.max_lag}` : undefined,
              title: `shard ${row.id} · ${row.state} · lsn ${row.primary_lsn} · ${row.replicas.length} replica(s)`,
            };
          }}
          details={(nodeId, shards) => {
            const v = vitalsByNode.get(nodeId);
            if (!v) return <span className="text-carbon-text-3">No metric samples are reaching this node — it may be unreachable from the console.</span>;
            const owned = shards.map((s) => rows.find((row) => row.id === s)).filter((r): r is api.ShardInventoryRow => !!r);
            const lag = owned.reduce((worst, row) => Math.max(worst, row.max_lag ?? 0), 0);
            const series = (bySource.get(v.source) ?? []).map((sample) => flatten(sample.stats)["writer.requests"] ?? 0);
            return (
              <div className="space-y-3">
                <dl className="grid grid-cols-[9rem_1fr] gap-x-3 gap-y-1">
                  <dt className="text-carbon-text-3">Writes</dt><dd className="font-mono">{formatRate(v.writesPerSecond)}</dd>
                  <dt className="text-carbon-text-3">Reads</dt><dd className="font-mono">{formatRate(v.readsPerSecond)}</dd>
                  <dt className="text-carbon-text-3">Batching</dt>
                  <dd className={`font-mono ${v.writesPerSecond > 1 && v.meanBatch <= 1 ? "text-carbon-red" : ""}`}>
                    {v.meanBatch.toFixed(2)} stmt/txn
                  </dd>
                  <dt className="text-carbon-text-3">Open shards</dt><dd className="font-mono">{v.openShards} of {shards.length} owned</dd>
                  <dt className="text-carbon-text-3">Worst lag</dt><dd className={`font-mono ${lag > 0 ? "text-carbon-yellow" : ""}`}>{lag}</dd>
                  <dt className="text-carbon-text-3">Reads shed</dt>
                  <dd className={`font-mono ${v.readersRejected + v.readersTimedOut > 0 ? "text-carbon-red" : ""}`}>
                    {v.readersRejected} busy · {v.readersTimedOut} timed out
                  </dd>
                  <dt className="text-carbon-text-3">WAL pending</dt>
                  <dd className="font-mono">
                    {formatBytes(v.walBytes)}
                    {v.checkpointStalls > 0 && <span className="text-carbon-yellow"> · {v.checkpointStalls} stalls</span>}
                  </dd>
                  {v.walContention > 0 && <>
                    <dt className="text-carbon-text-3">WAL contention</dt>
                    <dd className="font-mono text-carbon-yellow">{v.walContention} contended · max wait {v.walMaxWaitMs} ms</dd>
                  </>}
                  <dt className="text-carbon-text-3">Gateway</dt>
                  <dd className="font-mono">
                    {formatRate(v.httpPerSecond)}
                    {v.httpErrors > 0 && <span className="text-carbon-yellow"> · {v.httpErrors} errors</span>}
                  </dd>
                </dl>
                <div>
                  <div className="mb-1 font-mono text-[10px] uppercase tracking-wider text-carbon-text-3">writer.requests</div>
                  <Sparkline values={series} />
                </div>
              </div>
            );
          }}
        />
      </Card>

      {/* The raw feed, for when you already know which counter you want. */}
      <Card
        title="All counters"
        actions={<Button variant="ghost" onClick={() => setShowRaw((value) => !value)}>{showRaw ? "Hide" : `Show ${counterCount} per node`}</Button>}
      >
        {!showRaw ? (
          <p className="text-sm text-carbon-text-3">
            Every counter shardlite reports, per node, as a trend. The readings above are derived from these.
          </p>
        ) : (
          <div className="space-y-5">
            <div className="w-full max-w-sm">
              <TextInput label="Find a metric" placeholder="writer, checkpoint, reader…" value={filter} onChange={(event) => setFilter(event.target.value)} />
            </div>
            {Array.from(bySource).map(([source, sourceSamples]) => {
              const flats = sourceSamples.map((sample) => flatten(sample.stats));
              const keys = Array.from(new Set(flats.flatMap((flat) => Object.keys(flat)))).sort().filter((key) => key.toLowerCase().includes(filter.trim().toLowerCase()));
              return (
                <section key={source}>
                  <div className="mb-3 flex items-center justify-between border-b border-carbon-border pb-2">
                    <h2 className="truncate font-mono text-xs text-carbon-text-2" title={source}>{source}</h2>
                    <span className="text-xs text-carbon-text-3">{keys.length} metric{keys.length === 1 ? "" : "s"}</span>
                  </div>
                  {keys.length === 0 ? <p className="py-6 text-sm text-carbon-text-3">No metrics from this node match “{filter}”.</p> : (
                    <div className="grid grid-cols-1 gap-px bg-carbon-border sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
                      {keys.map((key) => {
                        const series = flats.map((flat) => flat[key] ?? 0);
                        return (
                          <div key={key} className="min-w-0 bg-carbon-layer p-4">
                            <div className="mb-2 flex items-baseline justify-between">
                              <span className="truncate pr-3 font-mono text-xs text-carbon-text-3" title={key}>{key}</span>
                              <span className="shrink-0 font-mono text-lg text-carbon-text">{series[series.length - 1]}</span>
                            </div>
                            <Sparkline values={series} />
                          </div>
                        );
                      })}
                    </div>
                  )}
                </section>
              );
            })}
          </div>
        )}
      </Card>
    </Page>
  );
}
