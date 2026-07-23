import { useEffect, useRef, useState } from "react";
import * as api from "../lib/api";
import { Banner, Button, EmptyState, Page, PageHeader, Sparkline, Spinner, TextInput } from "../components/ui";

// Flatten nested stats into dotted numeric leaves, e.g. { "writer.batches": 12, ... }.
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

export default function Stats({ name }: { name: string }) {
  const c = api.conn(name);
  const [samples, setSamples] = useState<api.MetricSample[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");
  const timer = useRef<number | null>(null);

  const load = async () => {
    try {
      setSamples(await c.metrics());
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

  if (error && !samples) return <Page><PageHeader eyebrow="Telemetry / sampled locally" title="Metrics" description="Live counters sampled by the console from each reachable node." /><Banner tone="error">Metrics could not be loaded. {error}</Banner></Page>;
  if (!samples) return <Page><Spinner label="Loading metrics…" /></Page>;

  if (samples.length === 0) {
    return (
      <Page><PageHeader eyebrow="Telemetry / sampled locally" title="Metrics" description="Live counters sampled by the console from each reachable node." /><EmptyState title="Collecting the first sample" description="The console polls metrics every five seconds. Trends will appear here after the first node responds." /></Page>
    );
  }

  const bySource = new Map<string, api.MetricSample[]>();
  for (const sample of samples) {
    const source = sample.source || "legacy/default endpoint";
    bySource.set(source, [...(bySource.get(source) ?? []), sample]);
  }

  return (
    <Page>
      <PageHeader eyebrow="Telemetry / sampled locally" title="Metrics" description={`Showing ${samples.length} samples, approximately ${Math.max(1, Math.round((samples.length * 5) / 60))} minutes. History is collected by the console, not stored by ShardLite.`} actions={<Button variant="secondary" onClick={() => void load()}>Refresh now</Button>} />
      {error && <Banner tone="error">Refresh failed; showing the last collected samples. {error}</Banner>}
      <div className="flex flex-wrap items-end justify-between gap-3"><div className="w-full max-w-sm"><TextInput label="Find a metric" placeholder="writer, raft, cache…" value={filter} onChange={(event) => setFilter(event.target.value)} /></div><span className="font-mono text-xs text-carbon-text-3">updates every 5s</span></div>
      <div className="space-y-7">
        {Array.from(bySource).map(([source, sourceSamples]) => {
          const flats = sourceSamples.map((sample) => flatten(sample.stats));
          const keys = Array.from(new Set(flats.flatMap((flat) => Object.keys(flat)))).sort().filter((key) => key.toLowerCase().includes(filter.trim().toLowerCase()));
          return (
            <section key={source}>
              <div className="mb-3 flex items-center justify-between border-b border-carbon-border pb-2"><h2 className="truncate font-mono text-xs text-carbon-text-2" title={source}>{source}</h2><span className="text-xs text-carbon-text-3">{keys.length} metric{keys.length === 1 ? "" : "s"}</span></div>
              {keys.length === 0 ? <p className="py-6 text-sm text-carbon-text-3">No metrics from this node match “{filter}”.</p> : <div className="grid grid-cols-1 gap-px bg-carbon-border sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
                {keys.map((key) => {
                  const series = flats.map((flat) => flat[key] ?? 0);
                  const current = series[series.length - 1];
                  return (
                    <div key={key} className="min-w-0 bg-carbon-layer p-4">
                      <div className="flex items-baseline justify-between mb-2">
                        <span className="truncate pr-3 font-mono text-xs text-carbon-text-3" title={key}>{key}</span>
                        <span className="shrink-0 font-mono text-lg text-carbon-text">{current}</span>
                      </div>
                      <Sparkline values={series} />
                    </div>
                  );
                })}
              </div>}
            </section>
          );
        })}
      </div>
    </Page>
  );
}
