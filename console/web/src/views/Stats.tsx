import { useEffect, useRef, useState } from "react";
import * as api from "../lib/api";
import { Banner, Sparkline, Spinner } from "../components/ui";

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
  const timer = useRef<number | null>(null);

  const load = async () => {
    try {
      setSamples(await c.metrics());
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

  if (error) return <div className="p-6"><Banner tone="error">{error}</Banner></div>;
  if (!samples) return <div className="p-6"><Spinner label="Loading metrics…" /></div>;

  if (samples.length === 0) {
    return (
      <div className="p-6">
        <Banner tone="info">
          No samples yet. The console polls each connection's stats every 5 seconds; charts fill in
          as data arrives.
        </Banner>
      </div>
    );
  }

  const flats = samples.map((s) => flatten(s.stats));
  const keys = Array.from(new Set(flats.flatMap((f) => Object.keys(f)))).sort();

  return (
    <div className="p-6">
      <p className="text-carbon-text-3 text-xs mb-4">
        Last {samples.length} samples (≈{Math.round((samples.length * 5) / 60)} min). meshdb keeps no
        time series itself — this history is sampled and held by the console.
      </p>
      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
        {keys.map((k) => {
          const series = flats.map((f) => f[k] ?? 0);
          const current = series[series.length - 1];
          return (
            <div key={k} className="bg-carbon-layer border border-carbon-border p-4">
              <div className="flex items-baseline justify-between mb-2">
                <span className="text-carbon-text-3 text-xs font-mono">{k}</span>
                <span className="text-carbon-text font-mono text-lg">{current}</span>
              </div>
              <Sparkline values={series} />
            </div>
          );
        })}
      </div>
    </div>
  );
}
