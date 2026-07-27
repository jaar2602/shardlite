import type { MetricSample } from "./api";

// Turning shardlite's counters into something a DBA can act on.
//
// The raw feed is monotonic counters — `writer.requests: 41293` says nothing about whether the
// database is healthy right now. What matters is the *rate* those counters move at, and whether a
// handful of specific ones are moving at all: every counter listed as a symptom below is one that
// should sit at zero on a healthy node, so any movement is the signal.

export type Tone = "green" | "yellow" | "red" | "gray";

/// Flatten nested stats into dotted numeric leaves, e.g. { "writer.batches": 12, … }.
export function flatten(obj: unknown, prefix = ""): Record<string, number> {
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

export interface NodeVitals {
  source: string;
  /// Per-second rates, derived across the sampling window rather than read off a counter.
  writesPerSecond: number;
  readsPerSecond: number;
  httpPerSecond: number;
  /// Statements per write transaction. This is *the* shardlite-specific efficiency signal: one
  /// fsync is amortised across a batch, so a value pinned at 1 under load means group commit is
  /// not batching and every write is paying for its own fsync.
  meanBatch: number;
  openShards: number;
  /// Counters that should never move on a healthy node.
  readersRejected: number;
  readersTimedOut: number;
  checkpointStalls: number;
  checkpointFailures: number;
  walBytes: number;
  walContention: number;
  walMaxWaitMs: number;
  httpErrors: number;
  authFailures: number;
  /// Leadership churn — a cluster that keeps re-electing is not stable, however green it looks.
  elections: number;
  steppedDown: number;
  samples: number;
}

function rate(series: { t: number; value: number }[]): number {
  if (series.length < 2) return 0;
  const first = series[0];
  const last = series[series.length - 1];
  const seconds = (last.t - first.t) / 1000;
  if (seconds <= 0) return 0;
  // Counters reset when a node restarts; a negative delta is that, not negative traffic.
  const delta = last.value - first.value;
  return delta < 0 ? 0 : delta / seconds;
}

export function vitalsFor(source: string, samples: MetricSample[]): NodeVitals {
  const flats = samples.map((sample) => ({ t: sample.t, flat: flatten(sample.stats) }));
  const series = (key: string) => flats.map(({ t, flat }) => ({ t, value: flat[key] ?? 0 }));
  const latest = flats.length ? flats[flats.length - 1].flat : {};

  return {
    source,
    writesPerSecond: rate(series("writer.requests")),
    readsPerSecond: rate(series("reader.queries")),
    httpPerSecond: rate(series("http.requests")),
    meanBatch: latest["writer.mean_batch"] ?? 0,
    openShards: latest["writer.open_now"] ?? 0,
    readersRejected: latest["reader.rejected_busy"] ?? 0,
    readersTimedOut: latest["reader.timed_out"] ?? 0,
    checkpointStalls: latest["checkpoint.stalls"] ?? 0,
    checkpointFailures: latest["checkpoint.failures"] ?? 0,
    walBytes: latest["checkpoint.wal_bytes"] ?? 0,
    walContention: latest["wal_conversion.contended_opens"] ?? 0,
    walMaxWaitMs: latest["wal_conversion.max_wait_ms"] ?? 0,
    httpErrors: latest["http.errors"] ?? 0,
    authFailures: latest["http.auth_failures"] ?? 0,
    elections: latest["cluster.elections_started"] ?? 0,
    steppedDown: latest["cluster.stepped_down"] ?? 0,
    samples: samples.length,
  };
}

export interface Indicator {
  label: string;
  value: string;
  tone: Tone;
  /// Why this reading is the tone it is — a number without its threshold is not actionable.
  note: string;
}

/// Whether write batching is amortising fsyncs, which is what shardlite's write throughput rests on.
export function batchingIndicator(vitals: NodeVitals[]): Indicator {
  const active = vitals.filter((v) => v.writesPerSecond > 1);
  if (active.length === 0) {
    return { label: "Write batching", value: "idle", tone: "gray", note: "No sustained write load to measure against." };
  }
  const worst = Math.min(...active.map((v) => v.meanBatch));
  return {
    label: "Write batching",
    value: `${worst.toFixed(1)} stmt/txn`,
    tone: worst >= 2 ? "green" : worst > 1 ? "yellow" : "red",
    note:
      worst >= 2
        ? "Group commit is amortising fsyncs across batched statements."
        : "Near 1 means each write pays for its own fsync — throughput is fsync-bound.",
  };
}

export function backpressureIndicator(vitals: NodeVitals[]): Indicator {
  const rejected = vitals.reduce((sum, v) => sum + v.readersRejected, 0);
  const timedOut = vitals.reduce((sum, v) => sum + v.readersTimedOut, 0);
  const stalls = vitals.reduce((sum, v) => sum + v.checkpointStalls, 0);
  const total = rejected + timedOut + stalls;
  return {
    label: "Backpressure",
    value: total === 0 ? "none" : `${total}`,
    tone: total === 0 ? "green" : timedOut > 0 || stalls > 0 ? "red" : "yellow",
    note:
      total === 0
        ? "No reads shed, no checkpoint stalls."
        : `${rejected} reads shed · ${timedOut} timed out · ${stalls} checkpoint stalls. The pool is saturated.`,
  };
}

export function durabilityIndicator(vitals: NodeVitals[]): Indicator {
  const failures = vitals.reduce((sum, v) => sum + v.checkpointFailures, 0);
  const wal = vitals.reduce((sum, v) => Math.max(sum, v.walBytes), 0);
  return {
    label: "Checkpointing",
    value: failures > 0 ? `${failures} failed` : wal > 0 ? `${formatBytes(wal)} WAL` : "clean",
    tone: failures > 0 ? "red" : wal > 64 * 1024 * 1024 ? "yellow" : "green",
    note:
      failures > 0
        ? "A failed checkpoint means the WAL is not being folded back — it will grow without bound."
        : "WAL is being checkpointed into the main files.",
  };
}

export function errorIndicator(vitals: NodeVitals[]): Indicator {
  const errors = vitals.reduce((sum, v) => sum + v.httpErrors, 0);
  const auth = vitals.reduce((sum, v) => sum + v.authFailures, 0);
  return {
    label: "Request errors",
    value: errors === 0 && auth === 0 ? "none" : `${errors}${auth ? ` · ${auth} auth` : ""}`,
    tone: errors === 0 && auth === 0 ? "green" : errors > 0 ? "yellow" : "red",
    note: auth > 0 ? "Authentication failures are worth investigating on their own." : "Gateway errors since the node started.",
  };
}

export function stabilityIndicator(vitals: NodeVitals[]): Indicator {
  const elections = vitals.reduce((sum, v) => Math.max(sum, v.elections), 0);
  const stepped = vitals.reduce((sum, v) => sum + v.steppedDown, 0);
  return {
    label: "Leadership",
    value: stepped === 0 ? (elections <= 1 ? "stable" : `${elections} elections`) : `${stepped} step-downs`,
    tone: stepped > 0 ? "yellow" : elections <= 1 ? "green" : "yellow",
    note:
      stepped > 0
        ? "A node stepped down — leadership moved, which pauses writes on its shards while it does."
        : "One election is the normal cost of starting; repeated ones mean the cluster is not settling.",
  };
}

export function formatBytes(bytes: number): string {
  if (bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const exp = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
  return `${(bytes / 1024 ** exp).toFixed(exp === 0 ? 0 : 1)} ${units[exp]}`;
}

export function formatRate(perSecond: number): string {
  if (perSecond >= 1000) return `${(perSecond / 1000).toFixed(1)}k/s`;
  if (perSecond >= 10) return `${perSecond.toFixed(0)}/s`;
  if (perSecond > 0) return `${perSecond.toFixed(1)}/s`;
  return "0/s";
}
