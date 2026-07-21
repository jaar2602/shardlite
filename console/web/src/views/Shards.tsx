import { useEffect, useState } from "react";
import * as api from "../lib/api";
import { Banner, Spinner, StatCard, Tag, TextInput } from "../components/ui";

// The WAL frame inspector, per shard — the console face of `meshdb frames`. Physical replication
// ships opaque WAL frames, so this is the honest window into what is physically present.
export default function Shards({ name }: { name: string }) {
  const c = api.conn(name);
  const [shard, setShard] = useState(0);
  const [report, setReport] = useState<Record<string, unknown> | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const load = async () => {
    setBusy(true);
    setError(null);
    try {
      setReport(await c.frames(shard));
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to load frames");
    } finally {
      setBusy(false);
    }
  };
  useEffect(() => {
    void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [shard]);

  const num = (k: string) => (report && typeof report[k] === "number" ? String(report[k]) : "—");
  const leftover = report && typeof report.leftover_frames === "number" ? report.leftover_frames : 0;
  const uncommitted = report && typeof report.uncommitted_frames === "number" ? report.uncommitted_frames : 0;

  return (
    <div className="p-6 space-y-6 max-w-4xl">
      <div className="w-28">
        <TextInput label="Shard" type="number" min={0} value={shard} onChange={(e) => setShard(Number(e.target.value))} />
      </div>

      {busy && <Spinner label="Reading WAL…" />}
      {error && <Banner tone="error">{error}</Banner>}

      {report && !report.wal && <Banner tone="info">No WAL present for this shard (checkpointed or empty).</Banner>}

      {report && report.wal ? (
        <>
          <div className="grid grid-cols-4 gap-4">
            <StatCard label="Frames" value={num("frames")} />
            <StatCard label="Committed txns" value={num("transactions")} />
            <StatCard label="Uncommitted frames" value={String(uncommitted)} tone={uncommitted > 0 ? "red" : undefined} />
            <StatCard label="Leftover frames" value={String(leftover)} tone={leftover > 0 ? "red" : undefined} />
          </div>
          <div className="flex flex-wrap gap-2 items-center text-xs text-carbon-text-3">
            <Tag>page size {num("page_size")}</Tag>
            <Tag>file {num("file_bytes")} bytes</Tag>
            <Tag>salt {String(report.salt ?? "—")}</Tag>
          </div>
          <p className="text-carbon-text-3 text-xs">
            Leftover frames are pre-checkpoint remnants SQLite ignores (salt mismatch); uncommitted
            frames sit past the last commit. Both are shown, not hidden — the inspector reports what
            is physically there.
          </p>
        </>
      ) : null}
    </div>
  );
}
