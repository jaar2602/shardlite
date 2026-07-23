import { useCallback, useEffect, useState } from "react";
import { useAuth } from "../auth";
import * as api from "../lib/api";
import { Banner, Button, Card, DataTable, Page, PageHeader, Spinner, StatCard, Tag } from "../components/ui";

function field(record: Record<string, unknown>, key: string): string {
  const value = record[key];
  return value === null || value === undefined ? "—" : String(value);
}

function time(value?: number | null): string {
  return value ? new Date(value).toLocaleTimeString() : "never";
}

export default function Replication({ name }: { name: string }) {
  const { me } = useAuth();
  const canOperate = api.permits(me?.role, "operate");
  const [replication, setReplication] = useState<api.ReplicationStatus | null>(null);
  const [s3, setS3] = useState<api.S3Status | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const c = api.conn(name);
      const [rep, status] = await Promise.all([c.replication(), c.s3.status()]);
      setReplication(rep);
      setS3(status);
      setError(null);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Replication status could not be loaded.");
    }
  }, [name]);

  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(), 5000);
    return () => window.clearInterval(timer);
  }, [load]);

  const run = async (label: string, action: () => Promise<unknown>, confirmMessage?: string) => {
    if (confirmMessage && !confirm(confirmMessage)) return;
    setBusy(label);
    setNotice(null);
    setError(null);
    try {
      const result = await action();
      setNotice(`${label}: ${JSON.stringify(result)}`);
      void load();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : `${label} failed.`);
    } finally {
      setBusy(null);
    }
  };

  if (!replication && !s3) return <div className="p-6">{error ? <Banner tone="error">{error}</Banner> : <Spinner label="Loading replication status…" />}</div>;

  const shards = replication?.shards ?? [];
  const shardKeys = Array.from(new Set(shards.flatMap((row) => Object.keys(row))));
  const columns = ["shard", ...shardKeys.filter((key) => key !== "shard")];
  const acks = replication?.acks;
  const s3Shards = s3?.shards ?? [];

  return (
    <Page>
      <PageHeader
        eyebrow="Diagnostics / replication"
        title="Replication & archival"
        description="Per-shard replication progress and S3 archival lifecycle for this database."
        actions={<><span className="font-mono text-xs text-carbon-text-3">updates every 5s</span><Button variant="secondary" onClick={() => void load()}>Refresh now</Button></>}
      />
      {error && <Banner tone="error">{error}</Banner>}
      {notice && <Banner tone="success">{notice}</Banner>}

      <div className="grid grid-cols-2 gap-px bg-carbon-border md:grid-cols-4">
        <StatCard label="Replicated" value={replication ? (replication.replicated ? "yes" : "no") : "—"} tone={replication?.replicated ? "green" : "yellow"} />
        <StatCard label="Acks confirmed" value={acks?.confirmed ?? "—"} />
        <StatCard label="Acks timed out" value={acks?.timed_out ?? "—"} tone={acks?.timed_out ? "red" : "green"} />
        <StatCard label="Ack wait" value={acks ? `${acks.waited_us} µs` : "—"} />
      </div>

      <Card title="Per-shard replication">
        <DataTable
          columns={columns.map((key) => key.replace(/_/g, " "))}
          empty="No replication evidence reported."
          rows={shards.map((row) => columns.map((key) => field(row, key)))}
        />
      </Card>

      <Card
        title="S3 archival"
        actions={canOperate && s3?.supported ? <>
          <Button variant="secondary" disabled={busy !== null} onClick={() => void run("Apply stored S3 config", () => api.conn(name).s3.apply(), "Push this connection's stored S3 config to every node?")}>{busy === "Apply stored S3 config" ? "Applying…" : "Apply stored S3 config"}</Button>
          <Button variant="secondary" disabled={busy !== null} onClick={() => void run("Snapshot now", () => api.conn(name).s3.snapshot())}>{busy === "Snapshot now" ? "Snapshotting…" : "Snapshot now"}</Button>
          <Button variant="secondary" disabled={busy !== null} onClick={() => void run("Flush", () => api.conn(name).s3.flush())}>{busy === "Flush" ? "Flushing…" : "Flush"}</Button>
        </> : undefined}
      >
        {!s3?.supported ? <p className="text-sm text-carbon-text-3">S3 archival is not supported by this database version.</p> : <div className="space-y-3">
          <div className="flex flex-wrap items-center gap-2">
            <Tag tone={s3.capture_ready ? "green" : "gray"}>{s3.capture_ready ? "capture ready" : "capture not ready"}</Tag>
            <Tag tone={s3.configured ? "green" : "gray"}>{s3.configured ? "configured" : "not configured"}</Tag>
            {s3.health != null && <Tag tone={s3.health ? "green" : "red"}>{s3.health ? "healthy" : "unhealthy"}</Tag>}
          </div>
          {s3.last_error && <Banner tone="error">{s3.last_error}</Banner>}
          {s3.summary && <div className="grid grid-cols-2 gap-x-6 gap-y-2 text-xs sm:grid-cols-4">
            <Summary label="Bucket" value={s3.summary.bucket} />
            <Summary label="Endpoint" value={s3.summary.endpoint} />
            <Summary label="Region" value={s3.summary.region} />
            <Summary label="Prefix" value={s3.summary.prefix} />
          </div>}
          <DataTable
            columns={["Shard", "Snapshot epoch", "Snapshot LSN", "Last snapshot", "Archived LSN"]}
            empty="No archival snapshots recorded."
            rows={s3Shards.map((row) => [row.shard, row.last_snapshot_epoch, row.last_snapshot_lsn, time(row.last_snapshot_ms), row.last_archived_lsn])}
          />
        </div>}
      </Card>
    </Page>
  );
}

function Summary({ label, value }: { label: string; value: string }) {
  return <div className="min-w-0"><div className="mb-1 text-[10px] uppercase tracking-wider text-carbon-text-3">{label}</div><div className="truncate font-mono text-carbon-text" title={value}>{value || "—"}</div></div>;
}
