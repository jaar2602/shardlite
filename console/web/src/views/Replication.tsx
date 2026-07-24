import { useCallback, useEffect, useState } from "react";
import { useAuth } from "../auth";
import * as api from "../lib/api";
import { Banner, Button, Card, DataTable, Page, PageHeader, Spinner, StatCard, Tag, TextInput } from "../components/ui";

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
  const [showConfig, setShowConfig] = useState(false);
  const [cfg, setCfg] = useState({ bucket: "", endpoint: "", region: "", prefix: "", access_key: "", secret_key: "" });

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

  // Prefill the form from the current target (keys are never returned, so they start blank).
  const openConfig = () => {
    setCfg({
      bucket: s3?.summary?.bucket ?? "",
      endpoint: s3?.summary?.endpoint ?? "",
      region: s3?.summary?.region ?? "",
      prefix: s3?.summary?.prefix ?? "",
      access_key: "",
      secret_key: "",
    });
    setShowConfig(true);
  };
  const saveConfig = () =>
    void run("Configure S3", () =>
      api.conn(name).s3.config({
        enabled: true,
        bucket: cfg.bucket.trim(),
        region: cfg.region.trim() || undefined,
        endpoint: cfg.endpoint.trim() || undefined,
        prefix: cfg.prefix.trim() || undefined,
        access_key: cfg.access_key,
        secret_key: cfg.secret_key,
      }),
    );

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
          <Button variant={showConfig ? "primary" : "secondary"} disabled={busy !== null} onClick={() => (showConfig ? setShowConfig(false) : openConfig())}>{showConfig ? "Close config" : "Configure S3"}</Button>
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
          {showConfig && (
            <div className="space-y-3 border border-carbon-border bg-carbon-field/40 p-3">
              <p className="text-xs text-carbon-text-3">
                Point this cluster at any S3-compatible store (AWS S3, MinIO, Cloudflare R2, …). Leave the
                endpoint blank for AWS; set it (path-style) for other stores. Applies to the live cluster now.
              </p>
              {!s3.capture_ready && (
                <Banner tone="info">Nodes must be started capture-ready (<span className="font-mono">--s3-ready</span> or <span className="font-mono">--s3-bucket</span>) before a target can attach.</Banner>
              )}
              <div className="grid gap-3 sm:grid-cols-2">
                <TextInput label="Bucket" value={cfg.bucket} onChange={(e) => setCfg({ ...cfg, bucket: e.target.value })} />
                <TextInput label="Endpoint (blank = AWS)" placeholder="https://minio.example.com" value={cfg.endpoint} onChange={(e) => setCfg({ ...cfg, endpoint: e.target.value })} />
                <TextInput label="Region" placeholder="us-east-1" value={cfg.region} onChange={(e) => setCfg({ ...cfg, region: e.target.value })} />
                <TextInput label="Key prefix (optional)" value={cfg.prefix} onChange={(e) => setCfg({ ...cfg, prefix: e.target.value })} />
                <TextInput label="Access key" value={cfg.access_key} onChange={(e) => setCfg({ ...cfg, access_key: e.target.value })} />
                <TextInput label="Secret key" type="password" value={cfg.secret_key} onChange={(e) => setCfg({ ...cfg, secret_key: e.target.value })} />
              </div>
              <div className="flex flex-wrap gap-2">
                <Button disabled={busy !== null || !cfg.bucket.trim() || !cfg.access_key || !cfg.secret_key} onClick={saveConfig}>
                  {busy === "Configure S3" ? "Saving…" : "Save & apply"}
                </Button>
                {s3.configured && (
                  <Button variant="secondary" disabled={busy !== null} onClick={() => void run("Disable S3", () => api.conn(name).s3.config({ enabled: false }), "Detach the S3 target from this cluster?")}>
                    {busy === "Disable S3" ? "Disabling…" : "Disable S3"}
                  </Button>
                )}
              </div>
              <p className="text-xs text-carbon-text-3">
                This configures the running cluster. To persist the target so it re-applies after a node restart,
                also set S3 on the connection (Connections → edit) and use “Apply stored S3 config”.
              </p>
            </div>
          )}
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
