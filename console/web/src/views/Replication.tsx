import { useCallback, useEffect, useMemo, useState } from "react";
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
          {canOperate ? (
            <S3ConnectionsPanel name={name} s3={s3} />
          ) : (
            s3.summary && <div className="grid grid-cols-2 gap-x-6 gap-y-2 text-xs sm:grid-cols-4">
              <Summary label="Bucket" value={s3.summary.bucket} />
              <Summary label="Endpoint" value={s3.summary.endpoint} />
              <Summary label="Region" value={s3.summary.region} />
              <Summary label="Prefix" value={s3.summary.prefix} />
            </div>
          )}
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

const emptyForm = { name: "", bucket: "", endpoint: "", region: "", prefix: "", access_key: "", secret_key: "" };
const normEndpoint = (e: string) => e.replace(/^https?:\/\//, "").replace(/\/+$/, "");

// Saved, persisted S3 connections for this console. One can be activated as the cluster's snapshot
// target; the one matching the cluster's live S3 status is labeled.
function S3ConnectionsPanel({ name, s3 }: { name: string; s3: api.S3Status }) {
  const [conns, setConns] = useState<api.S3Connection[]>([]);
  const [editing, setEditing] = useState<string | null>(null); // an id, "new", or null
  const [form, setForm] = useState(emptyForm);
  const [busy, setBusy] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [msg, setMsg] = useState<string | null>(null);

  const load = useCallback(() => api.s3Connections.list().then(setConns).catch((e) => setErr(e instanceof Error ? e.message : "Could not load S3 connections.")), []);
  useEffect(() => { void load(); }, [load]);

  // Which saved connection is the cluster's current snapshot target (matched on bucket + endpoint;
  // a blank saved endpoint matches an AWS default).
  const activeId = useMemo(() => {
    const su = s3.summary;
    if (!su) return null;
    return conns.find((c) =>
      c.bucket === su.bucket &&
      (normEndpoint(c.endpoint) === normEndpoint(su.endpoint) || (c.endpoint === "" && /amazonaws\.com/.test(su.endpoint))),
    )?.id ?? null;
  }, [conns, s3.summary]);

  const run = async (label: string, fn: () => Promise<unknown>) => {
    setBusy(label); setErr(null); setMsg(null);
    try { await fn(); await load(); } catch (e) { setErr(e instanceof Error ? e.message : `${label} failed.`); } finally { setBusy(null); }
  };

  const save = () => run("save", async () => {
    const input: api.S3ConnectionInput = {
      name: form.name, bucket: form.bucket, endpoint: form.endpoint, region: form.region,
      prefix: form.prefix, access_key: form.access_key,
      // On edit, a blank secret preserves the stored one (omit); on create, send what was typed.
      ...(editing !== "new" && form.secret_key === "" ? {} : { secret_key: form.secret_key }),
    };
    if (editing === "new") await api.s3Connections.create(input);
    else await api.s3Connections.update(editing!, input);
    setEditing(null);
  });

  const activate = (c: api.S3Connection) => run("activate:" + c.id, async () => {
    await api.conn(name).s3.activate(c.id);
    setMsg(`“${c.name}” is now this cluster's snapshot target.`);
  });

  const remove = (c: api.S3Connection) => {
    if (!confirm(`Delete the saved S3 connection “${c.name}”?`)) return;
    void run("delete:" + c.id, () => api.s3Connections.remove(c.id));
  };

  return (
    <div className="space-y-3">
      {err && <Banner tone="error">{err}</Banner>}
      {msg && <Banner tone="success">{msg}</Banner>}
      {!s3.capture_ready && <Banner tone="info">Nodes must be started capture-ready (<span className="font-mono">--s3-ready</span> or <span className="font-mono">--s3-bucket</span>) before a target can attach.</Banner>}

      <div className="flex items-center justify-between">
        <div className="text-xs font-semibold uppercase tracking-wider text-carbon-text-3">Saved S3 connections</div>
        <Button variant="secondary" className="min-h-0 px-3 py-1 text-xs" disabled={busy !== null} onClick={() => { setForm(emptyForm); setEditing("new"); }}>New S3 connection</Button>
      </div>

      {conns.length === 0 && editing === null ? (
        <p className="text-sm text-carbon-text-3">No saved S3 connections yet. Create one to archive this cluster to an S3-compatible store.</p>
      ) : (
        <div className="divide-y divide-carbon-border border border-carbon-border">
          {conns.map((c) => (
            <div key={c.id} className="flex flex-wrap items-center justify-between gap-2 px-3 py-2">
              <div className="min-w-0">
                <div className="flex items-center gap-2">
                  <span className="font-semibold text-carbon-text">{c.name}</span>
                  {activeId === c.id && <Tag tone="green">Snapshot target</Tag>}
                  {!c.has_secret && <Tag tone="yellow">no secret key</Tag>}
                </div>
                <div className="truncate font-mono text-xs text-carbon-text-3" title={c.bucket}>
                  {c.bucket}{c.endpoint ? ` @ ${c.endpoint}` : " (AWS)"}{c.prefix ? ` /${c.prefix}` : ""}
                </div>
              </div>
              <div className="flex gap-2">
                <Button variant="secondary" className="min-h-0 px-2 py-1 text-xs" disabled={busy !== null || activeId === c.id || !c.has_secret} onClick={() => activate(c)}>
                  {busy === "activate:" + c.id ? "Activating…" : activeId === c.id ? "Active" : "Use for snapshots"}
                </Button>
                <Button variant="secondary" className="min-h-0 px-2 py-1 text-xs" disabled={busy !== null} onClick={() => { setForm({ name: c.name, bucket: c.bucket, endpoint: c.endpoint, region: c.region, prefix: c.prefix, access_key: c.access_key, secret_key: "" }); setEditing(c.id); }}>Edit</Button>
                <Button variant="secondary" className="min-h-0 px-2 py-1 text-xs" disabled={busy !== null} onClick={() => remove(c)}>Delete</Button>
              </div>
            </div>
          ))}
        </div>
      )}

      {editing !== null && (
        <div className="space-y-3 border border-carbon-border bg-carbon-field/40 p-3">
          <div className="text-xs font-semibold uppercase tracking-wider text-carbon-text-3">{editing === "new" ? "New S3 connection" : "Edit S3 connection"}</div>
          <p className="text-xs text-carbon-text-3">S3-compatible: leave the endpoint blank for AWS; set it (path-style) for MinIO, Cloudflare R2, or other stores.</p>
          <div className="grid gap-3 sm:grid-cols-2">
            <TextInput label="Name" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} />
            <TextInput label="Bucket" value={form.bucket} onChange={(e) => setForm({ ...form, bucket: e.target.value })} />
            <TextInput label="Endpoint (blank = AWS)" placeholder="https://minio.example.com" value={form.endpoint} onChange={(e) => setForm({ ...form, endpoint: e.target.value })} />
            <TextInput label="Region" placeholder="us-east-1" value={form.region} onChange={(e) => setForm({ ...form, region: e.target.value })} />
            <TextInput label="Key prefix (optional)" value={form.prefix} onChange={(e) => setForm({ ...form, prefix: e.target.value })} />
            <TextInput label="Access key" value={form.access_key} onChange={(e) => setForm({ ...form, access_key: e.target.value })} />
            <TextInput label={editing === "new" ? "Secret key" : "Secret key (blank keeps current)"} type="password" value={form.secret_key} onChange={(e) => setForm({ ...form, secret_key: e.target.value })} />
          </div>
          <div className="flex gap-2">
            <Button disabled={busy !== null || !form.name.trim() || !form.bucket.trim()} onClick={save}>{busy === "save" ? "Saving…" : "Save"}</Button>
            <Button variant="secondary" disabled={busy !== null} onClick={() => setEditing(null)}>Cancel</Button>
          </div>
        </div>
      )}

      <p className="text-xs text-carbon-text-3">“Use for snapshots” pushes the saved connection to the live cluster. Saved connections persist across restarts; re-activate a target after a node restart if the cluster shows “not configured”.</p>
    </div>
  );
}
