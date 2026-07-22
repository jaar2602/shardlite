import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "../auth";
import * as api from "../lib/api";
import { Banner, Button, Card, EmptyState, Page, PageHeader, Spinner, Tag, TextInput } from "../components/ui";

export default function Connections() {
  const { me } = useAuth();
  const nav = useNavigate();
  const isAdmin = me?.role === "admin";
  const [list, setList] = useState<api.Connection[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);
  const [editing, setEditing] = useState<string | null>(null);
  const [testing, setTesting] = useState<string | null>(null);
  const [testResults, setTestResults] = useState<Record<string, { ok: boolean; message: string }>>({});
  const [verifyFor, setVerifyFor] = useState<string | null>(null);
  const [nodeEndpoint, setNodeEndpoint] = useState("");
  const [nodeVerification, setNodeVerification] = useState<api.NodeVerification | null>(null);
  const [verifyingNode, setVerifyingNode] = useState(false);
  const emptyForm = {
    name: "",
    url: "",
    additional_seeds: "",
    meshdb_user: "",
    meshdb_secret: "",
    enabled: true,
    timeout_ms: 60000,
    allow_insecure_http: false,
    custom_ca_pem: "",
    s3_bucket: "",
    s3_region: "",
    s3_endpoint: "",
    s3_access_key: "",
    s3_secret_key: "",
    s3_prefix: "",
    s3_enabled: false,
  };
  const [form, setForm] = useState(emptyForm);

  const load = async () => {
    try {
      setList(await api.connections.list());
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to load");
    }
  };
  useEffect(() => {
    void load();
  }, []);

  const save = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    try {
      await api.connections.create({
        name: form.name,
        url: form.url,
        seeds: [form.url, ...form.additional_seeds.split(/\s+/)].filter(Boolean),
        meshdb_user: form.meshdb_user || undefined,
        meshdb_secret: form.meshdb_secret || undefined,
        replace: editing !== null,
        enabled: form.enabled,
        timeout_ms: form.timeout_ms,
        allow_insecure_http: form.allow_insecure_http,
        custom_ca_pem: form.custom_ca_pem,
        s3_bucket: form.s3_bucket || undefined,
        s3_region: form.s3_region || undefined,
        s3_endpoint: form.s3_endpoint || undefined,
        s3_access_key: form.s3_access_key || undefined,
        s3_secret_key: form.s3_secret_key || undefined,
        s3_prefix: form.s3_prefix || undefined,
        s3_enabled: form.s3_enabled,
      });
      setForm(emptyForm);
      setAdding(false);
      setEditing(null);
      void load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to add");
    }
  };

  const startAdd = () => {
    setForm(emptyForm);
    setEditing(null);
    setAdding(true);
  };

  const startEdit = (connection: api.Connection) => {
    setForm({
      name: connection.name,
      url: connection.url,
      additional_seeds: connection.seeds.slice(1).join("\n"),
      meshdb_user: connection.meshdb_user ?? "",
      meshdb_secret: "",
      enabled: connection.enabled,
      timeout_ms: connection.timeout_ms,
      allow_insecure_http: connection.allow_insecure_http,
      custom_ca_pem: connection.custom_ca_pem ?? "",
      s3_bucket: connection.s3?.bucket ?? "",
      s3_region: connection.s3?.region ?? "",
      s3_endpoint: connection.s3?.endpoint ?? "",
      s3_access_key: connection.s3?.access_key ?? "",
      s3_secret_key: "",
      s3_prefix: connection.s3?.prefix ?? "",
      s3_enabled: connection.s3?.enabled ?? false,
    });
    setEditing(connection.name);
    setAdding(true);
  };

  const cancelEdit = () => {
    setAdding(false);
    setEditing(null);
    setForm(emptyForm);
  };

  const test = async (name: string) => {
    setTesting(name);
    setTestResults((current) => ({ ...current, [name]: { ok: false, message: "testing…" } }));
    try {
      const result = await api.connections.test(name);
      setTestResults((current) => ({
        ...current,
        [name]: { ok: true, message: `${result.latency_ms} ms · active endpoint ${result.seed}` },
      }));
    } catch (e) {
      setTestResults((current) => ({
        ...current,
        [name]: { ok: false, message: e instanceof Error ? e.message : "test failed" },
      }));
    } finally {
      setTesting(null);
    }
  };

  const beginVerify = (connection: api.Connection) => {
    setVerifyFor(connection.name);
    setNodeEndpoint("");
    setNodeVerification(null);
  };

  const verifyNode = async () => {
    if (!verifyFor || !nodeEndpoint.trim()) return;
    setVerifyingNode(true);
    setNodeVerification(null);
    setError(null);
    try { setNodeVerification(await api.conn(verifyFor).verifyNode(nodeEndpoint.trim())); }
    catch (caught) { setError(caught instanceof Error ? caught.message : "The node could not be verified."); }
    finally { setVerifyingNode(false); }
  };

  const useVerifiedEndpoint = () => {
    const connection = list?.find((item) => item.name === verifyFor);
    if (!connection || !nodeVerification) return;
    startEdit(connection);
    const candidate = nodeVerification.endpoint === connection.url ? [] : [nodeVerification.endpoint];
    const endpoints = [...connection.seeds.slice(1), ...candidate]
      .filter((value, index, values) => values.indexOf(value) === index);
    setForm((current) => ({ ...current, additional_seeds: endpoints.join("\n") }));
    setVerifyFor(null);
    setNodeVerification(null);
  };

  const remove = async (name: string) => {
    if (!confirm(`Remove connection "${name}"?`)) return;
    try {
      await api.connections.remove(name);
      void load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to remove");
    }
  };

  return (
    <Page>
      <PageHeader eyebrow="Databases / connection profiles" title="Connections" description="Connect once to a MeshDB database. The console automatically uses a healthy endpoint for queries, changes, and monitoring." actions={isAdmin && <Button onClick={adding ? cancelEdit : startAdd}>{adding ? "Close form" : "Add database"}</Button>} />

      {error && <div className="mb-4"><Banner tone="error">{error}</Banner></div>}

      {adding && (
        <Card title={editing ? `Edit ${editing}` : "New database connection"}>
          <form onSubmit={save} className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            <TextInput
              label="Name"
              value={form.name}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
              placeholder="prod-east"
              required
              disabled={editing !== null}
            />
            <TextInput
              label="Database endpoint"
              value={form.url}
              onChange={(e) => setForm({ ...form, url: e.target.value })}
              placeholder="http://10.0.0.5:4680"
              required
            />
            <label className="text-sm text-carbon-text sm:col-span-2">
              <span className="mb-1 block text-xs uppercase tracking-wide text-carbon-text-2">
                Failover endpoints (optional, one per line)
              </span>
              <textarea
                className="h-24 w-full resize-y border-b border-carbon-border bg-carbon-layer px-3 py-2 font-mono text-xs outline-none focus:border-carbon-blue"
                value={form.additional_seeds}
                onChange={(e) => setForm({ ...form, additional_seeds: e.target.value })}
                placeholder={"https://node-2.example:4680\nhttps://node-3.example:4680"}
              />
              <span className="mt-1 block text-xs text-carbon-text-3">Any healthy endpoint provides access to the whole database.</span>
            </label>
            <TextInput
              label="meshdb user (optional)"
              value={form.meshdb_user}
              onChange={(e) => setForm({ ...form, meshdb_user: e.target.value })}
              placeholder="app"
            />
            <TextInput
              label={editing ? "New meshdb secret (blank keeps current)" : "meshdb secret (optional, stored encrypted)"}
              type="password"
              value={form.meshdb_secret}
              onChange={(e) => setForm({ ...form, meshdb_secret: e.target.value })}
            />
            <TextInput
              label="Request timeout (milliseconds)"
              type="number"
              min={1000}
              max={300000}
              value={form.timeout_ms}
              onChange={(e) => setForm({ ...form, timeout_ms: Number(e.target.value) })}
              required
            />
            <label className="flex items-center gap-2 self-end py-2 text-sm text-carbon-text">
              <input
                type="checkbox"
                checked={form.enabled}
                onChange={(e) => setForm({ ...form, enabled: e.target.checked })}
              />
              Enable polling and access
            </label>
            <label className="flex items-start gap-2 border-l-2 border-carbon-yellow bg-carbon-yellow/10 px-3 py-2 text-sm text-carbon-text sm:col-span-2">
              <input
                className="mt-1"
                type="checkbox"
                checked={form.allow_insecure_http}
                onChange={(e) => setForm({ ...form, allow_insecure_http: e.target.checked })}
              />
              <span>
                Allow plaintext HTTP. Use only for localhost or a trusted development network;
                production connections should use HTTPS.
              </span>
            </label>
            <label className="text-sm text-carbon-text sm:col-span-2">
              <span className="mb-1 block text-xs uppercase tracking-wide text-carbon-text-2">
                Private CA bundle (optional PEM)
              </span>
              <textarea
                className="h-32 w-full resize-y border-b border-carbon-border bg-carbon-layer px-3 py-2 font-mono text-xs outline-none focus:border-carbon-blue"
                value={form.custom_ca_pem}
                onChange={(e) => setForm({ ...form, custom_ca_pem: e.target.value })}
                placeholder="-----BEGIN CERTIFICATE-----"
              />
              <span className="mt-1 block text-xs text-carbon-text-3">
                Adds private roots without disabling normal HTTPS certificate or hostname verification.
              </span>
            </label>
            <div className="sm:col-span-2 border-t border-carbon-border pt-4">
              <div className="mb-2 text-xs uppercase tracking-wide text-carbon-text-2">
                S3 replication (high availability)
              </div>
              <label className="flex items-start gap-2 text-sm text-carbon-text">
                <input
                  type="checkbox"
                  className="mt-1"
                  checked={form.s3_enabled}
                  onChange={(e) => setForm({ ...form, s3_enabled: e.target.checked })}
                />
                <span>
                  Replicate this cluster's shards to S3. A survivor can then serve a failed node's
                  shards from the bucket without a full restore. The secret key is stored encrypted.
                </span>
              </label>
            </div>
            <TextInput
              label="S3 bucket"
              value={form.s3_bucket}
              onChange={(e) => setForm({ ...form, s3_bucket: e.target.value })}
              placeholder="meshdb-backups"
            />
            <TextInput
              label="S3 region"
              value={form.s3_region}
              onChange={(e) => setForm({ ...form, s3_region: e.target.value })}
              placeholder="us-east-1"
            />
            <TextInput
              label="S3 endpoint (optional)"
              value={form.s3_endpoint}
              onChange={(e) => setForm({ ...form, s3_endpoint: e.target.value })}
              placeholder="https://s3.us-east-1.amazonaws.com"
            />
            <TextInput
              label="Key prefix (optional)"
              value={form.s3_prefix}
              onChange={(e) => setForm({ ...form, s3_prefix: e.target.value })}
              placeholder="cluster-a"
            />
            <TextInput
              label="S3 access key"
              value={form.s3_access_key}
              onChange={(e) => setForm({ ...form, s3_access_key: e.target.value })}
            />
            <TextInput
              label={editing ? "New S3 secret key (blank keeps current)" : "S3 secret key (stored encrypted)"}
              type="password"
              value={form.s3_secret_key}
              onChange={(e) => setForm({ ...form, s3_secret_key: e.target.value })}
            />
            <div className="sm:col-span-2">
              <Button type="submit">{editing ? "Update connection" : "Save database"}</Button>
            </div>
          </form>
        </Card>
      )}

      {verifyFor && <Card title={`Verify a new node for ${verifyFor}`}>
        <div className="grid items-end gap-3 md:grid-cols-[minmax(16rem,1fr)_auto_auto]">
          <TextInput label="New node endpoint" placeholder="https://node-4.example:4680" value={nodeEndpoint} onChange={(event) => { setNodeEndpoint(event.target.value); setNodeVerification(null); }} />
          <Button disabled={verifyingNode || !nodeEndpoint.trim()} onClick={() => void verifyNode()}>{verifyingNode ? "Verifying…" : "Verify node"}</Button>
          <Button variant="ghost" onClick={() => { setVerifyFor(null); setNodeVerification(null); }}>Close</Button>
        </div>
        <p className="mt-2 text-xs text-carbon-text-3">This check is read-only. Join the node through your existing MeshDB deployment process, then return here to confirm membership and health.</p>
        {nodeVerification && <div className="mt-4 border-t border-carbon-border pt-4">
          <div className="flex flex-wrap items-center gap-2"><Tag tone={nodeVerification.status === "ready" ? "green" : nodeVerification.status === "stabilizing" ? "yellow" : "red"}>{nodeVerification.status.replace("_", " ")}</Tag><span className="font-mono text-xs text-carbon-text-3">{nodeVerification.latency_ms} ms · node {nodeVerification.node ?? "unknown"} · version {nodeVerification.version ?? "unknown"}</span></div>
          <dl className="mt-3 grid grid-cols-2 gap-px bg-carbon-border text-xs md:grid-cols-4">
            <VerificationFact label="Compatible" value={nodeVerification.compatible ? "yes" : "no"} />
            <VerificationFact label="Recognized member" value={nodeVerification.member ? "yes" : "not yet"} />
            <VerificationFact label="Health" value={nodeVerification.health} />
            <VerificationFact label="Data distribution" value={nodeVerification.distribution_stable ? "stable" : "not stable"} />
          </dl>
          <ol className="mt-3 list-decimal space-y-1 pl-5 text-sm text-carbon-text-2">{nodeVerification.guidance.map((item) => <li key={item}>{item}</li>)}</ol>
          {nodeVerification.status === "ready" && <Button className="mt-3" variant="secondary" onClick={useVerifiedEndpoint}>Use as failover endpoint</Button>}
        </div>}
      </Card>}

      {list === null ? (
        <Spinner label="Loading connections…" />
      ) : list.length === 0 ? <EmptyState title="No connections yet" description={isAdmin ? "Add the first MeshDB endpoint to begin observing the fleet." : "An administrator needs to add a MeshDB connection."} action={isAdmin && <Button onClick={startAdd}>Add connection</Button>} /> : <div className="grid gap-2 lg:grid-cols-2 2xl:grid-cols-3">
        {list.map((connection) => <ConnectionRecord
          key={connection.name}
          connection={connection}
          isAdmin={isAdmin}
          testing={testing === connection.name}
          testResult={testResults[connection.name]}
          onOpen={() => nav(`/c/${encodeURIComponent(connection.name)}/overview`)}
          onTest={() => void test(connection.name)}
          onVerify={() => beginVerify(connection)}
          onEdit={() => startEdit(connection)}
          onRemove={() => void remove(connection.name)}
        />)}
      </div>}
    </Page>
  );
}

function ConnectionRecord({ connection, isAdmin, testing, testResult, onOpen, onTest, onVerify, onEdit, onRemove }: {
  connection: api.Connection;
  isAdmin: boolean;
  testing: boolean;
  testResult?: { ok: boolean; message: string };
  onOpen: () => void;
  onTest: () => void;
  onVerify: () => void;
  onEdit: () => void;
  onRemove: () => void;
}) {
  return <article className={`border border-carbon-border border-l-4 bg-carbon-layer ${connection.enabled ? "border-l-carbon-green" : "border-l-carbon-text-3"}`}>
    <div className="p-3">
      <div className="flex items-start justify-between gap-3"><div className="min-w-0"><h2 className="truncate text-lg font-semibold">{connection.name}</h2><p className="mt-1 truncate font-mono text-xs text-carbon-text-3" title={connection.seeds.join("\n")}>{connection.url}</p></div><Tag tone={connection.enabled ? "green" : "gray"}>{connection.enabled ? "enabled" : "disabled"}</Tag></div>
      <dl className="mt-3 grid grid-cols-3 gap-2 border-y border-carbon-border py-2.5 text-xs"><div><dt className="font-mono text-[9px] uppercase tracking-wider text-carbon-text-3">Endpoints</dt><dd className="mt-1 font-mono">{connection.seeds.length}</dd></div><div><dt className="font-mono text-[9px] uppercase tracking-wider text-carbon-text-3">Identity</dt><dd className="mt-1 truncate font-mono">{connection.meshdb_user ?? "none"}</dd></div><div><dt className="font-mono text-[9px] uppercase tracking-wider text-carbon-text-3">Timeout</dt><dd className="mt-1 font-mono">{Math.round(connection.timeout_ms / 1000)}s</dd></div></dl>
      {testResult && <p className={`mt-3 text-xs ${testResult.ok ? "text-carbon-green" : "text-carbon-red"}`}>{testResult.ok ? "Connected" : "Connection failed"} · {testResult.message}</p>}
    </div>
    <div className="flex flex-wrap items-center gap-1 border-t border-carbon-border p-2">
      <Button variant="ghost" disabled={!connection.enabled} onClick={onOpen}>Open database</Button>
      {isAdmin && <><Button variant="ghost" disabled={testing} onClick={onTest}>{testing ? "Testing…" : "Test connection"}</Button><Button variant="ghost" onClick={onVerify}>Verify new node</Button><Button className="ml-auto" variant="ghost" onClick={onEdit}>Edit</Button><Button variant="ghost" className="text-carbon-red" onClick={onRemove}>Remove</Button></>}
    </div>
  </article>;
}

function VerificationFact({ label, value }: { label: string; value: string }) {
  return <div className="bg-carbon-layer2 p-3"><dt className="font-mono text-[9px] uppercase tracking-wider text-carbon-text-3">{label}</dt><dd className="mt-1 font-mono text-xs">{value}</dd></div>;
}
