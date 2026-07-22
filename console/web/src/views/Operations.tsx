import { useCallback, useEffect, useMemo, useState } from "react";
import { useAuth } from "../auth";
import * as api from "../lib/api";
import { Banner, Button, Card, DataTable, EmptyState, Page, PageHeader, Select, Spinner, StatCard, Tag } from "../components/ui";

export default function Operations({ name }: { name: string }) {
  const { me } = useAuth();
  const [records, setRecords] = useState<api.OperationRecord[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [filter, setFilter] = useState<api.OperationStatus | "all">("all");
  const [busy, setBusy] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async (quiet = false) => {
    if (!quiet) setBusy(true);
    try {
      const value = (await api.operations.list()).filter((operation) => operation.connection === name);
      setRecords(value);
      setSelectedId((current) => current && value.some((operation) => operation.id === current)
        ? current
        : value[0]?.id ?? null);
      setError(null);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "failed to load operations");
    } finally {
      if (!quiet) setBusy(false);
    }
  }, [name]);

  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(true), 2000);
    return () => window.clearInterval(timer);
  }, [load]);

  const selected = records.find((operation) => operation.id === selectedId) ?? null;
  const visible = useMemo(() => records.filter((operation) => filter === "all" || operation.status === filter), [filter, records]);
  const active = records.filter((operation) => operation.status === "queued" || operation.status === "running").length;
  const attention = records.filter((operation) => operation.status === "partial" || operation.status === "interrupted" || operation.status === "failed").length;

  const cancel = async () => {
    if (!selected) return;
    setError(null);
    try {
      const value = await api.operations.cancel(selected.id);
      setRecords((current) => current.map((operation) => operation.id === value.id ? value : operation));
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "cancellation failed");
    }
  };

  return <Page>
    <PageHeader eyebrow="Changes / durable journal" title="Operations" description="Track approved schema changes from review through a database-wide outcome. The console coordinates existing MeshDB APIs and never edits database files." actions={<Button variant="secondary" onClick={() => void load()}>Refresh now</Button>} />
    <div className="grid gap-px bg-carbon-border sm:grid-cols-3">
      <StatCard label="Recorded" value={records.length} />
      <StatCard label="Queued / running" value={active} tone={active ? "blue" : undefined} />
      <StatCard label="Needs attention" value={attention} tone={attention ? "red" : undefined} />
    </div>
    {busy && <Spinner label="Loading durable operation journal…" />}
    {error && <Banner tone="error">{error}</Banner>}
    <div className="grid gap-4 xl:grid-cols-[minmax(34rem,1fr)_minmax(28rem,0.8fr)]">
      <Card title="Operation history" actions={<div className="w-44"><Select aria-label="Filter operation status" value={filter} onChange={(event) => setFilter(event.target.value as typeof filter)}><option value="all">all statuses</option>{["queued", "running", "succeeded", "partial", "failed", "cancelled", "interrupted"].map((status) => <option key={status}>{status}</option>)}</Select></div>}>
        <DataTable columns={["Created", "Status", "Actor", "Stage", "Operation"]} empty="No schema rollout operations for this connection" rows={visible.map((operation) => [
          new Date(operation.created_at_ms).toLocaleString(),
          <StatusTag status={operation.status} />,
          operation.actor,
          operation.stage.replace(/_/g, " "),
          <button className="text-left text-carbon-blue hover:underline" onClick={() => setSelectedId(operation.id)}>{shortId(operation.id)}</button>,
        ])} />
      </Card>
      {selected ? <OperationDetail operation={selected} showInternals={me?.role === "admin"} onCancel={() => void cancel()} /> : <EmptyState title={records.length ? "Select an operation" : "No operations recorded"} description={records.length ? "Choose an operation to inspect its approval evidence and database-wide outcome." : "Schema changes submitted from the SQL editor will appear here."} />}
    </div>
  </Page>;
}

function OperationDetail({ operation, showInternals, onCancel }: { operation: api.OperationRecord; showInternals: boolean; onCancel: () => void }) {
  const versions = groupVersions(operation.expected_versions);
  const mayCancel = operation.status === "queued" || (operation.status === "running" && operation.stage === "revalidating_preflight");
  const succeeded = operation.outcomes.filter((outcome) => outcome.ok).length;
  return <Card title={<span>{shortId(operation.id)} <StatusTag status={operation.status} /></span>} actions={mayCancel && <Button variant="danger" onClick={onCancel}>Cancel before execution</Button>}>
    <div className="space-y-4">
      {(operation.status === "partial" || operation.status === "interrupted") && <Banner tone="error">{operation.status === "partial" ? "The schema update was applied to only part of the database. Review the technical evidence and roll forward deliberately." : "The console restarted while this operation was in flight. It was not replayed. Verify the database schema before taking another action."}</Banner>}
      {operation.error && <Banner tone="error">{logicalError(operation.error)}</Banner>}
      {operation.cancel_requested && operation.status === "running" && <Banner tone="info">Cancellation is requested and will be honored before the MeshDB call if revalidation has not completed.</Banner>}
      <dl className="grid grid-cols-[9rem_1fr] gap-x-3 gap-y-2 text-xs">
        <dt className="text-carbon-text-3">Connection</dt><dd>{operation.connection}</dd>
        <dt className="text-carbon-text-3">Actor</dt><dd>{operation.actor}</dd>
        <dt className="text-carbon-text-3">Created</dt><dd>{new Date(operation.created_at_ms).toLocaleString()}</dd>
        <dt className="text-carbon-text-3">Updated</dt><dd>{new Date(operation.updated_at_ms).toLocaleString()}</dd>
        <dt className="text-carbon-text-3">Fingerprint</dt><dd className="break-all font-mono">{operation.sql_fingerprint}</dd>
        <dt className="text-carbon-text-3">Idempotency key</dt><dd className="break-all font-mono">{operation.idempotency_key}</dd>
      </dl>
      <div><div className="mb-2 text-xs font-semibold">Approved SQL</div><pre className="max-h-48 overflow-auto whitespace-pre-wrap border border-carbon-border bg-carbon-field p-3 font-mono text-xs">{operation.sql}</pre></div>
      <div className="border border-carbon-border bg-carbon-layer2 p-3 text-xs"><span className="text-carbon-text-3">Database application</span><span className="ml-3 font-mono">{operation.outcomes.length ? `${succeeded}/${operation.outcomes.length} internal steps applied` : "waiting"}</span></div>
      {showInternals && <details className="border border-carbon-border"><summary className="cursor-pointer px-3 py-2 text-xs font-semibold">Storage internals</summary><div className="space-y-3 border-t border-carbon-border p-3"><div><div className="mb-2 text-xs font-semibold">Approved schema versions</div>{versions.map((group) => <p key={group.version} className="font-mono text-xs text-carbon-text-3">v{group.version}: units {compact(group.shards)}</p>)}</div>{operation.outcomes.length > 0 && <DataTable columns={["Unit", "Result", "Error"]} rows={operation.outcomes.map((outcome) => [outcome.shard, <Tag tone={outcome.ok ? "green" : "red"}>{outcome.ok ? "applied" : "rejected"}</Tag>, outcome.error ?? "—"])} />}</div></details>}
      {operation.status === "running" && operation.stage === "executing_on_meshdb" && <Banner tone="info">MeshDB is applying the rollout. Cancellation is no longer safe because part of the database may already have changed.</Banner>}
    </div>
  </Card>;
}

function StatusTag({ status }: { status: api.OperationStatus }) {
  const tone = status === "succeeded" ? "green" : status === "queued" || status === "running" ? "blue" : status === "cancelled" ? "gray" : status === "partial" ? "yellow" : "red";
  return <Tag tone={tone}>{status.toUpperCase()}</Tag>;
}

function groupVersions(versions: api.ShardVersion[]) {
  return [...new Set(versions.map((item) => item.schema_version))].map((version) => ({
    version,
    shards: versions.filter((item) => item.schema_version === version).map((item) => item.shard),
  }));
}

function compact(values: number[]) { return values.length <= 20 ? values.join(", ") : `${values.slice(0, 12).join(", ")} … (${values.length} total)`; }
function shortId(value: string) { return value.length > 24 ? `${value.slice(0, 21)}…` : value; }
function logicalError(value: string) { return value.replace(/\bshard\s+\d+\b/gi, "part of the database").replace(/\bshards?\b/gi, "database storage"); }
