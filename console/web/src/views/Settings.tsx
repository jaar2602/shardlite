import { useEffect, useState } from "react";
import { useAuth } from "../auth";
import * as api from "../lib/api";
import { Banner, Button, Card, DataTable, Page, PageHeader, Spinner, Tag, TextInput } from "../components/ui";

function cell(value: unknown) {
  return value === null || value === undefined
    ? <span className="italic text-carbon-text-3">—</span>
    : typeof value === "object"
      ? JSON.stringify(value)
      : String(value);
}

export default function Settings({ name }: { name: string }) {
  const { me } = useAuth();
  const canOperate = api.permits(me?.role, "operate");
  const [config, setConfig] = useState<api.NodeConfig | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [form, setForm] = useState({ table: "", column: "" });
  const [notice, setNotice] = useState<string | null>(null);

  const load = async () => {
    setBusy(true);
    setError(null);
    try {
      setConfig(await api.conn(name).config());
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Node configuration could not be loaded.");
    } finally {
      setBusy(false);
    }
  };

  useEffect(() => { void load(); /* eslint-disable-next-line react-hooks/exhaustive-deps */ }, [name]);

  const declareShardKey = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!confirm(`Declare shard key ${form.table}.${form.column} on every node?`)) return;
    setError(null);
    setNotice(null);
    try {
      const result = await api.conn(name).shardkey(form.table, form.column);
      setNotice(`Applied on ${result.applied.length} node${result.applied.length === 1 ? "" : "s"}${result.failures.length ? `, ${result.failures.length} failed` : ""}.`);
      setForm({ table: "", column: "" });
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "The shard key could not be declared.");
    }
  };

  return (
    <Page>
      <PageHeader
        eyebrow="Database / configuration"
        title="Settings"
        description="Effective node configuration and shard-key declarations for this database."
        actions={<Button variant="secondary" onClick={() => void load()} disabled={busy}>{busy ? "Loading…" : "Refresh"}</Button>}
      />
      {error && <Banner tone="error">{error}</Banner>}
      {notice && <Banner tone="success">{notice}</Banner>}

      {canOperate && <Card title="Declare shard key">
        <form onSubmit={declareShardKey} className="grid grid-cols-1 items-end gap-4 md:grid-cols-3">
          <TextInput label="Table" value={form.table} onChange={(event) => setForm({ ...form, table: event.target.value })} required />
          <TextInput label="Column" value={form.column} onChange={(event) => setForm({ ...form, column: event.target.value })} required />
          <div><Button type="submit">Declare on all nodes</Button></div>
        </form>
      </Card>}

      {config === null ? (
        <Spinner label="Loading configuration…" />
      ) : (
        <DataTable
          columns={["Key", "Value", "Mutable", "Note"]}
          empty="No configuration reported."
          rows={config.settings.map((setting) => [
            setting.key,
            cell(setting.value),
            <Tag tone={setting.mutable ? "green" : "gray"}>{setting.mutable ? "mutable" : "fixed"}</Tag>,
            <span className="whitespace-normal text-carbon-text-3">{setting.note || "—"}</span>,
          ])}
        />
      )}
    </Page>
  );
}
