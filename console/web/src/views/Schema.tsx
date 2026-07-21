import { useEffect, useState } from "react";
import * as api from "../lib/api";
import { Banner, DataTable, Spinner, Tag, TextInput } from "../components/ui";

// The gateway's /v1/schema returns only a version number, so the real object list comes from
// querying sqlite_schema over the streaming query path — the same one the SQL editor uses.
export default function Schema({ name }: { name: string }) {
  const c = api.conn(name);
  const [shard, setShard] = useState(0);
  const [version, setVersion] = useState<number | null>(null);
  const [objects, setObjects] = useState<api.QueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const load = async () => {
    setBusy(true);
    setError(null);
    setObjects(null);
    try {
      const v = (await c.schema(shard)) as { schema_version?: number };
      setVersion(v.schema_version ?? null);
      const r = await c.query(
        "SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
        { shard },
      );
      setObjects(r);
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to load schema");
    } finally {
      setBusy(false);
    }
  };
  useEffect(() => {
    void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [shard]);

  return (
    <div className="p-6 space-y-4 max-w-5xl">
      <div className="flex items-end gap-4">
        <div className="w-28">
          <TextInput
            label="Shard"
            type="number"
            min={0}
            value={shard}
            onChange={(e) => setShard(Number(e.target.value))}
          />
        </div>
        {version !== null && <Tag tone="blue">schema version {version}</Tag>}
      </div>

      {busy && <Spinner label="Reading schema…" />}
      {error && <Banner tone="error">{error}</Banner>}

      {objects && (
        <DataTable
          columns={["Type", "Name", "Table", "Definition"]}
          empty="No user objects on this shard"
          rows={objects.rows.map((r) => [
            <Tag tone={String(r[0]) === "table" ? "green" : "gray"}>{String(r[0])}</Tag>,
            String(r[1]),
            String(r[2]),
            <span className="text-carbon-text-3">{r[3] === null ? "" : String(r[3])}</span>,
          ])}
        />
      )}
    </div>
  );
}
