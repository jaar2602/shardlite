import { useEffect, useMemo, useState } from "react";
import * as api from "../lib/api";
import { Banner, Button, Card, DataTable, Page, PageHeader, Spinner, Tag, TextInput } from "../components/ui";

export default function Schema({ name }: { name: string }) {
  const [distribution, setDistribution] = useState<Map<string, api.TableDistribution>>(new Map());
  const [catalog, setCatalog] = useState<api.SchemaCatalog | null>(null);
  const [agreement, setAgreement] = useState<api.SchemaAgreement | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [filter, setFilter] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [tab, setTab] = useState<"structure" | "data">("structure");
  const [data, setData] = useState<api.MaterializedQueryResult | null>(null);
  const [dataBusy, setDataBusy] = useState(false);
  const [dataError, setDataError] = useState<string | null>(null);

  const load = async () => {
    setBusy(true);
    setError(null);
    try {
      const [next, agree, dist] = await Promise.all([
        api.conn(name).schemaCatalog(),
        api.conn(name).schemaAgreement().catch(() => null),
        // Older servers have no /v1/tables; the schema is still worth showing without it.
        api.conn(name).tableDistribution().catch(() => null),
      ]);
      setCatalog(next);
      setAgreement(agree);
      setDistribution(new Map((dist?.tables ?? []).map((entry) => [entry.table.toLowerCase(), entry])));
      setSelected((current) => current && next.tables.some((table) => table.name === current)
        ? current
        : next.tables[0]?.name ?? null);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "The database schema could not be loaded.");
    } finally {
      setBusy(false);
    }
  };

  useEffect(() => { void load(); /* eslint-disable-next-line react-hooks/exhaustive-deps */ }, [name]);

  const visible = useMemo(() => {
    const term = filter.trim().toLowerCase();
    return (catalog?.objects ?? []).filter((object) => !term || `${object.type} ${object.name} ${object.table} ${object.sql ?? ""}`.toLowerCase().includes(term));
  }, [catalog, filter]);
  const table = catalog?.tables.find((item) => item.name === selected);
  const tableObject = catalog?.objects.find((item) => item.type === "table" && item.name === selected);

  // Switching tables resets to the Structure tab; the Data tab lazy-loads its own rows.
  useEffect(() => {
    setTab("structure");
    setData(null);
    setDataError(null);
  }, [selected]);

  const openData = async () => {
    setTab("data");
    if (!table || data || dataBusy) return;
    setDataBusy(true);
    setDataError(null);
    try {
      const result = await api.conn(name).queryAll(`SELECT * FROM "${table.name.replace(/"/g, '""')}" LIMIT 100`);
      setData(result);
    } catch (caught) {
      setDataError(caught instanceof Error ? caught.message : "The table data could not be loaded.");
    } finally {
      setDataBusy(false);
    }
  };

  return <Page className="flex h-[calc(100vh-3.5rem)] flex-col">
    <PageHeader
      eyebrow="Database / schema"
      title="Schema explorer"
      description="Browse the tables, views, indexes, and triggers available throughout this database. ShardLite checks physical storage internally."
      actions={<Button variant="secondary" onClick={() => void load()} disabled={busy}>{busy ? "Checking…" : "Refresh schema"}</Button>}
    />

    <div className="flex flex-wrap items-end gap-3 border border-carbon-border bg-carbon-layer p-3">
      <div className="min-w-64 flex-1 max-w-md"><TextInput label="Find a schema object" placeholder="Table, index, trigger…" value={filter} onChange={(event) => setFilter(event.target.value)} /></div>
      {catalog?.schema_version != null && <Tag tone="blue">schema version {catalog.schema_version}</Tag>}
      {catalog && <Tag tone={catalog.consistency.status === "consistent" ? "green" : catalog.consistency.status === "drifted" ? "yellow" : "gray"}>{catalog.consistency.status}</Tag>}
      {agreement && <Tag tone={agreement.status === "agreed" ? "green" : "red"}>{agreement.status === "agreed" ? `agreed · v${agreement.version}` : `disagreed · ${agreement.behind} behind`}</Tag>}
    </div>

    {busy && !catalog && <Spinner label="Reading the database schema…" />}
    {error && <Banner tone="error">{error}</Banner>}
    {catalog && <Banner tone={catalog.consistency.status === "consistent" ? "success" : catalog.consistency.status === "drifted" ? "error" : "info"}>{catalog.consistency.summary}</Banner>}

    {/* Fill the remaining viewport rather than growing the page: each column scrolls on its own,
        so the object list and the structure/data panel stay visible together instead of the whole
        page scrolling and the list disappearing off the top. */}
    <div className="grid min-h-0 flex-1 gap-3 xl:grid-cols-[20rem_minmax(0,1fr)] xl:overflow-hidden">
      <Card
        title={`${visible.length} schema object${visible.length === 1 ? "" : "s"}`}
        className="flex min-h-0 flex-col xl:overflow-hidden"
        bodyClassName="min-h-0 flex-1 overflow-hidden"
      >
        <div className="h-full min-h-0 space-y-1 overflow-auto">
          {visible.length === 0 && <p className="py-6 text-center text-sm text-carbon-text-3">No matching objects</p>}
          {visible.map((object) => <button
            key={`${object.type}:${object.name}`}
            disabled={object.type !== "table"}
            title={object.type === "table" ? `Inspect ${object.name}` : `${object.type} definition appears with its related table`}
            onClick={() => setSelected(object.name)}
            className={`flex w-full items-start justify-between gap-3 border-l-2 px-3 py-2 text-left disabled:cursor-default ${selected === object.name && object.type === "table" ? "border-carbon-blue bg-carbon-layer2" : object.type === "table" ? "border-transparent hover:bg-carbon-layer2/50" : "border-transparent opacity-70"}`}
          >
            <span className="min-w-0"><span className="block truncate text-sm text-carbon-text">{object.name}</span><span className="block truncate text-xs text-carbon-text-3">{object.table}</span></span>
            <Tag tone={objectTone(object.type)}>{object.type}</Tag>
          </button>)}
        </div>
      </Card>

      <div className="flex min-h-0 flex-col space-y-3 xl:overflow-y-auto">
        {table ? <>
          <div className="flex shrink-0 gap-1 border-b border-carbon-border">
            <SchemaTab active={tab === "structure"} onClick={() => setTab("structure")}>Structure</SchemaTab>
            <SchemaTab active={tab === "data"} onClick={() => void openData()}>Data</SchemaTab>
          </div>
          {tab === "structure" ? <>
            <Card
              title={<span>
                {table.name} <Tag tone="green">table</Tag>{" "}
                {(() => {
                  const spread = distribution.get(table.name.toLowerCase());
                  if (!spread) return null;
                  // The class decides what this table's constraints mean, so it belongs beside its
                  // name rather than being something you have to know from elsewhere.
                  return spread.class === "global"
                    ? <Tag tone="blue">global · one shard, full SQLite constraints</Tag>
                    : <Tag tone="gray">sharded{spread.shard_key ? ` by ${spread.shard_key}` : ""}</Tag>;
                })()}
              </span>}
            ><pre className="overflow-x-auto whitespace-pre-wrap font-mono text-xs text-carbon-text-2">{table.sql ?? tableObject?.sql ?? "No CREATE statement reported"}</pre></Card>
            <Card title="Columns"><DataTable columns={["CID", "Name", "Type", "Not null", "Default", "PK", "Hidden"]} empty="No columns" rows={table.columns.map((row) => row.map(cell))} /></Card>
            <Card title="Indexes"><DataTable columns={["Seq", "Name", "Unique", "Origin", "Partial"]} empty="No indexes" rows={table.indexes.map((row) => row.map(cell))} /></Card>
            <Card title="Foreign keys"><DataTable columns={["ID", "Seq", "Target table", "From", "To", "On update", "On delete", "Match"]} empty="No foreign keys" rows={table.foreign_keys.map((row) => row.map(cell))} /></Card>
            <Card title="Related definitions"><div className="space-y-3">{(catalog?.objects ?? []).filter((object) => object.table === table.name && object.name !== table.name).length === 0 ? <p className="text-sm text-carbon-text-3">No indexes or triggers with stored definitions.</p> : (catalog?.objects ?? []).filter((object) => object.table === table.name && object.name !== table.name).map((object) => <div key={`${object.type}:${object.name}`} className="border-t border-carbon-border pt-3"><div className="mb-2 flex items-center gap-2"><Tag tone={objectTone(object.type)}>{object.type}</Tag><span className="text-sm">{object.name}</span></div><pre className="overflow-x-auto whitespace-pre-wrap font-mono text-xs text-carbon-text-3">{object.sql ?? "Implicit definition"}</pre></div>)}</div></Card>
          </> : <Card
            title={<span>{table.name} <Tag tone="blue">data</Tag></span>}
            className="flex min-h-0 flex-1 flex-col"
            bodyClassName="flex min-h-0 flex-1 flex-col"
          >
            {dataBusy && <Spinner label="Reading table data…" />}
            {dataError && <Banner tone="error">{dataError}</Banner>}
            {data && !dataBusy && !dataError && <div className="flex min-h-0 flex-1 flex-col space-y-2">
              <p className="shrink-0 text-xs text-carbon-text-3">first 100 rows</p>
              <DataTable
                columns={data.columns}
                empty="No rows"
                rows={data.rows.map((row) => row.map(dataCell))}
                scrollClassName="min-h-0 flex-1"
              />
            </div>}
          </Card>}
        </> : <Card><p className="py-12 text-center text-sm text-carbon-text-3">Select a table to inspect its columns, indexes, foreign keys, triggers, and SQL definition.</p></Card>}
      </div>
    </div>
  </Page>;
}

function objectTone(type: string): "green" | "blue" | "yellow" | "gray" { return type === "table" ? "green" : type === "index" ? "blue" : type === "trigger" ? "yellow" : "gray"; }
function cell(value: unknown) { return value === null ? <span className="italic text-carbon-text-3">NULL</span> : typeof value === "object" ? JSON.stringify(value) : String(value); }
function SchemaTab({ active, onClick, children }: { active: boolean; onClick: () => void; children: React.ReactNode }) { return <button className={`border-b-2 px-3 py-2 text-sm ${active ? "border-carbon-blue text-carbon-text" : "border-transparent text-carbon-text-3"}`} onClick={onClick}>{children}</button>; }
function isBlob(value: unknown): value is number[] { return Array.isArray(value) && value.every((item) => Number.isInteger(item) && item >= 0 && item <= 255); }
function dataCell(value: unknown) {
  if (value === null) return <span className="italic text-carbon-text-3">NULL</span>;
  if (isBlob(value)) { const hex = value.slice(0, 12).map((item) => item.toString(16).padStart(2, "0")).join(""); return <span className="text-carbon-yellow">BLOB · {value.length} bytes · {hex}{value.length > 12 ? "…" : ""}</span>; }
  return typeof value === "object" ? JSON.stringify(value) : String(value);
}
