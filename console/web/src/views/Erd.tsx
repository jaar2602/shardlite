import { useCallback, useEffect, useMemo, useState } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  Handle,
  Position,
  type Edge,
  type Node,
  type NodeProps,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import * as api from "../lib/api";
import { Banner, Card, Spinner, Tag } from "../components/ui";

// The view has three layers over the same set of tables:
//   erd      — the logical schema: tables + foreign-key relationships.
//   shard    — where a table's ROWS physically live (per-shard counts + owner node).
//   shard-s3 — the shard layer plus what each shard has archived to S3.
type Layer = "erd" | "shard" | "shard-s3";

// The catalog rows are pragma output, carried as loosely-typed arrays. Read them positionally:
// columns = pragma_table_xinfo (cid,name,type,notnull,dflt,pk,hidden);
// foreign_keys = pragma_foreign_key_list (id,seq,table,from,to,...).
type Column = { name: string; type: string; pk: boolean; notnull: boolean };
function readColumn(row: unknown[]): Column {
  return {
    name: String(row[1] ?? ""),
    type: String(row[2] ?? ""),
    notnull: Number(row[3] ?? 0) > 0,
    pk: Number(row[5] ?? 0) > 0,
  };
}
function readForeignKey(row: unknown[]): { table: string; from: string; to: string } {
  return { table: String(row[2] ?? ""), from: String(row[3] ?? ""), to: String(row[4] ?? "") };
}

type TableNodeData = { name: string; columns: Column[]; extra: number };

function TableNode({ data }: NodeProps) {
  const d = data as unknown as TableNodeData;
  return (
    <div className="min-w-[190px] border border-carbon-border bg-carbon-layer text-xs shadow-md">
      <Handle type="target" position={Position.Left} className="!bg-carbon-blue" />
      <Handle type="source" position={Position.Right} className="!bg-carbon-blue" />
      <div className="border-b border-carbon-border bg-carbon-field px-2 py-1 font-semibold text-carbon-text">
        {d.name}
      </div>
      <div className="divide-y divide-carbon-border/40">
        {d.columns.map((c) => (
          <div key={c.name} className="flex items-center justify-between gap-3 px-2 py-0.5">
            <span className={c.pk ? "font-semibold text-carbon-text" : "text-carbon-text-2"}>
              {c.pk ? "🔑 " : ""}
              {c.name}
            </span>
            <span className="font-mono text-[10px] text-carbon-text-3">{c.type}</span>
          </div>
        ))}
        {d.extra > 0 && (
          <div className="px-2 py-0.5 text-[10px] text-carbon-text-3">+{d.extra} more…</div>
        )}
      </div>
    </div>
  );
}
const nodeTypes = { table: TableNode };

const MAX_COLUMNS = 12;

export default function Erd({ name }: { name: string }) {
  const [layer, setLayer] = useState<Layer>("erd");
  const [catalog, setCatalog] = useState<api.SchemaCatalog | null>(null);
  const [inventory, setInventory] = useState<api.ShardInventory | null>(null);
  const [s3, setS3] = useState<api.S3Status | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [placement, setPlacement] = useState<api.TablePlacement | null>(null);
  const [placementBusy, setPlacementBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Load the schema (+ shard/S3 context) once. The shard layers reuse inventory/s3 already fetched.
  useEffect(() => {
    let live = true;
    const c = api.conn(name);
    Promise.all([c.schemaCatalog(), c.shardInventory().catch(() => null), c.s3.status().catch(() => null)])
      .then(([cat, inv, s3s]) => {
        if (!live) return;
        setCatalog(cat);
        setInventory(inv);
        setS3(s3s);
        setSelected((prev) => prev ?? cat.tables[0]?.name ?? null);
      })
      .catch((e) => live && setError(e instanceof Error ? e.message : "Failed to load the schema."));
    return () => {
      live = false;
    };
  }, [name]);

  // Fetch per-shard row counts for the selected table when a shard layer is active.
  useEffect(() => {
    if (layer === "erd" || !selected) return;
    if (placement?.table === selected) return;
    let live = true;
    setPlacementBusy(true);
    api
      .conn(name)
      .tablePlacement(selected)
      .then((p) => live && setPlacement(p))
      .catch((e) => live && setError(e instanceof Error ? e.message : "Failed to read placement."))
      .finally(() => live && setPlacementBusy(false));
    return () => {
      live = false;
    };
  }, [layer, selected, name, placement?.table]);

  const tables = useMemo(() => catalog?.tables ?? [], [catalog]);

  // ERD graph: one node per table, edges from each foreign key to its referenced table.
  const { nodes, edges } = useMemo(() => {
    const names = new Set(tables.map((t) => t.name));
    const cols = Math.max(1, Math.ceil(Math.sqrt(tables.length)));
    const nodes: Node[] = tables.map((t, i) => {
      const columns = t.columns.map(readColumn);
      return {
        id: t.name,
        type: "table",
        position: { x: (i % cols) * 320, y: Math.floor(i / cols) * 280 },
        data: {
          name: t.name,
          columns: columns.slice(0, MAX_COLUMNS),
          extra: Math.max(0, columns.length - MAX_COLUMNS),
        },
      };
    });
    const edges: Edge[] = [];
    for (const t of tables) {
      t.foreign_keys.forEach((row, i) => {
        const fk = readForeignKey(row);
        if (!names.has(fk.table) || fk.table === t.name) return;
        edges.push({
          id: `${t.name}:${fk.from}->${fk.table}:${fk.to}:${i}`,
          source: t.name,
          target: fk.table,
          label: `${fk.from} → ${fk.to}`,
          animated: false,
          style: { stroke: "#4589ff" },
          labelStyle: { fontSize: 10 },
        });
      });
    }
    return { nodes, edges };
  }, [tables]);

  const onNodeClick = useCallback((_: unknown, node: Node) => {
    setSelected(node.id);
    setLayer((l) => (l === "erd" ? "shard" : l));
  }, []);

  if (error && !catalog) return <div className="p-6"><Banner tone="error">{error}</Banner></div>;
  if (!catalog) return <div className="p-6"><Spinner label="Loading schema…" /></div>;

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-carbon-border bg-carbon-layer px-4 py-2">
        <div>
          <div className="text-sm font-semibold text-carbon-text">Entity-relationship diagram</div>
          <div className="text-xs text-carbon-text-3">
            {tables.length} tables · {catalog.consistency.status} schema across the cluster
          </div>
        </div>
        <Segmented layer={layer} onChange={setLayer} s3Supported={s3?.supported ?? false} />
      </div>

      {error && <Banner tone="error">{error}</Banner>}

      {layer === "erd" ? (
        <div className="min-h-0 flex-1">
          {tables.length === 0 ? (
            <p className="p-6 text-sm text-carbon-text-3">This database has no tables yet.</p>
          ) : (
            <ReactFlow
              nodes={nodes}
              edges={edges}
              nodeTypes={nodeTypes}
              onNodeClick={onNodeClick}
              nodesConnectable={false}
              fitView
              proOptions={{ hideAttribution: true }}
            >
              <Background />
              <Controls showInteractive={false} />
            </ReactFlow>
          )}
        </div>
      ) : (
        <ShardLayer
          layer={layer}
          tables={tables.map((t) => t.name)}
          selected={selected}
          onSelect={setSelected}
          placement={placement}
          busy={placementBusy}
          inventory={inventory}
          s3={s3}
        />
      )}
    </div>
  );
}

function Segmented({
  layer,
  onChange,
  s3Supported,
}: {
  layer: Layer;
  onChange: (l: Layer) => void;
  s3Supported: boolean;
}) {
  const opts: { key: Layer; label: string; title?: string }[] = [
    { key: "erd", label: "ERD" },
    { key: "shard", label: "Shard" },
    {
      key: "shard-s3",
      label: "Shard + S3",
      title: s3Supported ? undefined : "S3 archival is not enabled on this cluster",
    },
  ];
  return (
    <div className="inline-flex overflow-hidden border border-carbon-border">
      {opts.map((o) => (
        <button
          key={o.key}
          type="button"
          title={o.title}
          onClick={() => onChange(o.key)}
          className={`px-3 py-1 text-xs ${
            layer === o.key
              ? "bg-carbon-blue text-white"
              : "bg-carbon-layer text-carbon-text-2 hover:bg-carbon-field"
          }`}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}

function ShardLayer({
  layer,
  tables,
  selected,
  onSelect,
  placement,
  busy,
  inventory,
  s3,
}: {
  layer: Layer;
  tables: string[];
  selected: string | null;
  onSelect: (t: string) => void;
  placement: api.TablePlacement | null;
  busy: boolean;
  inventory: api.ShardInventory | null;
  s3: api.S3Status | null;
}) {
  const ownerByShard = useMemo(
    () => new Map((inventory?.rows ?? []).map((r) => [r.id, r.owner ?? r.primary_node ?? null])),
    [inventory],
  );
  const s3ByShard = useMemo(
    () => new Map((s3?.shards ?? []).map((r) => [r.shard, r])),
    [s3],
  );
  const maxRows = useMemo(
    () => Math.max(1, ...(placement?.shards ?? []).map((s) => s.rows ?? 0)),
    [placement],
  );
  const withS3 = layer === "shard-s3";

  return (
    <div className="min-h-0 flex-1 space-y-4 overflow-auto p-4">
      <div className="flex flex-wrap items-center gap-3">
        <label className="text-xs text-carbon-text-3">Table</label>
        <select
          className="border border-carbon-border bg-carbon-field px-2 py-1 text-sm text-carbon-text"
          value={selected ?? ""}
          onChange={(e) => onSelect(e.target.value)}
        >
          {tables.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
        {placement && (
          <span className="text-xs text-carbon-text-3">
            {placement.total_rows.toLocaleString()} rows across {placement.shard_count} shards
          </span>
        )}
      </div>

      {withS3 && s3 && !s3.supported && (
        <Banner tone="info">This cluster was built without S3 archival, so nothing resides in S3.</Banner>
      )}
      {withS3 && s3?.supported && !s3.configured && (
        <Banner tone="info">
          S3 archival is supported but not configured. Set the bucket on the connection and apply it from the{" "}
          <span className="font-semibold">Replication</span> view; per-shard archive status will then appear here.
        </Banner>
      )}

      {busy && !placement ? (
        <Spinner label="Counting rows per shard…" />
      ) : placement ? (
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-4">
          {placement.shards.map((sh) => {
            const owner = ownerByShard.get(sh.shard);
            const arc = s3ByShard.get(sh.shard);
            const rows = sh.rows;
            const pct = rows == null ? 0 : Math.round((rows / maxRows) * 100);
            return (
              <Card key={sh.shard} className="space-y-1.5">
                <div className="flex items-center justify-between">
                  <span className="font-mono text-xs text-carbon-text">shard {sh.shard}</span>
                  {owner ? <Tag tone="blue">{owner}</Tag> : <Tag tone="gray">owner ?</Tag>}
                </div>
                <div className="text-lg font-semibold text-carbon-text">
                  {rows == null ? "—" : rows.toLocaleString()}
                  <span className="ml-1 text-xs font-normal text-carbon-text-3">rows</span>
                </div>
                <div className="h-1.5 w-full bg-carbon-field">
                  <div className="h-full bg-carbon-blue" style={{ width: `${pct}%` }} />
                </div>
                {withS3 && (
                  <div className="border-t border-carbon-border pt-1.5 text-[11px] text-carbon-text-3">
                    {arc ? (
                      <>
                        <div>S3 snapshot LSN {arc.last_snapshot_lsn}</div>
                        <div>archived LSN {arc.last_archived_lsn}</div>
                      </>
                    ) : (
                      <div>not archived</div>
                    )}
                  </div>
                )}
              </Card>
            );
          })}
        </div>
      ) : (
        <p className="text-sm text-carbon-text-3">Select a table to see where its rows live.</p>
      )}
    </div>
  );
}
