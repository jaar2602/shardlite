import type { ReactNode } from "react";
import { Tag } from "./ui";

// A spatial "which shards live where" view: cluster nodes as cards, each holding its shards as
// chips. Every shard-oriented view (storage, replication, ERD, operations) feeds it the same
// node→shard shape, colouring/labelling each chip with its own metric.
export type ShardTone = "gray" | "blue" | "green" | "red" | "yellow";

export interface ShardCell {
  shard: number;
  tone?: ShardTone;
  label?: string; // small caption under the shard number (row count, lag, …)
  title?: string; // hover tooltip
}

export interface TopologyNode {
  id: string; // node id / owner
  leader?: boolean;
  shards: ShardCell[];
}

const CHIP: Record<ShardTone, string> = {
  gray: "bg-carbon-layer2 text-carbon-text-2 border-carbon-border",
  blue: "bg-carbon-blue/15 text-carbon-blue border-carbon-blue/40",
  green: "bg-carbon-green/15 text-carbon-green border-carbon-green/40",
  red: "bg-carbon-red/15 text-carbon-red border-carbon-red/40",
  yellow: "bg-carbon-yellow/15 text-carbon-yellow border-carbon-yellow/40",
};

/// Group rows that know their owner node into topology nodes, sorted by node then shard. `cell`
/// builds each chip's tone/label/title from the row.
export function groupByOwner<T extends { id: number; owner?: string | null; primary_node?: string | null }>(
  rows: T[],
  cell: (row: T) => Omit<ShardCell, "shard">,
): TopologyNode[] {
  const byNode = new Map<string, ShardCell[]>();
  for (const row of rows) {
    const owner = row.owner ?? row.primary_node ?? "unassigned";
    const list = byNode.get(owner) ?? [];
    list.push({ shard: row.id, ...cell(row) });
    byNode.set(owner, list);
  }
  return Array.from(byNode, ([id, shards]) => ({
    id,
    shards: shards.sort((a, b) => a.shard - b.shard),
  })).sort((a, b) => a.id.localeCompare(b.id, undefined, { numeric: true }));
}

export function ShardTopology({
  nodes,
  selected,
  onSelect,
  legend,
  empty,
}: {
  nodes: TopologyNode[];
  selected?: number | null;
  onSelect?: (shard: number) => void;
  legend?: ReactNode;
  empty?: string;
}) {
  if (nodes.length === 0) {
    return <p className="text-sm text-carbon-text-3">{empty ?? "No shard placement to show yet."}</p>;
  }
  return (
    <div className="space-y-3">
      {legend && <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-carbon-text-3">{legend}</div>}
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
        {nodes.map((node) => (
          <div key={node.id} className="border border-carbon-border bg-carbon-layer">
            <div className="flex items-center justify-between gap-2 border-b border-carbon-border bg-carbon-field px-3 py-1.5">
              <span className="truncate font-mono text-xs font-semibold text-carbon-text" title={node.id}>
                {/^\d+$/.test(node.id) ? `node ${node.id}` : node.id}
              </span>
              <span className="flex shrink-0 items-center gap-2">
                {node.leader && <Tag tone="blue">leader</Tag>}
                <span className="text-[10px] text-carbon-text-3">{node.shards.length}</span>
              </span>
            </div>
            <div className="flex flex-wrap gap-1.5 p-2">
              {node.shards.map((cell) => {
                const active = selected != null && selected === cell.shard;
                return (
                  <button
                    key={cell.shard}
                    type="button"
                    title={cell.title}
                    onClick={onSelect ? () => onSelect(cell.shard) : undefined}
                    className={`flex min-w-[2.75rem] flex-col items-center border px-1.5 py-1 leading-tight ${CHIP[cell.tone ?? "gray"]} ${active ? "ring-2 ring-carbon-blue" : ""} ${onSelect ? "cursor-pointer hover:opacity-80" : "cursor-default"}`}
                  >
                    <span className="font-mono text-xs font-semibold">{cell.shard}</span>
                    {cell.label && <span className="text-[10px] opacity-80">{cell.label}</span>}
                  </button>
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// A small coloured-dot legend entry, for the views to describe their tones.
export function LegendDot({ tone, children }: { tone: ShardTone; children: ReactNode }) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <span className={`inline-block h-2.5 w-2.5 border ${CHIP[tone]}`} />
      {children}
    </span>
  );
}
