import { useCallback, useEffect, useMemo, useState } from "react";
import * as api from "../lib/api";
import { shardsByNode, topologyNodes } from "../lib/topology";
import TopologyMap, { STATUS_STYLES } from "./TopologyMap";
import { LegendDot, type ShardTone } from "./ShardTopology";
import { Spinner, Tag } from "./ui";

// The cluster map, shared by every shard-oriented page.
//
// Each of those pages used to draw its own flat node→chip grid, which answered "where are the
// shards" but not "what does this cluster look like" — and looked nothing like the Topology page,
// so the same cluster appeared to be two different things depending on which tab you were on. This
// renders the *same* isometric map as Topology, and each page supplies only what is specific to it:
// a caption per node, and a tone/label per shard.

export interface ShardAnnotation {
  tone?: ShardTone;
  label?: string;
  title?: string;
}

const CHIP: Record<ShardTone, string> = {
  gray: "bg-carbon-layer2 text-carbon-text-2 border-carbon-border",
  blue: "bg-carbon-blue/15 text-carbon-blue border-carbon-blue/40",
  green: "bg-carbon-green/15 text-carbon-green border-carbon-green/40",
  red: "bg-carbon-red/15 text-carbon-red border-carbon-red/40",
  yellow: "bg-carbon-yellow/15 text-carbon-yellow border-carbon-yellow/40",
};

export default function ClusterTopologyPanel({
  name,
  caption,
  annotateShard,
  onSelectShard,
  selectedShard,
  legend,
}: {
  name: string;
  /// A short line under each node, from the calling page's own data (lag, ops, bytes, writes/s).
  caption?: (nodeId: string, shards: number[]) => string | null;
  annotateShard?: (shard: number) => ShardAnnotation | null;
  onSelectShard?: (shard: number) => void;
  selectedShard?: number | null;
  legend?: React.ReactNode;
}) {
  const [info, setInfo] = useState<api.NodeInfo | null>(null);
  const [cluster, setCluster] = useState<api.ClusterInfo | null>(null);
  const [observation, setObservation] = useState<api.ClusterObservation | null>(null);
  const [connection, setConnection] = useState<api.Connection | undefined>();
  const [error, setError] = useState<string | null>(null);
  const [selectedNode, setSelectedNode] = useState<string>("");

  const load = useCallback(async () => {
    try {
      const c = api.conn(name);
      const [nodeInfo, clusterInfo, obs, conns] = await Promise.all([
        c.info(),
        c.cluster(),
        c.observation(),
        api.connections.list().catch(() => [] as api.Connection[]),
      ]);
      setInfo(nodeInfo);
      setCluster(clusterInfo);
      setObservation(obs);
      setConnection(conns.find((entry) => entry.name === name));
      setError(null);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Topology could not be loaded.");
    }
  }, [name]);

  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(), 5000);
    return () => window.clearInterval(timer);
  }, [load]);

  const nodes = useMemo(() => {
    if (!cluster || !observation || !info) return [];
    return topologyNodes({
      cluster,
      observation,
      shardCount: info.shard_count,
      connectionName: name,
      connectionUrl: connection?.url,
    });
  }, [cluster, observation, info, name, connection]);

  const owned = useMemo(() => {
    if (!cluster || !info) return new Map<string, number[]>();
    return shardsByNode(cluster, info.shard_count, nodes.find((n) => n.isCurrent)?.id ?? "local");
  }, [cluster, info, nodes]);

  if (error) return <p className="text-sm text-carbon-red">{error}</p>;
  if (!cluster || !info || !observation) return <Spinner label="Loading topology…" />;
  if (nodes.length === 0) return <p className="text-sm text-carbon-text-3">No node placement to show yet.</p>;

  const active = selectedNode || nodes.find((n) => n.isCurrent)?.id || nodes[0].id;

  return (
    <div className="space-y-4">
      <TopologyMap nodes={nodes} selectedId={active} onSelect={setSelectedNode} />

      {legend && (
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-carbon-text-3">{legend}</div>
      )}

      {/* The map shows the cluster; this shows what each node is holding, which is what a
          shard-oriented page came here for. */}
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
        {nodes.map((node) => {
          const shards = owned.get(node.id) ?? [];
          const note = caption?.(node.id, shards) ?? null;
          const style = STATUS_STYLES[node.status];
          return (
            <div
              key={node.id}
              className={`border bg-carbon-layer ${node.id === active ? "border-carbon-blue" : "border-carbon-border"}`}
            >
              <button
                type="button"
                onClick={() => setSelectedNode(node.id)}
                className="flex w-full items-center justify-between gap-2 border-b border-carbon-border bg-carbon-field px-3 py-1.5 text-left"
              >
                <span className="flex items-center gap-2 truncate">
                  <span className="h-1.5 w-1.5 rounded-full" style={{ background: style.color }} />
                  <span className="truncate font-mono text-xs font-semibold text-carbon-text">
                    {/^\d+$/.test(node.id) ? `node ${node.id}` : node.id}
                  </span>
                </span>
                <span className="flex shrink-0 items-center gap-2">
                  {node.isLeader && <Tag tone="blue">leader</Tag>}
                  <span className="text-[10px] text-carbon-text-3">{shards.length} shards</span>
                </span>
              </button>
              {note && <div className="border-b border-carbon-border px-3 py-1 text-[11px] text-carbon-text-3">{note}</div>}
              <div className="flex flex-wrap gap-1.5 p-2">
                {shards.length === 0 && <span className="px-1 text-[11px] text-carbon-text-3">no shards</span>}
                {shards.map((shard) => {
                  const mark = annotateShard?.(shard) ?? null;
                  const selected = selectedShard != null && selectedShard === shard;
                  return (
                    <button
                      key={shard}
                      type="button"
                      title={mark?.title}
                      onClick={onSelectShard ? () => onSelectShard(shard) : undefined}
                      className={`flex min-w-[2.75rem] flex-col items-center border px-1.5 py-1 leading-tight ${CHIP[mark?.tone ?? "gray"]} ${selected ? "ring-2 ring-carbon-blue" : ""} ${onSelectShard ? "cursor-pointer hover:opacity-80" : "cursor-default"}`}
                    >
                      <span className="font-mono text-xs font-semibold">{shard}</span>
                      {mark?.label && <span className="text-[10px] opacity-80">{mark.label}</span>}
                    </button>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

export { LegendDot };
