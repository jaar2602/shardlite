import * as api from "./api";
import type { TopologyNode } from "../components/TopologyMap";

// Building the node map lives here rather than in a view because every shard-oriented page needs
// the *same* answer to "which nodes are there, who leads, who is up". When each page derived it
// separately they disagreed — most visibly on status, where a peer's second-hand "unknown" would
// show a node as down on one page and up on another.

export function asId(value: string | number | null | undefined): string | null {
  return value === null || value === undefined ? null : String(value);
}

export interface TopologyInput {
  cluster: api.ClusterInfo;
  observation: api.ClusterObservation;
  shardCount: number;
  connectionName?: string;
  connectionUrl?: string | null;
}

/// The cluster as a list of nodes, with who leads, who is reachable, and each one's share of the
/// shards.
///
/// A node's own report of its peers can say "unknown" when they are perfectly healthy — it only
/// knows what it last heard. The console polls every seed directly, so a node whose own poll just
/// succeeded is provably up, and that first-hand evidence wins over a peer's second-hand status.
export function topologyNodes(input: TopologyInput): TopologyNode[] {
  const { cluster, observation, shardCount, connectionName, connectionUrl } = input;

  const reachedIds = new Set(
    observation.nodes
      .filter((node) => !node.error && node.topology)
      .map((node) => asId(node.topology?.node))
      .filter((id): id is string => id !== null),
  );
  const currentId = asId(cluster.node) ?? (cluster.clustered ? "this node" : connectionName ?? "local");
  const leaderId = asId(cluster.leader);
  const assignments = { ...(cluster.placement?.assignments ?? {}) };
  // A standalone node reports no placement: it owns everything, so say so rather than drawing a
  // node with a 0% share.
  if (!cluster.clustered && Object.keys(assignments).length === 0) {
    for (let shard = 0; shard < shardCount; shard += 1) assignments[String(shard)] = currentId;
  }

  const ids = new Set<string>();
  ids.add(currentId);
  if (leaderId) ids.add(leaderId);
  for (const owner of Object.values(assignments)) ids.add(String(owner));
  for (const member of cluster.members ?? []) ids.add(String(member.node));

  const explicit = new Map((cluster.members ?? []).map((member) => [String(member.node), member]));
  const totalAssignments = Object.keys(assignments).length;

  return Array.from(ids)
    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))
    .map((id) => {
      const member = explicit.get(id);
      const isCurrent = member?.this_node ?? id === currentId;
      const isLeader = id === leaderId || (!cluster.clustered && isCurrent);
      return {
        id,
        address: member?.address ?? (isCurrent ? connectionUrl : null),
        status: reachedIds.has(id) ? "up" : member?.status ?? (isCurrent ? "up" : "unknown"),
        role: isLeader ? "leader" : isCurrent ? cluster.role ?? "member" : "member",
        isLeader,
        isCurrent,
        dataShare:
          totalAssignments === 0
            ? 0
            : Math.round(
                (Object.entries(assignments).filter(([, owner]) => String(owner) === id).length /
                  totalAssignments) *
                  100,
              ),
      } satisfies TopologyNode;
    });
}

/// Which shards each node owns, from the cluster's placement.
export function shardsByNode(cluster: api.ClusterInfo, shardCount: number, fallbackNode: string): Map<string, number[]> {
  const assignments = cluster.placement?.assignments ?? {};
  const byNode = new Map<string, number[]>();
  if (Object.keys(assignments).length === 0) {
    byNode.set(fallbackNode, Array.from({ length: shardCount }, (_, i) => i));
    return byNode;
  }
  for (const [shard, owner] of Object.entries(assignments)) {
    const key = String(owner);
    const list = byNode.get(key) ?? [];
    list.push(Number(shard));
    byNode.set(key, list);
  }
  for (const list of byNode.values()) list.sort((a, b) => a - b);
  return byNode;
}
