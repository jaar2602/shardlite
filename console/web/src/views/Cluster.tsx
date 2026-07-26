import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useAuth } from "../auth";
import * as api from "../lib/api";
import TopologyMap, { STATUS_STYLES, TopologyNode } from "../components/TopologyMap";
import { Banner, Button, Spinner, Tag } from "../components/ui";

interface Snapshot {
  info: api.NodeInfo;
  cluster: api.ClusterInfo;
  stats: api.NodeStats;
  connection?: api.Connection;
  sampledAt: number;
  observation: api.ClusterObservation;
  catalog: api.ClusterCatalog | null;
}

function asId(value: string | number | null | undefined): string | null {
  return value === null || value === undefined ? null : String(value);
}

function topologyNodes(snapshot: Snapshot): TopologyNode[] {
  const { cluster, connection, info, observation } = snapshot;
  // The topology below is one node's view of its peers, which can report a peer as "unknown" even
  // when it is fine. But the console polls every seed directly, so a node whose own poll just
  // succeeded is provably up — trust that over a peer's second-hand status.
  const reachedIds = new Set(
    observation.nodes
      .filter((node) => !node.error && node.topology)
      .map((node) => asId(node.topology?.node))
      .filter((id): id is string => id !== null),
  );
  const currentId = asId(cluster.node) ?? (cluster.clustered ? "this node" : connection?.name ?? "local");
  const leaderId = asId(cluster.leader);
  const assignments = { ...(cluster.placement?.assignments ?? {}) };
  if (!cluster.clustered && Object.keys(assignments).length === 0) {
    for (let shard = 0; shard < info.shard_count; shard += 1) assignments[String(shard)] = currentId;
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
        address: member?.address ?? (isCurrent ? connection?.url : null),
        status: reachedIds.has(id) ? "up" : member?.status ?? (isCurrent ? "up" : "unknown"),
        role: isLeader ? "leader" : isCurrent ? cluster.role ?? "member" : "member",
        isLeader,
        isCurrent,
        dataShare: totalAssignments === 0 ? 0 : Math.round(Object.entries(assignments)
          .filter(([, owner]) => String(owner) === id)
          .length / totalAssignments * 100),
      } satisfies TopologyNode;
    });
}

function StatusPill({ status }: { status: api.MemberStatus }) {
  const style = STATUS_STYLES[status];
  return (
    <span
      className="inline-flex items-center gap-1.5 px-2 py-1 font-mono text-[10px] font-semibold tracking-wide"
      style={{ color: style.text, background: style.background }}
    >
      <span className="h-1.5 w-1.5 rounded-full" style={{ background: style.color }} />
      {style.label}
    </span>
  );
}

function Fact({ label, value, mono = false }: { label: string; value: React.ReactNode; mono?: boolean }) {
  return (
    <div className="min-w-0">
      <div className="mb-1 text-[10px] uppercase tracking-[0.08em] text-carbon-text-3">{label}</div>
      <div className={`truncate text-sm text-carbon-text ${mono ? "font-mono font-semibold" : ""}`}>{value}</div>
    </div>
  );
}

function DetailRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-start justify-between gap-5 py-1.5 text-xs">
      <span className="text-carbon-text-3">{label}</span>
      <span className="max-w-[62%] break-all text-right font-mono text-carbon-text">{value}</span>
    </div>
  );
}

function formatTime(timestamp: number): string {
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(timestamp);
}

function valueOrDash(value: unknown): string {
  return value === null || value === undefined ? "—" : String(value);
}

export default function Cluster({ name }: { name: string }) {
  const { me } = useAuth();
  const canOperate = api.permits(me?.role, "operate");
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [draining, setDraining] = useState(false);
  const [drainMessage, setDrainMessage] = useState<string | null>(null);
  const [catalogBusy, setCatalogBusy] = useState(false);
  const [catalogMessage, setCatalogMessage] = useState<string | null>(null);
  const sequence = useRef(0);
  const inFlight = useRef<{ id: number; name: string } | null>(null);

  const load = useCallback(async () => {
    if (inFlight.current?.name === name) return;
    const requestId = sequence.current + 1;
    sequence.current = requestId;
    inFlight.current = { id: requestId, name };
    setRefreshing(true);
    try {
      const c = api.conn(name);
      const [observation, connectionList, catalog] = await Promise.all([
        c.observation(),
        api.connections.list(),
        c.catalog().catch(() => null),
      ]);
      const candidates = observation.nodes
        .filter((node) => node.meta && node.topology)
        .sort((left, right) => (right.last_success_ms ?? 0) - (left.last_success_ms ?? 0));
      const source = candidates.find((node) => node.seed === observation.preferred_seed) ?? candidates[0];
      if (!source?.meta || !source.topology) throw new Error("no successful topology observation yet");
      if (sequence.current !== requestId) return;
      setSnapshot({
        info: source.meta,
        cluster: source.topology,
        stats: source.stats ?? {},
        connection: connectionList.find((item) => item.name === name),
        sampledAt: source.last_success_ms ?? Date.now(),
        observation,
        catalog,
      });
      setError(null);
    } catch (e) {
      if (sequence.current !== requestId) return;
      setError(e instanceof Error ? e.message : "failed to load topology");
    } finally {
      if (inFlight.current?.id === requestId) inFlight.current = null;
      if (sequence.current === requestId) setRefreshing(false);
    }
  }, [name]);

  useEffect(() => {
    setSnapshot(null);
    setSelectedId(null);
    setError(null);
    void load();
    const timer = window.setInterval(() => void load(), 5000);
    return () => {
      window.clearInterval(timer);
      sequence.current += 1;
      if (inFlight.current?.name === name) inFlight.current = null;
    };
  }, [load]);

  const drain = async () => {
    if (!confirm("Drain this node? It is removed from the cluster until restart and its shards move to survivors.")) return;
    setDraining(true);
    setDrainMessage(null);
    try {
      const result = await api.conn(name).drain();
      setDrainMessage(`Drain requested${result.was_leader ? " (was leader; handover initiated)" : ""}.`);
      void load();
    } catch (e) {
      setDrainMessage(e instanceof Error ? e.message : "drain failed");
    } finally {
      setDraining(false);
    }
  };

  const catalogAction = async (
    confirmation: string,
    action: (client: ReturnType<typeof api.conn>) => Promise<api.CatalogMutation>,
  ) => {
    if (!confirm(confirmation)) return;
    setCatalogBusy(true);
    setCatalogMessage(null);
    try {
      const result = await action(api.conn(name));
      setCatalogMessage(
        result.operation
          ? `Operation ${result.operation} accepted. Progress is durable and resumes after restart.`
          : "Catalog change committed.",
      );
      await load();
    } catch (e) {
      setCatalogMessage(e instanceof Error ? e.message : "catalog operation failed");
    } finally {
      setCatalogBusy(false);
    }
  };

  const nodes = useMemo(() => (snapshot ? topologyNodes(snapshot) : []), [snapshot]);
  const activeId =
    selectedId && nodes.some((node) => node.id === selectedId)
      ? selectedId
      : nodes.find((node) => node.isLeader)?.id ?? nodes.find((node) => node.isCurrent)?.id ?? nodes[0]?.id ?? "";
  const selected = nodes.find((node) => node.id === activeId);

  if (!snapshot && !error) {
    return (
      <div className="p-6">
        <Spinner label="Loading cluster topology…" />
      </div>
    );
  }
  if (!snapshot) {
    return (
      <div className="p-6 space-y-4">
        <Banner tone="error">{error}</Banner>
        <Button variant="secondary" onClick={() => void load()} disabled={refreshing}>
          Retry
        </Button>
      </div>
    );
  }

  const { info, cluster, stats, connection, sampledAt, observation, catalog } = snapshot;
  const placementTerm = cluster.placement?.term;
  const primaryTotal = Object.keys(cluster.placement?.assignments ?? {}).length;
  const currentNode = nodes.find((node) => node.isCurrent);
  const legacyTopology = cluster.clustered && !cluster.members;
  const observerStats = cluster.stats ?? {};

  return (
    <div className="min-h-full p-4 lg:p-5">
      <header className="mb-4 flex flex-wrap items-start justify-between gap-3 border-b border-carbon-border pb-3">
        <div>
          <div className="mb-3 text-xs uppercase tracking-[0.14em] text-carbon-text-3">
            shardlite console · database topology
          </div>
          <h1 className="text-2xl font-semibold tracking-tight text-carbon-text">One database, every node</h1>
          <p className="mt-2 max-w-4xl text-sm text-carbon-text-3">
            A live map of the nodes serving this ShardLite database. Select a node to inspect its health,
            role, workload, and share of data responsibility.
          </p>
        </div>
        <div className="flex items-center gap-3">
          <span className="font-mono text-[10px] uppercase tracking-wide text-carbon-text-3">
            sampled {formatTime(sampledAt)}
          </span>
          <Button variant="ghost" onClick={() => void load()} disabled={refreshing}>
            {refreshing ? "Refreshing…" : "Refresh"}
          </Button>
          {canOperate && catalog?.enabled && (
            <Button
              variant="secondary"
              disabled={catalogBusy}
              onClick={() =>
                void catalogAction(
                  "Plan one stable rebalance movement now?",
                  (client) => client.rebalance(),
                )
              }
            >
              Rebalance one shard
            </Button>
          )}
        </div>
      </header>

      {error && (
        <div className="mb-5">
          <Banner tone="error">Refresh failed; showing the last successful observation. {error}</Banner>
        </div>
      )}
      {observation.issues.length > 0 && (
        <div className="mb-5">
          <Banner tone="error">{observation.issues.join(" · ")}</Banner>
        </div>
      )}
      {legacyTopology && (
        <div className="mb-5">
          <Banner tone="info">
            This ShardLite version does not report member liveness yet. Nodes discovered from data placement are
            shown as unknown rather than assumed healthy.
          </Banner>
        </div>
      )}

      <div className="grid items-start gap-4 2xl:grid-cols-[minmax(440px,0.92fr)_minmax(680px,1.35fr)]">
        <div className="space-y-4">
          <section className="border border-carbon-border bg-carbon-layer">
            <div className="flex flex-wrap items-center gap-2 border-b border-carbon-border px-4 py-4">
              <h2 className="mr-1 text-base font-semibold text-carbon-text">Membership</h2>
              <Tag tone="blue">{nodes.length} {nodes.length === 1 ? "node" : "nodes"}</Tag>
              <Tag tone={cluster.clustered ? "green" : "gray"}>{cluster.clustered ? "clustered" : "standalone"}</Tag>
            </div>

            <div className="grid grid-cols-2 gap-x-6 gap-y-5 px-4 py-5 sm:grid-cols-4">
              <Fact label="This node" value={currentNode?.id ?? "—"} mono />
              <Fact label="Term" value={valueOrDash(cluster.term ?? placementTerm)} mono />
              <Fact
                label="Forwarding"
                value={info.forwarding === undefined ? "not reported" : info.forwarding ? "enabled" : "disabled"}
              />
              <Fact label="Database health" value={observation.status} />
            </div>

            <div className="px-4 pb-2 pt-1 text-[10px] uppercase tracking-[0.08em] text-carbon-text-3">
              Consensus
            </div>
            <div className="grid grid-cols-2 gap-x-6 gap-y-5 px-4 pb-5 sm:grid-cols-4">
              <Fact label="Leader" value={asId(cluster.leader) ?? (cluster.clustered ? "unknown" : currentNode?.id ?? "—")} mono />
              <Fact label="Voters" value={valueOrDash(cluster.voters ?? nodes.length)} mono />
              <Fact label="Distribution term" value={valueOrDash(placementTerm)} mono />
              <Fact label="This role" value={cluster.role ?? (cluster.clustered ? "unknown" : "standalone")} />
            </div>

            <div className="px-4 pb-2 pt-1 text-[10px] uppercase tracking-[0.08em] text-carbon-text-3">
              Members
            </div>
            <div className="overflow-x-auto px-2 pb-3">
              <table className="w-full border-collapse text-left text-xs">
                <thead className="text-[10px] uppercase tracking-[0.06em] text-carbon-text-3">
                  <tr>
                    <th className="px-2 py-2 font-normal">Node</th>
                    <th className="px-2 py-2 font-normal">Address</th>
                    <th className="px-2 py-2 font-normal">Status</th>
                    <th className="px-2 py-2 font-normal">Data share</th>
                  </tr>
                </thead>
                <tbody>
                  {nodes.map((node) => (
                    <tr
                      key={node.id}
                      className={`cursor-pointer border-t border-carbon-border hover:bg-carbon-layer2/50 ${
                        node.id === activeId ? "bg-carbon-layer2/40" : ""
                      }`}
                      onClick={() => setSelectedId(node.id)}
                    >
                      <td className="whitespace-nowrap px-2 py-3 font-mono font-semibold text-carbon-text">
                        <span className="inline-flex items-center gap-2">
                          {node.id}
                          {node.isLeader && <span className="text-carbon-blue" title="Leader">♛</span>}
                        </span>
                      </td>
                      <td className="max-w-48 truncate px-2 py-3 font-mono text-carbon-text-2" title={node.address ?? "Not advertised"}>
                        {node.address ?? "not advertised"}
                      </td>
                      <td className="whitespace-nowrap px-2 py-3"><StatusPill status={node.status} /></td>
                      <td className="whitespace-nowrap px-2 py-3">
                        <span className="font-mono text-carbon-text">{node.dataShare}%</span>
                        <span className="text-carbon-text-3"> responsibility</span>
                        {node.isCurrent && <span className="ml-2 bg-carbon-blue/20 px-1.5 py-1 font-mono text-[9px] text-carbon-blue">THIS NODE</span>}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          {catalog?.enabled && (
            <section className="border border-carbon-border bg-carbon-layer">
              <div className="border-b border-carbon-border px-4 py-4">
                <h2 className="text-base font-semibold text-carbon-text">Dynamic scaling</h2>
                <p className="mt-1 text-xs text-carbon-text-3">
                  Catalog v{catalog.version ?? "—"} · routing epoch {catalog.routing_epoch ?? "—"} ·{" "}
                  {catalog.active_shards ?? "—"} active / {catalog.local_shard_capacity ?? "—"} local capacity
                </p>
              </div>
              <div className="space-y-3 px-4 py-4">
                {(catalog.operations ?? []).filter((operation) => !["complete", "aborted"].includes(operation.phase)).map((operation) => (
                  <div key={operation.id} className="border border-carbon-border bg-carbon-bg px-3 py-2 text-xs">
                    <span className="font-mono text-carbon-blue">#{operation.id}</span>
                    <span className="ml-2 text-carbon-text">{operation.kind} · {operation.phase}</span>
                    {operation.shard !== null && operation.shard !== undefined && (
                      <span className="ml-2 text-carbon-text-3">shard {operation.shard}</span>
                    )}
                  </div>
                ))}
                {(catalog.operations ?? []).every((operation) => ["complete", "aborted"].includes(operation.phase)) && (
                  <p className="text-xs text-carbon-text-3">No topology operation is currently active.</p>
                )}
                {catalog.voter_transition && (
                  <p className="border border-carbon-yellow/40 bg-carbon-yellow/10 px-3 py-2 text-xs text-carbon-text">
                    Joint consensus: [{catalog.voter_transition.old.join(", ")}] → [{catalog.voter_transition.new.join(", ")}]
                  </p>
                )}
                {canOperate && (
                  <div className="flex flex-wrap gap-2 pt-2">
                    <Button
                      variant="secondary"
                      disabled={catalogBusy}
                      onClick={() => {
                        const value = prompt("New voter node IDs, comma separated");
                        if (!value) return;
                        const voters = value.split(",").map((item) => Number(item.trim())).filter(Number.isFinite);
                        void catalogAction(
                          `Enter joint consensus with voters ${voters.join(", ")}?`,
                          (client) => client.changeVoters(voters),
                        );
                      }}
                    >
                      Change voters
                    </Button>
                    <Button
                      variant="secondary"
                      disabled={catalogBusy}
                      onClick={() =>
                        void catalogAction(
                          "Finalize the current joint voter configuration?",
                          (client) => client.finalizeVoters(),
                        )
                      }
                    >
                      Finalize voters
                    </Button>
                  </div>
                )}
                {catalogMessage && <Banner tone="info">{catalogMessage}</Banner>}
              </div>
            </section>
          )}

          <section className="border border-carbon-border bg-carbon-layer">
            <div className="border-b border-carbon-border px-4 py-4">
              <h2 className="text-base font-semibold text-carbon-text">Effective configuration</h2>
            </div>
            <div className="grid grid-cols-2 gap-x-6 gap-y-5 px-4 py-5 sm:grid-cols-3">
              <Fact label="Version" value={info.version ?? "not reported"} mono />
              <Fact label="Mode" value={cluster.clustered ? "clustered" : "standalone"} />
              <Fact label="Database endpoint" value={observation.preferred_seed ?? connection?.url ?? "not available"} mono />
              <Fact label="Read consistency" value="linearizable · stale" />
              <Fact label="Data distribution" value={primaryTotal || !cluster.clustered ? "reported" : "not reported"} />
              <Fact label="Topology polling" value={`${observation.seeds} endpoint${observation.seeds === 1 ? "" : "s"} · bounded`} />
            </div>
          </section>
        </div>

        <section className="border border-carbon-border bg-carbon-layer">
          <div className="flex flex-wrap items-baseline gap-2 border-b border-carbon-border px-4 py-4">
            <h2 className="text-base font-semibold text-carbon-text">Infrastructure</h2>
            <span className="text-[10px] uppercase tracking-[0.09em] text-carbon-text-3">nodes &amp; logical database</span>
          </div>

          <div className="grid min-h-[490px] xl:grid-cols-[minmax(0,1fr)_270px]">
            <div className="flex min-w-0 flex-col justify-between p-3">
              <TopologyMap nodes={nodes} selectedId={activeId} onSelect={setSelectedId} />
              <div className="mx-1 mb-1 flex flex-wrap gap-x-4 gap-y-2 bg-carbon-bg/70 px-3 py-2 text-[10px] text-carbon-text-3">
                <Legend color="#4589ff" label="data responsibility" />
                <Legend color="#8a3ffc" label="node role" />
                <Legend color="#42be65" label="up" />
                <Legend color="#f1c21b" label="suspected" />
                <Legend color="#fa4d56" label="down" />
                <Legend color="#8d8d8d" label="unknown" />
              </div>
            </div>

            <aside className="border-t border-carbon-border bg-[#242424] p-4 xl:border-l xl:border-t-0">
              {selected ? (
                <>
                  <div className="mb-3 flex flex-wrap items-center gap-2">
                    <h3 className="font-mono text-base font-semibold text-carbon-text">{selected.id}</h3>
                    {selected.isLeader && <Tag tone="blue">LEADER</Tag>}
                    {selected.isCurrent && <Tag>THIS NODE</Tag>}
                  </div>
                  <DetailRow label="status" value={<StatusPill status={selected.status} />} />
                  <DetailRow label="role" value={selected.role} />
                  <DetailRow label="term" value={valueOrDash(cluster.term)} />
                  <DetailRow label="address" value={selected.address ?? "not advertised"} />

                  <div className="my-3 border-t border-carbon-border" />
                  <div className="mb-1 flex items-center gap-2 text-[10px] uppercase tracking-[0.08em] text-carbon-text-3">
                    <span className="h-2.5 w-2.5 bg-[#4589ff]" /> Data distribution
                  </div>
                  <DetailRow label="data responsibility" value={`${selected.dataShare}%`} />
                  <DetailRow label="replication" value="managed by ShardLite" />

                  <div className="my-3 border-t border-carbon-border" />
                  <div className="mb-1 text-[10px] uppercase tracking-[0.08em] text-carbon-text-3">Node workload</div>
                  {selected.isCurrent ? (
                    <>
                      <DetailRow label="writer opens" value={valueOrDash(stats.writer?.open_now)} />
                      <DetailRow label="writer threads" value={valueOrDash(stats.writer?.threads)} />
                      <DetailRow label="reader threads" value={valueOrDash(stats.reader?.threads)} />
                      <DetailRow label="queries" value={valueOrDash(stats.reader?.queries)} />
                      <DetailRow label="HTTP requests" value={valueOrDash(stats.http?.requests)} />
                    </>
                  ) : (
                    <p className="py-2 text-xs leading-5 text-carbon-text-3">
                      Per-node workload is unavailable because this connection currently samples only the reporting node.
                    </p>
                  )}

                  {selected.isCurrent && cluster.clustered && (
                    <>
                      <div className="my-3 border-t border-carbon-border" />
                      <div className="mb-1 text-[10px] uppercase tracking-[0.08em] text-carbon-text-3">Consensus counters</div>
                      <DetailRow label="elections" value={valueOrDash(observerStats.elections_started)} />
                      <DetailRow label="step-downs" value={valueOrDash(observerStats.stepped_down)} />
                      <DetailRow label="unreachable peers" value={valueOrDash(observerStats.peer_unreachable)} />
                      <DetailRow label="handover failures" value={valueOrDash(observerStats.handover_failed)} />
                    </>
                  )}

                  {canOperate && selected.isCurrent && !catalog?.enabled && (
                    <>
                      <div className="my-3 border-t border-carbon-border" />
                      <div className="mb-2 text-[10px] uppercase tracking-[0.08em] text-carbon-text-3">Node maintenance</div>
                      <Button variant="danger" disabled={draining} onClick={() => void drain()}>{draining ? "Draining…" : "Drain node"}</Button>
                      {drainMessage && <p className="mt-2 text-xs leading-5 text-carbon-text-2">{drainMessage}</p>}
                    </>
                  )}
                  {canOperate && catalog?.enabled && selected && (
                    <>
                      <div className="my-3 border-t border-carbon-border" />
                      <div className="mb-2 text-[10px] uppercase tracking-[0.08em] text-carbon-text-3">Catalog membership</div>
                      <div className="flex flex-wrap gap-2">
                        <Button
                          variant="secondary"
                          disabled={catalogBusy}
                          onClick={() => {
                            const member = catalog.members?.find((item) => String(item.node) === selected.id);
                            const cordoned = member?.state !== "cordoned";
                            void catalogAction(
                              `${cordoned ? "Cordon" : "Uncordon"} node ${selected.id}?`,
                              (client) => client.cordonMember(Number(selected.id), cordoned),
                            );
                          }}
                        >
                          {catalog.members?.find((item) => String(item.node) === selected.id)?.state === "cordoned"
                            ? "Uncordon"
                            : "Cordon"}
                        </Button>
                        <Button
                          variant="danger"
                          disabled={catalogBusy}
                          onClick={() =>
                            void catalogAction(
                              `Drain node ${selected.id}? Primaries move one shard at a time.`,
                              (client) => client.drainMember(Number(selected.id)),
                            )
                          }
                        >
                          Drain
                        </Button>
                        <Button
                          variant="danger"
                          disabled={catalogBusy}
                          onClick={() =>
                            void catalogAction(
                              `Remove node ${selected.id}? This is refused until drain and voter removal are complete.`,
                              (client) => client.removeMember(Number(selected.id)),
                            )
                          }
                        >
                          Remove
                        </Button>
                      </div>
                    </>
                  )}
                </>
              ) : (
                <p className="text-sm text-carbon-text-3">No nodes were reported.</p>
              )}
            </aside>
          </div>

          <div className="border-t border-carbon-border px-4 py-4 text-xs leading-5 text-carbon-text-3">
            Live in the console: polls configured endpoints with bounded concurrency, jitter, backoff, and stale-evidence
            rules. Blue layers show each node's relative data responsibility; purple caps identify the node role. Conflicting
            observer reports remain visible in the health banner rather than being flattened.
          </div>
        </section>
      </div>
    </div>
  );
}

function Legend({ color, label }: { color: string; label: string }) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <span className="h-2.5 w-2.5 rounded-[1px]" style={{ background: color }} />
      {label}
    </span>
  );
}
