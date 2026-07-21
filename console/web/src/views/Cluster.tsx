import { useEffect, useState } from "react";
import * as api from "../lib/api";
import { Banner, DataTable, JsonBlock, Spinner, StatCard, Tag } from "../components/ui";

export default function Cluster({ name }: { name: string }) {
  const c = api.conn(name);
  const [info, setInfo] = useState<Record<string, unknown> | null>(null);
  const [cluster, setCluster] = useState<Record<string, unknown> | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    (async () => {
      try {
        setInfo(await c.info());
        setCluster(await c.cluster());
      } catch (e) {
        setError(e instanceof Error ? e.message : "failed to load");
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [name]);

  if (error) return <div className="p-6"><Banner tone="error">{error}</Banner></div>;
  if (!info || !cluster) return <div className="p-6"><Spinner label="Loading cluster…" /></div>;

  const clustered = Boolean(cluster.clustered);
  const placement = (cluster.placement ?? cluster.shards) as Record<string, unknown> | undefined;

  return (
    <div className="p-6 space-y-6 max-w-5xl">
      <div className="grid grid-cols-3 gap-4">
        <StatCard label="Shards" value={String(info.shard_count ?? "—")} />
        <StatCard label="Epoch" value={info.epoch === null || info.epoch === undefined ? "—" : String(info.epoch)} />
        <StatCard label="Mode" value={<Tag tone={clustered ? "blue" : "gray"}>{clustered ? "clustered" : "single node"}</Tag>} />
      </div>

      {placement && typeof placement === "object" ? (
        <div>
          <h3 className="text-carbon-text text-sm font-semibold mb-2">Shard placement</h3>
          <DataTable
            columns={["Shard", "Primary node"]}
            rows={Object.entries(placement).map(([shard, node]) => [shard, String(node)])}
          />
        </div>
      ) : (
        <div>
          <h3 className="text-carbon-text text-sm font-semibold mb-2">Cluster</h3>
          <JsonBlock value={cluster} />
        </div>
      )}

      <div>
        <h3 className="text-carbon-text text-sm font-semibold mb-2">Node info</h3>
        <JsonBlock value={info} />
      </div>
    </div>
  );
}
