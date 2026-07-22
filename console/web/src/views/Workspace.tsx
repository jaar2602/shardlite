import { Navigate, Route, Routes } from "react-router-dom";
import { lazy, Suspense } from "react";
import { useAuth } from "../auth";
import { Spinner } from "../components/ui";
import * as api from "../lib/api";
import Cluster from "./Cluster";
import Stats from "./Stats";
import MeshUsers from "./MeshUsers";
import Overview from "./Overview";
import ShardInventory from "./ShardInventory";

const SqlEditor = lazy(() => import("./SqlEditor"));
const Schema = lazy(() => import("./Schema"));
const Operations = lazy(() => import("./Operations"));

export default function Workspace({ name }: { name: string }) {
  const { me } = useAuth();
  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="min-h-0 flex-1 overflow-auto">
        <Suspense fallback={<div className="p-6"><Spinner label="Loading workspace…" /></div>}>
        <Routes>
          <Route path="overview" element={<Overview name={name} />} />
          <Route path="shard-inventory" element={<Navigate replace to={`/c/${encodeURIComponent(name)}/${api.permits(me?.role, "operate") ? "storage-internals" : "overview"}`} />} />
          <Route path="query" element={<SqlEditor name={name} />} />
          <Route path="schema" element={<Schema name={name} />} />
          {api.permits(me?.role, "write") && <Route path="operations" element={<Operations name={name} />} />}
          <Route path="cluster" element={<Cluster name={name} />} />
          {api.permits(me?.role, "operate") && <Route path="storage-internals" element={<ShardInventory name={name} />} />}
          <Route path="frames" element={<Navigate replace to={`/c/${encodeURIComponent(name)}/${api.permits(me?.role, "operate") ? "storage-internals" : "overview"}`} />} />
          <Route path="stats" element={<Stats name={name} />} />
          {api.permits(me?.role, "admin") && <Route path="users" element={<MeshUsers name={name} />} />}
          <Route path="*" element={<Overview name={name} />} />
        </Routes>
        </Suspense>
      </div>
    </div>
  );
}
