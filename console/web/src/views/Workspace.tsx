import { NavLink, Route, Routes, useNavigate } from "react-router-dom";
import SqlEditor from "./SqlEditor";
import Schema from "./Schema";
import Cluster from "./Cluster";
import Shards from "./Shards";
import Stats from "./Stats";
import MeshUsers from "./MeshUsers";

const TABS = [
  ["query", "SQL editor"],
  ["schema", "Schema"],
  ["cluster", "Cluster"],
  ["shards", "Shards & frames"],
  ["stats", "Stats"],
  ["users", "meshdb users"],
] as const;

export default function Workspace({ name }: { name: string }) {
  const nav = useNavigate();
  const tab = "px-4 py-3 text-sm text-carbon-text-2 hover:text-carbon-text border-b-2 border-transparent";
  const active = "text-carbon-text border-carbon-blue";

  return (
    <div className="h-full flex flex-col">
      <div className="border-b border-carbon-border bg-carbon-layer">
        <div className="px-6 pt-4 flex items-center gap-2">
          <button className="text-carbon-text-3 hover:text-carbon-text text-sm" onClick={() => nav("/")}>
            ← Connections
          </button>
          <span className="text-carbon-text font-semibold">{name}</span>
        </div>
        <div className="px-4 flex">
          {TABS.map(([path, label]) => (
            <NavLink key={path} to={path} className={({ isActive }) => `${tab} ${isActive ? active : ""}`}>
              {label}
            </NavLink>
          ))}
        </div>
      </div>
      <div className="flex-1 overflow-auto">
        <Routes>
          <Route path="query" element={<SqlEditor name={name} />} />
          <Route path="schema" element={<Schema name={name} />} />
          <Route path="cluster" element={<Cluster name={name} />} />
          <Route path="shards" element={<Shards name={name} />} />
          <Route path="stats" element={<Stats name={name} />} />
          <Route path="users" element={<MeshUsers name={name} />} />
          <Route path="*" element={<SqlEditor name={name} />} />
        </Routes>
      </div>
    </div>
  );
}
