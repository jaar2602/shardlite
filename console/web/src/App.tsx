import { useState } from "react";
import { matchPath, NavLink, Route, Routes, useLocation, useParams } from "react-router-dom";
import { useAuth } from "./auth";
import { Spinner } from "./components/ui";
import * as api from "./lib/api";
import Login from "./views/Login";
import Connections from "./views/Connections";
import ConsoleUsers from "./views/ConsoleUsers";
import Activity from "./views/Activity";
import Fleet from "./views/Fleet";
import Workspace from "./views/Workspace";

const WORKSPACE_LINKS: { path: string; label: string; short: string; permission: api.Permission }[] = [
  { path: "overview", label: "Overview", short: "⌂", permission: "observe" },
  { path: "cluster", label: "Topology", short: "◇", permission: "observe" },
  { path: "query", label: "SQL editor", short: ">_", permission: "query" },
  { path: "schema", label: "Schema", short: "▤", permission: "observe" },
  { path: "operations", label: "Operations", short: "↻", permission: "write" },
  { path: "storage-internals", label: "Storage internals", short: "≋", permission: "operate" },
  { path: "stats", label: "Stats", short: "⌁", permission: "observe" },
  { path: "users", label: "meshdb users", short: "◎", permission: "admin" },
];

function SideNav({ collapsed, onToggle }: { collapsed: boolean; onToggle: () => void }) {
  const { me, signOut } = useAuth();
  const location = useLocation();
  const workspace = matchPath("/c/:name/*", location.pathname);
  const connectionName = workspace?.params.name;
  const link = `flex items-center gap-3 border-l-2 border-transparent py-2 text-sm text-carbon-text-2 hover:bg-carbon-layer2 hover:text-carbon-text ${collapsed ? "justify-center px-2" : "px-4"}`;
  const active = "border-carbon-blue bg-carbon-layer2 text-carbon-text";
  const label = (short: string, text: string) => <><span aria-hidden="true" className="w-5 shrink-0 text-center font-mono text-xs">{short}</span>{!collapsed && <span className="truncate">{text}</span>}</>;
  return (
    <nav aria-label="Console navigation" className={`flex shrink-0 flex-col border-r border-carbon-border bg-carbon-layer transition-[width] duration-150 ${collapsed ? "w-14" : "w-56"}`}>
      <div className={`flex h-16 shrink-0 items-center border-b border-carbon-border ${collapsed ? "justify-center" : "px-4"}`}>
        {!collapsed && <div className="min-w-0 flex-1"><div className="font-semibold text-carbon-text">meshdb</div><div className="text-xs text-carbon-text-3">console</div></div>}
        <button
          aria-label={collapsed ? "Expand navigation" : "Collapse navigation"}
          aria-expanded={!collapsed}
          className="grid h-9 w-9 shrink-0 place-items-center text-carbon-text-3 hover:bg-carbon-layer2 hover:text-carbon-text"
          title={collapsed ? "Expand navigation" : "Collapse navigation"}
          onClick={onToggle}
        >
          {collapsed ? "›" : "‹"}
        </button>
      </div>
      <div className="min-h-0 flex-1 overflow-y-auto py-2">
        <NavLink to="/" end title="Fleet" className={({ isActive }) => `${link} ${isActive ? active : ""}`}>
          {label("◆", "Fleet")}
        </NavLink>
        <NavLink to="/connections" title="Connections" className={({ isActive }) => `${link} ${isActive ? active : ""}`}>
          {label("↗", "Connections")}
        </NavLink>
        {me?.role === "admin" && (
          <>
            <NavLink to="/activity" title="Activity" className={({ isActive }) => `${link} ${isActive ? active : ""}`}>
              {label("≡", "Activity")}
            </NavLink>
            <NavLink to="/console-users" title="Console users" className={({ isActive }) => `${link} ${isActive ? active : ""}`}>
              {label("●", "Console users")}
            </NavLink>
          </>
        )}

        {connectionName && <div className="mt-3 border-t border-carbon-border pt-3">
          {collapsed
            ? <div className="mx-auto mb-2 grid h-8 w-8 place-items-center bg-carbon-layer2 font-mono text-xs text-carbon-blue" title={connectionName}>{connectionName.slice(0, 1).toUpperCase()}</div>
            : <div className="mb-2 px-4"><div className="text-[10px] font-semibold uppercase tracking-wider text-carbon-text-3">Database</div><div className="truncate text-sm font-semibold text-carbon-text" title={connectionName}>{connectionName}</div></div>}
          {WORKSPACE_LINKS.filter((item) => api.permits(me?.role, item.permission)).map((item) => <NavLink
            key={item.path}
            to={`/c/${encodeURIComponent(connectionName)}/${item.path}`}
            title={item.label}
            className={({ isActive }) => `${link} ${isActive ? active : ""}`}
          >
            {label(item.short, item.label)}
          </NavLink>)}
        </div>}
      </div>
      <div className={`shrink-0 border-t border-carbon-border py-3 text-xs text-carbon-text-3 ${collapsed ? "px-2 text-center" : "px-4"}`}>
        {!collapsed && <div className="mb-2 truncate" title={`${me?.user} · ${me?.role}`}>{me?.user} · {me?.role}</div>}
        <button className="text-carbon-blue hover:underline" title="Sign out" aria-label="Sign out" onClick={() => void signOut()}>
          {collapsed ? "⇥" : "Sign out"}
        </button>
      </div>
    </nav>
  );
}

function WorkspaceRoute() {
  const { name } = useParams();
  return <Workspace name={name!} />;
}

export default function App() {
  const { me, loading } = useAuth();
  const [navigationCollapsed, setNavigationCollapsed] = useState(() => {
    try { return localStorage.getItem("meshdb.console.navigation.collapsed") === "true"; } catch { return false; }
  });

  if (loading) {
    return (
      <div className="h-full grid place-items-center">
        <Spinner label="Loading console…" />
      </div>
    );
  }
  if (!me) return <Login />;

  return (
    <div className="h-full flex">
      <SideNav collapsed={navigationCollapsed} onToggle={() => setNavigationCollapsed((current) => {
        const next = !current;
        try { localStorage.setItem("meshdb.console.navigation.collapsed", String(next)); } catch { /* browser storage can be unavailable */ }
        return next;
      })} />
      <main className="min-w-0 flex-1 overflow-auto">
        <Routes>
          <Route path="/" element={<Fleet />} />
          <Route path="/connections" element={<Connections />} />
          {me.role === "admin" && (
            <>
              <Route path="/console-users" element={<ConsoleUsers />} />
              <Route path="/activity" element={<Activity />} />
            </>
          )}
          <Route path="/c/:name/*" element={<WorkspaceRoute />} />
        </Routes>
      </main>
    </div>
  );
}
