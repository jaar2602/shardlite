import { NavLink, Route, Routes, useParams } from "react-router-dom";
import { useAuth } from "./auth";
import { Spinner } from "./components/ui";
import Login from "./views/Login";
import Connections from "./views/Connections";
import ConsoleUsers from "./views/ConsoleUsers";
import Workspace from "./views/Workspace";

function SideNav() {
  const { me, signOut } = useAuth();
  const link = "block px-4 py-2 text-sm text-carbon-text-2 hover:bg-carbon-layer2";
  const active = "bg-carbon-layer2 text-carbon-text border-l-2 border-carbon-blue";
  return (
    <nav className="w-56 shrink-0 bg-carbon-layer border-r border-carbon-border flex flex-col">
      <div className="px-4 py-4 border-b border-carbon-border">
        <div className="text-carbon-text font-semibold">meshdb</div>
        <div className="text-carbon-text-3 text-xs">console</div>
      </div>
      <div className="py-2 flex-1">
        <NavLink to="/" end className={({ isActive }) => `${link} ${isActive ? active : ""}`}>
          Connections
        </NavLink>
        {me?.role === "admin" && (
          <NavLink to="/console-users" className={({ isActive }) => `${link} ${isActive ? active : ""}`}>
            Console users
          </NavLink>
        )}
      </div>
      <div className="px-4 py-3 border-t border-carbon-border text-xs text-carbon-text-3">
        <div className="mb-2">
          {me?.user} · {me?.role}
        </div>
        <button className="text-carbon-blue hover:underline" onClick={() => void signOut()}>
          Sign out
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
      <SideNav />
      <main className="flex-1 overflow-auto">
        <Routes>
          <Route path="/" element={<Connections />} />
          <Route path="/console-users" element={<ConsoleUsers />} />
          <Route path="/c/:name/*" element={<WorkspaceRoute />} />
        </Routes>
      </main>
    </div>
  );
}
