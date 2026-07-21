import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "../auth";
import * as api from "../lib/api";
import { Banner, Button, Card, DataTable, Spinner, Tag, TextInput } from "../components/ui";

export default function Connections() {
  const { me } = useAuth();
  const nav = useNavigate();
  const isAdmin = me?.role === "admin";
  const [list, setList] = useState<api.Connection[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);
  const [form, setForm] = useState({ name: "", url: "", meshdb_user: "", meshdb_secret: "" });

  const load = async () => {
    try {
      setList(await api.connections.list());
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to load");
    }
  };
  useEffect(() => {
    void load();
  }, []);

  const add = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    try {
      await api.connections.create({
        name: form.name,
        url: form.url,
        meshdb_user: form.meshdb_user || undefined,
        meshdb_secret: form.meshdb_secret || undefined,
      });
      setForm({ name: "", url: "", meshdb_user: "", meshdb_secret: "" });
      setAdding(false);
      void load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to add");
    }
  };

  const remove = async (name: string) => {
    if (!confirm(`Remove connection "${name}"?`)) return;
    try {
      await api.connections.remove(name);
      void load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to remove");
    }
  };

  return (
    <div className="p-8 max-w-4xl">
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl text-carbon-text">Connections</h1>
        {isAdmin && (
          <Button onClick={() => setAdding((v) => !v)}>{adding ? "Cancel" : "Add connection"}</Button>
        )}
      </div>

      {error && <div className="mb-4"><Banner tone="error">{error}</Banner></div>}

      {adding && (
        <Card title="New connection" className="mb-6">
          <form onSubmit={add} className="grid grid-cols-2 gap-4">
            <TextInput
              label="Name"
              value={form.name}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
              placeholder="prod-east"
              required
            />
            <TextInput
              label="Cluster URL (/v1 edge)"
              value={form.url}
              onChange={(e) => setForm({ ...form, url: e.target.value })}
              placeholder="http://10.0.0.5:4680"
              required
            />
            <TextInput
              label="meshdb user (optional)"
              value={form.meshdb_user}
              onChange={(e) => setForm({ ...form, meshdb_user: e.target.value })}
              placeholder="app"
            />
            <TextInput
              label="meshdb secret (optional, stored encrypted)"
              type="password"
              value={form.meshdb_secret}
              onChange={(e) => setForm({ ...form, meshdb_secret: e.target.value })}
            />
            <div className="col-span-2">
              <Button type="submit">Save connection</Button>
            </div>
          </form>
        </Card>
      )}

      {list === null ? (
        <Spinner label="Loading connections…" />
      ) : (
        <DataTable
          columns={["Name", "URL", "meshdb user", ""]}
          empty="No connections yet. Add one to get started."
          rows={list.map((c) => [
            <button className="text-carbon-blue hover:underline" onClick={() => nav(`/c/${encodeURIComponent(c.name)}/query`)}>
              {c.name}
            </button>,
            c.url,
            c.meshdb_user ? <Tag tone="blue">{c.meshdb_user}</Tag> : <Tag>no auth</Tag>,
            isAdmin ? (
              <button className="text-carbon-red hover:underline" onClick={() => void remove(c.name)}>
                Remove
              </button>
            ) : (
              ""
            ),
          ])}
        />
      )}
    </div>
  );
}
