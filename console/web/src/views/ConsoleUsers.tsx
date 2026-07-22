import { useEffect, useState } from "react";
import * as api from "../lib/api";
import { Banner, Button, Card, DataTable, Page, PageHeader, Select, Spinner, Tag, TextInput } from "../components/ui";

export default function ConsoleUsers() {
  const [list, setList] = useState<{ name: string; role: api.Role }[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [form, setForm] = useState<{ username: string; password: string; role: api.Role }>({
    username: "",
    password: "",
    role: "viewer",
  });

  const load = async () => {
    try {
      setList(await api.consoleUsers.list());
      setError(null);
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
      await api.consoleUsers.create(form.username, form.password, form.role);
      setForm({ username: "", password: "", role: "viewer" });
      void load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to add");
    }
  };

  const remove = async (name: string) => {
    if (!confirm(`Remove console user "${name}"?`)) return;
    try {
      await api.consoleUsers.remove(name);
      void load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to remove");
    }
  };

  return (
    <Page>
      <PageHeader eyebrow="Console / access control" title="Console users" description="Accounts for this console, separate from credentials stored for each MeshDB connection. Roles limit which observations and actions are available." />

      {error && <div className="mb-4"><Banner tone="error">{error}</Banner></div>}

      <Card title="Add user" className="mb-6">
        <form onSubmit={add} className="grid grid-cols-1 items-end gap-4 md:grid-cols-3">
          <TextInput
            label="Username"
            value={form.username}
            onChange={(e) => setForm({ ...form, username: e.target.value })}
            required
          />
          <TextInput
            label="Password"
            type="password"
            value={form.password}
            onChange={(e) => setForm({ ...form, password: e.target.value })}
            required
          />
          <Select
            label="Role"
            value={form.role}
            onChange={(e) => setForm({ ...form, role: e.target.value as api.Role })}
          >
            <option value="viewer">viewer</option>
            <option value="developer">developer</option>
            <option value="operator">operator</option>
            <option value="admin">admin</option>
          </Select>
          <div className="md:col-span-3">
            <Button type="submit">Create user</Button>
          </div>
        </form>
      </Card>

      {list === null ? (
        <Spinner label="Loading users…" />
      ) : (
        <DataTable
          columns={["Name", "Role", ""]}
          rows={list.map((u) => [
            u.name,
            <Tag tone={u.role === "admin" ? "red" : u.role === "developer" ? "blue" : u.role === "operator" ? "yellow" : "gray"}>
              {u.role}
            </Tag>,
            <button className="text-carbon-red hover:underline" onClick={() => void remove(u.name)}>
              Remove
            </button>,
          ])}
        />
      )}
    </Page>
  );
}
