import { useEffect, useState } from "react";
import * as api from "../lib/api";
import { Banner, Button, Card, DataTable, Select, Spinner, Tag, TextInput } from "../components/ui";

export default function ConsoleUsers() {
  const [list, setList] = useState<{ name: string; role: api.Role }[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [form, setForm] = useState<{ username: string; password: string; role: api.Role }>({
    username: "",
    password: "",
    role: "user",
  });

  const load = async () => {
    try {
      setList(await api.consoleUsers.list());
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
      setForm({ username: "", password: "", role: "user" });
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
    <div className="p-8 max-w-3xl">
      <h1 className="text-2xl text-carbon-text mb-2">Console users</h1>
      <p className="text-carbon-text-3 text-sm mb-6">
        These are the console's own accounts, separate from the meshdb credentials stored per
        connection. Admins manage users and connections; users may use connections and read
        observability.
      </p>

      {error && <div className="mb-4"><Banner tone="error">{error}</Banner></div>}

      <Card title="Add user" className="mb-6">
        <form onSubmit={add} className="grid grid-cols-3 gap-4 items-end">
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
            <option value="user">user</option>
            <option value="admin">admin</option>
          </Select>
          <div className="col-span-3">
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
            <Tag tone={u.role === "admin" ? "blue" : "gray"}>{u.role}</Tag>,
            <button className="text-carbon-red hover:underline" onClick={() => void remove(u.name)}>
              Remove
            </button>,
          ])}
        />
      )}
    </div>
  );
}
