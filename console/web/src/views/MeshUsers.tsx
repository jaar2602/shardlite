import { useEffect, useState } from "react";
import * as api from "../lib/api";
import { Banner, Button, Card, DataTable, Page, PageHeader, Select, Spinner, Tag, TextInput } from "../components/ui";

// shardlite's own users on the cluster (Read/Write/Admin/Cluster), managed through the connection's
// stored credential — which must itself be an Admin for these calls to succeed. A 403 here means
// the stored shardlite credential lacks the Admin role, and is surfaced as such.
export default function MeshUsers({ name }: { name: string }) {
  const c = api.conn(name);
  const [users, setUsers] = useState<{ name: string; role: string }[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [form, setForm] = useState({ name: "", secret: "", role: "read" });

  const load = async () => {
    setError(null);
    try {
      const r = await c.meshUsers.list();
      setUsers(r.users);
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to load users");
      setUsers([]);
    }
  };
  useEffect(() => {
    void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [name]);

  const add = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    try {
      await c.meshUsers.create(form.name, form.secret, form.role);
      setForm({ name: "", secret: "", role: "read" });
      void load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to create user");
    }
  };

  const remove = async (n: string) => {
    if (!confirm(`Drop shardlite user "${n}"?`)) return;
    setError(null);
    try {
      await c.meshUsers.remove(n);
      void load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to drop user");
    }
  };

  const tone = (role: string) =>
    role === "admin" ? "red" : role === "write" ? "blue" : role === "cluster" ? "yellow" : "gray";

  return (
    <Page>
      <PageHeader eyebrow="Database / access control" title="ShardLite users" description="Manage identities stored by this database. The connection credential must have ShardLite admin access." />
      {error && <Banner tone="error">{error}</Banner>}

      <Card title="Create shardlite user">
        <form onSubmit={add} className="grid grid-cols-1 items-end gap-4 md:grid-cols-3">
          <TextInput label="Name" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} required />
          <TextInput
            label="Secret"
            type="password"
            value={form.secret}
            onChange={(e) => setForm({ ...form, secret: e.target.value })}
            required
          />
          <Select label="Role" value={form.role} onChange={(e) => setForm({ ...form, role: e.target.value })}>
            <option value="read">read</option>
            <option value="write">write</option>
            <option value="admin">admin</option>
          </Select>
          <div className="md:col-span-3">
            <Button type="submit">Create user</Button>
          </div>
        </form>
      </Card>

      {users === null ? (
        <Spinner label="Loading users…" />
      ) : (
        <DataTable
          columns={["Name", "Role", ""]}
          empty="No users, or the stored credential is not an admin."
          rows={users.map((u) => [
            u.name,
            <Tag tone={tone(u.role)}>{u.role}</Tag>,
            <button className="text-carbon-red hover:underline" onClick={() => void remove(u.name)}>
              Drop
            </button>,
          ])}
        />
      )}
    </Page>
  );
}
