import { useEffect, useState } from "react";
import * as api from "../lib/api";
import { Banner, Button, Card, Page, PageHeader, Spinner, TextInput } from "../components/ui";

export default function AiSettings() {
  const [settings, setSettings] = useState<api.AiSettings | null>(null);
  const [form, setForm] = useState<{ base_url: string; model: string; enabled: boolean; max_tool_calls: number; api_key: string }>({
    base_url: "",
    model: "",
    enabled: false,
    max_tool_calls: 0,
    api_key: "",
  });
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const load = async () => {
    try {
      const value = await api.getAiConfig();
      setSettings(value);
      setForm({ base_url: value.base_url, model: value.model, enabled: value.enabled, max_tool_calls: value.max_tool_calls, api_key: "" });
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to load");
    }
  };
  useEffect(() => {
    void load();
  }, []);

  const save = async (e: React.FormEvent) => {
    e.preventDefault();
    setSaving(true);
    setError(null);
    setMessage(null);
    // Omit api_key to preserve the stored key when the field is blank and a key is already set;
    // send whatever the user typed (including "" to clear) when there is no stored key.
    const preserve = form.api_key === "" && settings?.has_key === true;
    try {
      const value = await api.putAiConfig({
        base_url: form.base_url,
        model: form.model,
        enabled: form.enabled,
        max_tool_calls: form.max_tool_calls,
        ...(preserve ? {} : { api_key: form.api_key }),
      });
      setSettings(value);
      setForm({ base_url: value.base_url, model: value.model, enabled: value.enabled, max_tool_calls: value.max_tool_calls, api_key: "" });
      setMessage("Assistant settings saved.");
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to save");
    } finally {
      setSaving(false);
    }
  };

  return (
    <Page>
      <PageHeader eyebrow="Console / assistant" title="AI settings" description="Configure the OpenAI-compatible provider that powers the per-connection assistant. The API key is stored encrypted and never returned." />

      {error && <div className="mb-4"><Banner tone="error">{error}</Banner></div>}
      {message && <div className="mb-4"><Banner tone="success">{message}</Banner></div>}

      {settings === null ? (
        <Spinner label="Loading settings…" />
      ) : (
        <Card title="Provider">
          <form onSubmit={save} className="grid grid-cols-1 items-end gap-4 md:grid-cols-2">
            <TextInput
              label="Base URL"
              value={form.base_url}
              onChange={(e) => setForm({ ...form, base_url: e.target.value })}
              placeholder="https://api.openai.com/v1"
            />
            <TextInput
              label="Model"
              value={form.model}
              onChange={(e) => setForm({ ...form, model: e.target.value })}
              placeholder="gpt-4o"
            />
            <TextInput
              label="API key"
              type="password"
              value={form.api_key}
              onChange={(e) => setForm({ ...form, api_key: e.target.value })}
              placeholder={settings.has_key ? "•••••••• key is set — leave blank to keep it" : "sk-…"}
            />
            <TextInput
              label="Max tool calls (0 = default 8)"
              type="number"
              min={0}
              value={form.max_tool_calls}
              onChange={(e) => setForm({ ...form, max_tool_calls: Number(e.target.value) })}
            />
            <label className="flex items-center gap-2 self-end py-2 text-sm text-carbon-text md:col-span-2">
              <input
                type="checkbox"
                checked={form.enabled}
                onChange={(e) => setForm({ ...form, enabled: e.target.checked })}
              />
              Enable the assistant
            </label>
            <div className="md:col-span-2">
              <Button type="submit" disabled={saving}>{saving ? "Saving…" : "Save settings"}</Button>
            </div>
          </form>
        </Card>
      )}
    </Page>
  );
}
