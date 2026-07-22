import { useEffect, useState } from "react";
import * as api from "../lib/api";
import { Banner, Button, DataTable, Page, PageHeader, Spinner, Tag } from "../components/ui";

export default function Activity() {
  const [events, setEvents] = useState<api.AuditEvent[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = async () => {
    try {
      setEvents(await api.audit.list());
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : "failed to load audit events");
    }
  };

  useEffect(() => {
    void load();
  }, []);

  return (
    <Page>
      <PageHeader eyebrow="Console / audit ledger" title="Activity" description="Security and change events retained by the console. SQL text, parameters, passwords, and credentials are never recorded." actions={<Button variant="secondary" onClick={() => void load()}>Refresh now</Button>} />
      {error && <div className="mb-4"><Banner tone="error">{error}</Banner></div>}
      {events === null ? (
        <Spinner label="Loading activity…" />
      ) : (
        <DataTable
          columns={["Time", "Actor", "Action", "Target", "Outcome"]}
          empty="No audit events yet"
          rows={events.map((event) => [
            new Date(event.t).toLocaleString(),
            event.actor ?? "anonymous",
            event.action,
            event.target,
            <Tag tone={event.outcome === "ok" ? "green" : event.outcome === "denied" || event.outcome === "throttled" ? "red" : "yellow"}>
              {event.outcome}
            </Tag>,
          ])}
        />
      )}
    </Page>
  );
}
