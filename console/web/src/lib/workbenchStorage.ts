export type HistoryStatus = "ok" | "queued" | "failed" | "cancelled";

export type SavedQuery = {
  id: number;
  name: string;
  sql: string;
  updatedAt: number;
};

export type HistoryItem = {
  id: number;
  at: number;
  sql: string;
  summary: string;
  status: HistoryStatus;
  elapsedMs: number;
  rowCount?: number;
};

export function normalizeSaved(value: unknown): SavedQuery[] {
  if (!Array.isArray(value)) return [];
  return value.flatMap((item) => isRecord(item) && typeof item.id === "number" && typeof item.name === "string" && typeof item.sql === "string"
    ? [{ id: item.id, name: item.name, sql: item.sql, updatedAt: typeof item.updatedAt === "number" ? item.updatedAt : item.id }]
    : []);
}

export function normalizeHistory(value: unknown): HistoryItem[] {
  if (!Array.isArray(value)) return [];
  return value.flatMap((item) => {
    if (!isRecord(item) || typeof item.id !== "number" || typeof item.at !== "number") return [];
    const status: HistoryStatus = item.status === "ok" || item.status === "queued" || item.status === "failed" || item.status === "cancelled" ? item.status : "failed";
    const legacyMode = typeof item.mode === "string" ? item.mode.replace("_", " ") : "Execution";
    const legacyTarget = typeof item.target === "string" ? ` · ${item.target}` : "";
    return [{
      id: item.id,
      at: item.at,
      sql: typeof item.sql === "string" ? item.sql : "",
      summary: typeof item.summary === "string" ? item.summary : `${legacyMode}${legacyTarget}`,
      status,
      elapsedMs: typeof item.elapsedMs === "number" ? item.elapsedMs : 0,
      rowCount: typeof item.rowCount === "number" ? item.rowCount : undefined,
    }];
  }).slice(0, 100);
}

/** Drop placement controls from older workbench profiles without touching user-authored SQL. */
export function removeLegacyPlacementSettings(storage: Pick<Storage, "removeItem">, prefix: string): void {
  for (const suffix of [".routeTarget", ".shard", ".placement"]) storage.removeItem(prefix + suffix);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
