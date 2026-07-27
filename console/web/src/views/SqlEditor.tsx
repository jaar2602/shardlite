import { useEffect, useMemo, useRef, useState } from "react";
import { useAuth } from "../auth";
import { CodeEditor, CodeEditorHandle, EditorRange } from "../components/CodeEditor";
import { Banner, Button, Select, Spinner, Tag, TextInput } from "../components/ui";
import * as api from "../lib/api";
import {
  buildExecutionPlan,
  dispatchExecutionPlan,
  ExecutionPlan,
  RunTarget,
  SqlStatement,
  statementsForTarget,
} from "../lib/sqlWorkbench";
import { HistoryItem, normalizeHistory, normalizeSaved, removeLegacyPlacementSettings, SavedQuery } from "../lib/workbenchStorage";

type ParamType = "null" | "integer" | "real" | "text" | "boolean" | "blob";
type ParamDraft = { id: number; type: ParamType; value: string };

/// One editor tab. The active tab's values live in component state; this is what the others hold
/// while they wait.
interface TabSnapshot {
  id: number;
  title: string;
  sql: string;
  scratch: string;
  selection: EditorRange;
  activeSavedId: number | null;
  results: ResultSet[];
  activeResult: number | null;
  params: ParamDraft[];
}
type ResultSet = { id: number; title: string; result: api.QueryResult };
type Message = { tone: "info" | "success"; text: string; operation?: string };
type SchemaReview = {
  sql: string;
  value: api.OperationPreflight;
  idempotencyKey: string;
  confirmed: boolean;
};
type Analysis = { statements: SqlStatement[]; plan: ExecutionPlan | null; error: string | null };
type LibraryView = "saved" | "history" | "tables";
type TableReference = { name: string; type: "table" | "view" };
type ColumnReference = { cid: number; name: string; type: string; notNull: boolean; primaryKey: number; hidden: number };

let nextId = 1;
const id = () => Date.now() * 100 + nextId++;
const emptyParam = (): ParamDraft => ({ id: id(), type: "text", value: "" });

export default function SqlEditor({ name }: { name: string }) {
  const { me } = useAuth();
  const mayWrite = api.permits(me?.role, "write");
  const c = api.conn(name);
  const storageKey = `shardlite.workbench.${me?.user ?? "unknown"}.${name}`;
  const draftKey = storageKey + ".draft";
  const savedKey = storageKey + ".saved";
  const historyKey = storageKey + ".history";
  const splitKey = storageKey + ".split";

  const initialDraft = readText(draftKey, "SELECT 1;");
  const [sql, setSql] = useState(initialDraft);
  const [scratch, setScratch] = useState(initialDraft);
  const [activeSavedId, setActiveSavedId] = useState<number | null>(null);
  const [selection, setSelection] = useState<EditorRange>({ from: 0, to: 0 });
  const [runTarget, setRunTarget] = useState<RunTarget>("current");
  const [params, setParams] = useState<ParamDraft[]>([]);
  const [routeKey, setRouteKey] = useState("");
  const [targetReads, setTargetReads] = useState(false);
  const [consistency, setConsistency] = useState<"linearizable" | "stale" | "at_least_lsn">("linearizable");
  const [atLeastLsn, setAtLeastLsn] = useState(0);
  const [showParameters, setShowParameters] = useState(false);
  const [showOptions, setShowOptions] = useState(false);
  // When on, a run adds to this tab's result sets instead of replacing them, so you can compare
  // two answers side by side without re-running the first.
  const [keepResults, setKeepResults] = useState(() => readBoolean(storageKey + ".keepResults", false));
  const [busy, setBusy] = useState(false);
  const [cancellable, setCancellable] = useState(false);
  const [busyLabel, setBusyLabel] = useState("Running…");
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<Message | null>(null);
  const [queryPlan, setQueryPlan] = useState<api.QueryPlan | null>(null);
  const [results, setResults] = useState<ResultSet[]>([]);
  const [activeResult, setActiveResult] = useState<number | null>(null);
  const [schemaReview, setSchemaReview] = useState<SchemaReview | null>(null);
  const [saved, setSaved] = useState<SavedQuery[]>(() => readSaved(savedKey));
  const [historyItems, setHistoryItems] = useState<HistoryItem[]>(() => readHistory(historyKey));
  const [libraryTab, setLibraryTab] = useState<LibraryView>("tables");
  const [librarySearch, setLibrarySearch] = useState("");
  const [tables, setTables] = useState<TableReference[]>([]);
  const [tablesLoaded, setTablesLoaded] = useState(false);
  const [tablesBusy, setTablesBusy] = useState(false);
  const [tableError, setTableError] = useState<string | null>(null);
  const [expandedTables, setExpandedTables] = useState<string[]>([]);
  const [tableColumns, setTableColumns] = useState<Record<string, ColumnReference[]>>({});
  const [columnsBusy, setColumnsBusy] = useState<string[]>([]);
  const [desktopLibrary, setDesktopLibrary] = useState(() => readBoolean(storageKey + ".library", true));
  const [mobileLibrary, setMobileLibrary] = useState(false);
  // --- editor tabs -------------------------------------------------------------------------
  // The active tab's state stays in the hooks above, so nothing below has to change. Inactive
  // tabs keep a snapshot here, swapped in and out on switch. Storing every tab's state in one
  // array instead would mean rewriting every `sql`/`results`/`params` reference in this file.
  const [tabs, setTabs] = useState<TabSnapshot[]>(() => [
    { id: 1, title: "Query 1", sql: initialDraft, scratch: initialDraft, selection: { from: 0, to: 0 }, activeSavedId: null, results: [], activeResult: null, params: [] },
  ]);
  const [activeTab, setActiveTab] = useState(1);

  const snapshotActive = (): TabSnapshot[] =>
    tabs.map((tab) => (tab.id === activeTab ? { ...tab, sql, scratch, selection, activeSavedId, results, activeResult, params } : tab));

  const switchTab = (target: number) => {
    if (target === activeTab) return;
    const snapshots = snapshotActive();
    const next = snapshots.find((tab) => tab.id === target);
    if (!next) return;
    setTabs(snapshots);
    setActiveTab(target);
    setSql(next.sql);
    setScratch(next.scratch);
    setSelection(next.selection);
    setActiveSavedId(next.activeSavedId);
    setResults(next.results);
    setActiveResult(next.activeResult);
    setParams(next.params);
    setError(null);
    setMessage(null);
    setSchemaReview(null);
    setQueryPlan(null);
  };

  const addTab = () => {
    const nextId = Math.max(0, ...tabs.map((tab) => tab.id)) + 1;
    const blank: TabSnapshot = { id: nextId, title: `Query ${nextId}`, sql: "", scratch: "", selection: { from: 0, to: 0 }, activeSavedId: null, results: [], activeResult: null, params: [] };
    setTabs([...snapshotActive(), blank]);
    setActiveTab(nextId);
    setSql("");
    setScratch("");
    setSelection({ from: 0, to: 0 });
    setActiveSavedId(null);
    setResults([]);
    setActiveResult(null);
    setParams([]);
    setError(null);
    setMessage(null);
    setSchemaReview(null);
    setQueryPlan(null);
  };

  const closeTab = (target: number) => {
    if (tabs.length === 1) return;                       // always leave one to type in
    const remaining = snapshotActive().filter((tab) => tab.id !== target);
    if (target !== activeTab) {
      setTabs(remaining);
      return;
    }
    const fallback = remaining[remaining.length - 1];
    setTabs(remaining);
    setActiveTab(fallback.id);
    setSql(fallback.sql);
    setScratch(fallback.scratch);
    setSelection(fallback.selection);
    setActiveSavedId(fallback.activeSavedId);
    setResults(fallback.results);
    setActiveResult(fallback.activeResult);
    setParams(fallback.params);
    setError(null);
    setMessage(null);
    setSchemaReview(null);
    setQueryPlan(null);
  };

  const [showSave, setShowSave] = useState(false);
  const [saveName, setSaveName] = useState("");
  const [renamingId, setRenamingId] = useState<number | null>(null);
  const [renameValue, setRenameValue] = useState("");
  const [splitPercent, setSplitPercent] = useState(() => clamp(readNumber(splitKey, 50), 25, 75));
  const [exportFormat, setExportFormat] = useState<"ndjson" | "csv">("ndjson");
  const [exportLimit, setExportLimit] = useState("1000000");
  const splitHost = useRef<HTMLDivElement>(null);
  const editor = useRef<CodeEditorHandle>(null);
  const abort = useRef<AbortController | null>(null);

  useEffect(() => {
    try { removeLegacyPlacementSettings(localStorage, storageKey); } catch { /* storage can be unavailable */ }
  }, [storageKey]);

  const analysis = useMemo<Analysis>(() => {
    try {
      const statements = statementsForTarget(sql, runTarget, selection);
      return { statements, plan: buildExecutionPlan(statements), error: null };
    } catch (caught) {
      return { statements: [], plan: null, error: caught instanceof Error ? caught.message : "Cannot analyze SQL." };
    }
  }, [runTarget, selection, sql]);
  const parameterCount = analysis.statements.reduce((sum, statement) => sum + statement.parameterCount, 0);
  const readPlan = analysis.plan?.kind === "reads";
  const writePlan = analysis.plan?.kind === "write" || analysis.plan?.kind === "transaction";
  const targetedRead = readPlan && (targetReads || parameterCount > 0);
  const needsRoute = writePlan || targetedRead;

  // Ask the cluster how a single read statement would run across shards, so a heavy central
  // execution is flagged before it runs. Debounced, best-effort, and only when not routed to one
  // shard (a single-shard read has no cross-shard plan to warn about).
  const explainSql = readPlan && analysis.statements.length === 1 && !targetedRead
    ? analysis.statements[0].sql
    : null;
  useEffect(() => {
    if (!explainSql || !explainSql.trim()) { setQueryPlan(null); return; }
    const controller = new AbortController();
    const timer = window.setTimeout(() => {
      api.conn(name).explain(explainSql, controller.signal).then(setQueryPlan).catch(() => { /* best-effort; Run surfaces real errors */ });
    }, 400);
    return () => { controller.abort(); window.clearTimeout(timer); };
  }, [explainSql, name]);
  const loadedSaved = saved.find((item) => item.id === activeSavedId);
  const dirty = loadedSaved ? loadedSaved.sql !== sql : false;

  const setDocument = (value: string) => {
    setSql(value);
    if (activeSavedId === null) {
      setScratch(value);
      localStorage.setItem(draftKey, value);
    }
    if (schemaReview?.sql !== value) setSchemaReview(null);
  };

  const clearOutput = () => {
    setError(null);
    setMessage(null);
    setSchemaReview(null);
    if (!keepResults) {
      setResults([]);
      setActiveResult(null);
    }
  };

  const resolveTarget = async (): Promise<number> => {
    if (!routeKey.trim()) throw new Error("Enter a data key, such as a tenant, customer, or account ID.");
    try {
      return (await c.route(routeKey)).shard;
    } catch {
      throw new Error("ShardLite could not locate the data for this key. Check the value and try again.");
    }
  };

  const parameterValues = (statements: SqlStatement[]): unknown[][] => {
    const expected = statements.reduce((sum, statement) => sum + statement.parameterCount, 0);
    if (expected === 0) return statements.map(() => []);
    if (params.length !== expected) {
      throw new Error(`This SQL has ${expected} positional parameter${expected === 1 ? "" : "s"}; ${params.length} value${params.length === 1 ? " is" : "s are"} configured.`);
    }
    const values = typedParams(params);
    let offset = 0;
    return statements.map((statement) => {
      const next = values.slice(offset, offset + statement.parameterCount);
      offset += statement.parameterCount;
      return next;
    });
  };

  const addHistory = (item: Omit<HistoryItem, "id" | "at">) => {
    setHistoryItems((current) => {
      const next = [{ id: id(), at: Date.now(), ...item }, ...current].slice(0, 100);
      localStorage.setItem(historyKey, JSON.stringify(next));
      return next;
    });
  };

  /// `targetOverride` lets a keystroke ask for something other than the dropdown's setting:
  /// Ctrl+Enter runs the selection or the statement at the cursor, Ctrl+Shift+Enter runs all.
  const run = async (editorSelection?: EditorRange, targetOverride?: RunTarget) => {
    const effectiveSelection = editorSelection ?? selection;
    const target = targetOverride ?? runTarget;
    let statements: SqlStatement[];
    let plan: ExecutionPlan;
    try {
      statements = statementsForTarget(sql, target, effectiveSelection);
      plan = buildExecutionPlan(statements);
      if (plan.kind !== "reads" && !mayWrite) throw new Error("Your console role does not permit data or schema changes.");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Cannot analyze SQL.");
      return;
    }

    const executedSql = statements.map((statement) => statement.sql).join("\n");
    const started = performance.now();
    const controller = new AbortController();
    abort.current = controller;
    setBusy(true);
    setCancellable(plan.kind === "reads");
    setBusyLabel(plan.kind === "reads" ? "Reading results…" : plan.kind === "schema" ? "Checking schema change…" : "Applying change…");
    clearOutput();
    let summary = planSummary(plan);
    let rowCount: number | undefined;
    try {
      const statementParams = parameterValues(statements);
      const dispatched = await dispatchExecutionPlan(
        plan,
        statementParams,
        targetReads || statements.some((statement) => statement.parameterCount > 0),
        {
          route: resolveTarget,
          queryAll: (statement) => c.queryAll(statement, controller.signal).then((value) => ({ ...value, truncated: false })),
          query: (statement, selected, values) => c.query(statement, {
            shard: selected,
            params: values,
            consistency: consistencyValue(consistency, atLeastLsn),
            signal: controller.signal,
          }),
          execute: (statement, selected, values) => c.execute(statement, selected, values),
          run: (statement) => c.run(statement),
          transaction: (items, selected) => c.tx(items, selected),
          preflight: (statement) => api.operations.preflight(name, statement),
        },
        (value, statement, index) => {
          const resultSet = { id: id(), title: resultTitle(index, statement), result: value };
          setResults((current) => [...current, resultSet]);
          setActiveResult(resultSet.id);
        },
      );
      if (dispatched.kind === "reads") {
        const rows = dispatched.values.reduce((sum, value) => sum + value.rows.length, 0);
        rowCount = rows;
        summary = `${statements.length} read${statements.length === 1 ? "" : "s"} · ${rows} displayed row${rows === 1 ? "" : "s"}`;
      } else if (dispatched.kind === "write") {
        const value = dispatched.value;
        summary = `Data change · ${value.rows_affected} rows affected`;
        setMessage({ tone: "success", text: `${summary} · last rowid ${value.last_insert_rowid}` });
      } else if (dispatched.kind === "transaction") {
        const value = dispatched.value;
        summary = `Atomic transaction · ${value.rows_affected} rows affected`;
        setMessage({ tone: "success", text: `${summary} · last rowid ${value.last_insert_rowid}` });
      } else if (dispatched.kind === "schema") {
        const statement = plan.statements[0];
        setSchemaReview({ sql: statement.sql, value: dispatched.value, idempotencyKey: crypto.randomUUID(), confirmed: false });
        setMessage({ tone: "info", text: "Schema check complete. Review and approve the change below." });
        return;
      }
      const elapsedMs = Math.round(performance.now() - started);
      addHistory({ sql: executedSql, summary, status: "ok", elapsedMs, rowCount });
    } catch (caught) {
      const cancelled = caught instanceof DOMException && caught.name === "AbortError";
      const elapsedMs = Math.round(performance.now() - started);
      const text = cancelled ? "Request cancelled; remaining statements were not run." : databaseError(caught, "Request failed.");
      setError(text);
      addHistory({ sql: executedSql, summary, status: cancelled ? "cancelled" : "failed", elapsedMs, rowCount });
    } finally {
      if (abort.current === controller) abort.current = null;
      setCancellable(false);
      setBusy(false);
    }
  };

  const applySchema = async () => {
    if (!schemaReview?.confirmed) return;
    setBusy(true);
    setCancellable(false);
    setBusyLabel("Queueing schema change…");
    setError(null);
    const started = performance.now();
    try {
      const operation = await api.operations.submit({
        connection: name,
        sql: schemaReview.sql,
        idempotency_key: schemaReview.idempotencyKey,
        preflight_token: schemaReview.value.token,
        expected_versions: schemaReview.value.versions,
      });
      setMessage({ tone: "success", text: `Schema change ${operation.id} is ${operation.status}.`, operation: operation.id });
      addHistory({ sql: schemaReview.sql, summary: "Schema change", status: "queued", elapsedMs: Math.round(performance.now() - started) });
      setSchemaReview(null);
    } catch (caught) {
      setError(databaseError(caught, "Schema change could not be queued."));
    } finally {
      setBusy(false);
    }
  };

  const explain = async () => {
    if (analysis.plan?.kind !== "reads" || analysis.statements.length !== 1) {
      setError("Explain requires one selected read statement.");
      return;
    }
    setTargetReads(true);
    setBusy(true);
    setCancellable(true);
    setBusyLabel("Building query plan…");
    setError(null);
    setMessage(null);
    const controller = new AbortController();
    abort.current = controller;
    try {
      const statementParams = parameterValues(analysis.statements);
      const selected = await resolveTarget();
      const value = await c.query(`EXPLAIN QUERY PLAN ${analysis.statements[0].sql}`, {
        shard: selected,
        params: statementParams[0],
        consistency: consistencyValue(consistency, atLeastLsn),
        signal: controller.signal,
      });
      const resultSet = { id: id(), title: "Query plan", result: value };
      setResults((current) => (keepResults ? [...current, resultSet] : [resultSet]));
      setActiveResult(resultSet.id);
    } catch (caught) {
      setError(databaseError(caught, "Explain failed."));
    } finally {
      if (abort.current === controller) abort.current = null;
      setCancellable(false);
      setBusy(false);
    }
  };

  const exportQuery = async () => {
    if (analysis.plan?.kind !== "reads" || analysis.statements.length !== 1) {
      setError("Export requires one selected read statement.");
      return;
    }
    setTargetReads(true);
    try {
      const statementParams = parameterValues(analysis.statements);
      const selected = await resolveTarget();
      api.downloadQuery(name, analysis.statements[0].sql, {
        shard: selected,
        params: statementParams[0],
        consistency: consistencyValue(consistency, atLeastLsn),
        format: exportFormat,
        maxRows: exportLimit ? Number(exportLimit) : null,
      });
    } catch (caught) {
      setError(databaseError(caught, "Export failed."));
    }
  };

  const openDocument = (value: string, savedId: number | null) => {
    if (dirty && !window.confirm("Discard unsaved changes to the current saved query?")) return;
    setSql(value);
    setActiveSavedId(savedId);
    setParams([]);
    setSchemaReview(null);
    setError(null);
    setMobileLibrary(false);
    if (savedId === null) {
      setScratch(value);
      localStorage.setItem(draftKey, value);
    }
  };

  const saveCurrent = (forceName = false) => {
    if (activeSavedId !== null && !forceName) {
      const next = saved.map((item) => item.id === activeSavedId ? { ...item, sql, updatedAt: Date.now() } : item);
      setSaved(next);
      localStorage.setItem(savedKey, JSON.stringify(next));
      return;
    }
    setSaveName(activeSavedId === null ? "" : loadedSaved?.name ?? "");
    setShowSave(true);
  };

  const confirmSave = () => {
    const nameValue = saveName.trim();
    if (!nameValue) return;
    const existing = saved.find((item) => item.name.toLowerCase() === nameValue.toLowerCase());
    const savedId = existing?.id ?? id();
    const next = existing
      ? saved.map((item) => item.id === existing.id ? { ...item, name: nameValue, sql, updatedAt: Date.now() } : item)
      : [{ id: savedId, name: nameValue, sql, updatedAt: Date.now() }, ...saved];
    setSaved(next);
    setActiveSavedId(savedId);
    setShowSave(false);
    localStorage.setItem(savedKey, JSON.stringify(next));
  };

  const renameSaved = (savedId: number) => {
    const nameValue = renameValue.trim();
    if (!nameValue) return;
    const next = saved.map((item) => item.id === savedId ? { ...item, name: nameValue, updatedAt: Date.now() } : item);
    setSaved(next);
    setRenamingId(null);
    localStorage.setItem(savedKey, JSON.stringify(next));
  };

  const removeSaved = (savedId: number) => {
    if (!window.confirm("Delete this saved query?")) return;
    const next = saved.filter((item) => item.id !== savedId);
    setSaved(next);
    if (activeSavedId === savedId) {
      setActiveSavedId(null);
      setScratch(sql);
      localStorage.setItem(draftKey, sql);
    }
    localStorage.setItem(savedKey, JSON.stringify(next));
  };

  const toggleLibrary = () => {
    if (window.matchMedia("(min-width: 1024px)").matches) {
      setDesktopLibrary((current) => {
        localStorage.setItem(storageKey + ".library", JSON.stringify(!current));
        return !current;
      });
    } else setMobileLibrary(true);
  };

  const loadTables = async () => {
    setTablesBusy(true);
    setTableError(null);
    try {
      const catalog = await c.schemaCatalog();
      setTables(catalog.objects
        .filter((object) => object.type === "table" || object.type === "view")
        .map((object) => ({ name: object.name, type: object.type === "view" ? "view" : "table" })));
      setTableColumns(Object.fromEntries(catalog.tables.map((table) => [table.name, table.columns.map(toColumnReference)])));
      setTablesLoaded(true);
    } catch (caught) {
      setTableError(databaseError(caught, "Tables could not be loaded."));
    } finally {
      setTablesBusy(false);
    }
  };

  const selectLibraryTab = (value: LibraryView) => {
    setLibraryTab(value);
    setLibrarySearch("");
    if (value === "tables" && !tablesLoaded && !tablesBusy) void loadTables();
  };

  const toggleTable = async (table: TableReference) => {
    if (expandedTables.includes(table.name)) {
      setExpandedTables((current) => current.filter((item) => item !== table.name));
      return;
    }
    setExpandedTables((current) => [...current, table.name]);
    if (tableColumns[table.name] || columnsBusy.includes(table.name)) return;
    setColumnsBusy((current) => [...current, table.name]);
    setTableError(null);
    try {
      const catalog = await c.schemaCatalog();
      const details = catalog.tables.find((item) => item.name === table.name);
      setTableColumns((current) => ({ ...current, [table.name]: (details?.columns ?? []).map(toColumnReference) }));
    } catch (caught) {
      setTableError(databaseError(caught, `Columns for ${table.name} could not be loaded.`));
    } finally {
      setColumnsBusy((current) => current.filter((item) => item !== table.name));
    }
  };

  const refreshTables = () => {
    setTablesLoaded(false);
    setExpandedTables([]);
    setTableColumns({});
    void loadTables();
  };

  useEffect(() => {
    void loadTables();
    // Reload the table reference when the selected ShardLite connection changes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [name]);

  const resize = (clientY: number) => {
    const host = splitHost.current;
    if (!host) return;
    const rect = host.getBoundingClientRect();
    const minimum = Math.min(42, 200 / rect.height * 100);
    const next = clamp((clientY - rect.top) / rect.height * 100, minimum, 100 - minimum);
    setSplitPercent(next);
    localStorage.setItem(splitKey, String(next));
  };

  const stopResize = () => {
    window.removeEventListener("pointermove", moveResize);
    window.removeEventListener("pointerup", stopResize);
  };
  const moveResize = (event: PointerEvent) => resize(event.clientY);
  const startResize = (event: React.PointerEvent) => {
    event.preventDefault();
    window.addEventListener("pointermove", moveResize);
    window.addEventListener("pointerup", stopResize);
  };

  return (
    <div className="relative flex h-full min-h-0 overflow-hidden bg-carbon-bg">
      {desktopLibrary && <div className="hidden h-full lg:flex"><QueryLibrary
        activeSavedId={activeSavedId}
        historyItems={historyItems}
        librarySearch={librarySearch}
        libraryTab={libraryTab}
        columnsBusy={columnsBusy}
        expandedTables={expandedTables}
        renameValue={renameValue}
        renamingId={renamingId}
        saved={saved}
        scratch={scratch}
        tableColumns={tableColumns}
        tableError={tableError}
        tables={tables}
        tablesBusy={tablesBusy}
        onClearHistory={() => {
          if (!window.confirm("Clear local SQL history for this connection?")) return;
          setHistoryItems([]);
          localStorage.setItem(historyKey, "[]");
        }}
        onClose={() => {
          setDesktopLibrary(false);
          localStorage.setItem(storageKey + ".library", "false");
        }}
        onDelete={removeSaved}
        onOpenHistory={(item) => item.sql && openDocument(item.sql, null)}
        onOpenSaved={(item) => openDocument(item.sql, item.id)}
        onOpenScratch={() => openDocument(scratch, null)}
        onInsertIdentifier={(value) => editor.current?.insert(quoteIdentifier(value))}
        onRename={renameSaved}
        onRenameStart={(item) => { setRenamingId(item.id); setRenameValue(item.name); }}
        onRenameValue={setRenameValue}
        onSearch={setLibrarySearch}
        onTab={selectLibraryTab}
        onRefreshTables={refreshTables}
        onToggleTable={(table) => void toggleTable(table)}
      /></div>}

      {mobileLibrary && <>
        <button aria-label="Close query library" className="fixed inset-0 z-40 bg-black/60 lg:hidden" onClick={() => setMobileLibrary(false)} />
        <div className="fixed inset-y-0 left-0 z-50 flex w-72 lg:hidden"><QueryLibrary
          activeSavedId={activeSavedId}
          historyItems={historyItems}
          librarySearch={librarySearch}
          libraryTab={libraryTab}
          columnsBusy={columnsBusy}
          expandedTables={expandedTables}
          renameValue={renameValue}
          renamingId={renamingId}
          saved={saved}
          scratch={scratch}
          tableColumns={tableColumns}
          tableError={tableError}
          tables={tables}
          tablesBusy={tablesBusy}
          onClearHistory={() => {
            if (!window.confirm("Clear local SQL history for this connection?")) return;
            setHistoryItems([]);
            localStorage.setItem(historyKey, "[]");
          }}
          onClose={() => setMobileLibrary(false)}
          onDelete={removeSaved}
          onOpenHistory={(item) => item.sql && openDocument(item.sql, null)}
          onOpenSaved={(item) => openDocument(item.sql, item.id)}
          onOpenScratch={() => openDocument(scratch, null)}
          onInsertIdentifier={(value) => {
            editor.current?.insert(quoteIdentifier(value));
            setMobileLibrary(false);
          }}
          onRename={renameSaved}
          onRenameStart={(item) => { setRenamingId(item.id); setRenameValue(item.name); }}
          onRenameValue={setRenameValue}
          onSearch={setLibrarySearch}
          onTab={selectLibraryTab}
          onRefreshTables={refreshTables}
          onToggleTable={(table) => void toggleTable(table)}
        /></div>
      </>}

      <main className="flex min-w-0 flex-1 flex-col overflow-hidden">
        <div className="flex shrink-0 items-stretch overflow-x-auto whitespace-nowrap border-b border-carbon-border bg-carbon-layer2">
          {tabs.map((tab) => {
            const isActive = tab.id === activeTab;
            const label = isActive
              ? (activeSavedId !== null ? saved.find((entry) => entry.id === activeSavedId)?.name : null) ?? tab.title
              : (tab.activeSavedId !== null ? saved.find((entry) => entry.id === tab.activeSavedId)?.name : null) ?? tab.title;
            const count = isActive ? results.length : tab.results.length;
            return (
              <div
                key={tab.id}
                className={`group flex items-center gap-2 border-r border-carbon-border px-3 py-2 text-xs ${isActive ? "bg-carbon-layer text-carbon-text" : "text-carbon-text-3 hover:bg-carbon-layer/60"}`}
              >
                <button type="button" className="max-w-40 truncate" title={label} onClick={() => switchTab(tab.id)}>
                  {label}
                </button>
                {count > 0 && <span className="rounded-sm bg-carbon-border px-1 text-[10px]">{count}</span>}
                {tabs.length > 1 && (
                  <button
                    type="button"
                    aria-label={`Close ${label}`}
                    className="opacity-0 transition-opacity hover:text-carbon-red group-hover:opacity-100"
                    onClick={() => closeTab(tab.id)}
                  >
                    ×
                  </button>
                )}
              </div>
            );
          })}
          <button
            type="button"
            aria-label="New query tab"
            title="New query tab"
            className="px-3 py-2 text-xs text-carbon-text-3 hover:text-carbon-text"
            onClick={addTab}
          >
            +
          </button>
        </div>
        <div className="flex h-12 shrink-0 items-center gap-2 overflow-x-auto whitespace-nowrap border-b border-carbon-border bg-carbon-layer px-3">
          <Button className="px-3" variant="ghost" onClick={toggleLibrary}>☰ <span className="ml-1 hidden sm:inline">Queries &amp; tables</span></Button>
          <Button disabled={busy || !analysis.plan || (analysis.plan.kind !== "reads" && !mayWrite)} onClick={() => void run()}>{busy ? "Running…" : "Run"}</Button>
          {busy && cancellable && <Button variant="secondary" onClick={() => abort.current?.abort()}>Cancel</Button>}
          <div className="w-44">
            <Select aria-label="Run target" value={runTarget} onChange={(event) => setRunTarget(event.target.value as RunTarget)} title="Ctrl/Cmd+Enter runs the selection or the statement at the cursor · Ctrl/Cmd+Shift+Enter runs all">
              <option value="current">Current statement</option>
              <option value="selection">Selection</option>
              <option value="all">All SQL</option>
            </Select>
          </div>
          <Tag tone={planTone(analysis.plan)}>{analysis.plan ? planLabel(analysis.plan) : "Check SQL"}</Tag>
          {explainSql && queryPlan && (
            <Tag tone={!queryPlan.supported ? "red" : queryPlan.heavy ? "yellow" : "gray"}>
              <span title={queryPlan.note}>{queryPlan.heavy ? "⚠ " : ""}{queryPlan.strategy}</span>
            </Tag>
          )}
          {needsRoute && <input
            aria-label="Data key"
            className="min-w-40 max-w-64 flex-1 border-b border-carbon-text-3 bg-carbon-field px-3 py-2 text-sm outline-none focus:border-carbon-blue"
            placeholder="Data key · tenant, customer, account…"
            title="An application value ShardLite uses to find the data. This is not a storage location."
            value={routeKey}
            onChange={(event) => setRouteKey(event.target.value)}
          />}
          <div className="ml-auto flex items-center gap-1">
            {(parameterCount > 0 || params.length > 0) && <Button variant="ghost" onClick={() => setShowParameters((value) => !value)}>Parameters {parameterCount ? `(${parameterCount})` : ""}</Button>}
            <Button variant="ghost" onClick={() => saveCurrent()}>{activeSavedId === null ? "Save" : dirty ? "Save changes" : "Saved"}</Button>
            {activeSavedId !== null && <Button variant="ghost" onClick={() => saveCurrent(true)}>Save as</Button>}
            <label
              className="flex cursor-pointer select-none items-center gap-1.5 px-2 text-xs text-carbon-text-3"
              title="Keep this tab's previous results when you run again, instead of replacing them."
            >
              <input
                type="checkbox"
                className="accent-carbon-blue"
                checked={keepResults}
                onChange={(event) => {
                  setKeepResults(event.target.checked);
                  try { localStorage.setItem(storageKey + ".keepResults", JSON.stringify(event.target.checked)); } catch { /* storage can be unavailable */ }
                }}
              />
              Keep results
            </label>
            <Button variant="ghost" onClick={() => setShowOptions((value) => !value)}>Options</Button>
          </div>
        </div>

        {showSave && <div className="flex shrink-0 items-end gap-2 border-b border-carbon-border bg-carbon-layer px-4 py-3">
          <div className="w-72"><TextInput autoFocus label="Saved query name" value={saveName} onChange={(event) => setSaveName(event.target.value)} onKeyDown={(event) => { if (event.key === "Enter") confirmSave(); }} /></div>
          <Button variant="secondary" disabled={!saveName.trim()} onClick={confirmSave}>Save query</Button>
          <Button variant="ghost" onClick={() => setShowSave(false)}>Cancel</Button>
        </div>}

        {showParameters && <div className="max-h-44 shrink-0 overflow-auto border-b border-carbon-border bg-carbon-layer">
          <ParamEditor expected={parameterCount} params={params} onChange={setParams} />
        </div>}

        {showOptions && <AdvancedOptions
          analysis={analysis}
          atLeastLsn={atLeastLsn}
          consistency={consistency}
          exportFormat={exportFormat}
          exportLimit={exportLimit}
          targetReads={targetReads}
          onConsistency={setConsistency}
          onExplain={() => void explain()}
          onExport={() => void exportQuery()}
          onExportFormat={setExportFormat}
          onExportLimit={setExportLimit}
          onLsn={setAtLeastLsn}
          onTargetReads={setTargetReads}
        />}

        <div ref={splitHost} className="flex min-h-0 flex-1 flex-col overflow-hidden">
          <section className="min-h-[200px] overflow-hidden p-2" style={{ flexBasis: `${splitPercent}%` }}>
            <CodeEditor
              ref={editor}
              value={sql}
              onChange={setDocument}
              onRun={(currentSelection, target) => void run(currentSelection, target)}
              onSelectionChange={setSelection}
            />
          </section>

          <div
            aria-label="Resize editor and results"
            aria-orientation="horizontal"
            role="separator"
            tabIndex={0}
            className="group hidden h-2 shrink-0 cursor-row-resize items-center bg-carbon-border focus:bg-carbon-blue md:flex"
            onPointerDown={startResize}
            onKeyDown={(event) => {
              if (event.key !== "ArrowUp" && event.key !== "ArrowDown") return;
              event.preventDefault();
              const next = clamp(splitPercent + (event.key === "ArrowDown" ? 2 : -2), 25, 75);
              setSplitPercent(next);
              localStorage.setItem(splitKey, String(next));
            }}
          >
            <span className="mx-auto h-px w-12 bg-carbon-text-3 group-focus:bg-white" />
          </div>

          <section className="flex min-h-[200px] flex-1 flex-col overflow-hidden border-t border-carbon-border md:border-t-0">
            <ResultsPane
              activeResult={activeResult}
              busy={busy}
              busyLabel={busyLabel}
              error={error ?? (!analysis.plan && sql.trim() ? analysis.error : null)}
              message={message}
              results={results}
              schemaReview={schemaReview}
              onActiveResult={setActiveResult}
              onApplySchema={() => void applySchema()}
              onConfirmSchema={(confirmed) => setSchemaReview((current) => current ? { ...current, confirmed } : null)}
            />
          </section>
        </div>
      </main>
    </div>
  );
}

function QueryLibrary({
  activeSavedId,
  columnsBusy,
  expandedTables,
  historyItems,
  librarySearch,
  libraryTab,
  renameValue,
  renamingId,
  saved,
  scratch,
  tableColumns,
  tableError,
  tables,
  tablesBusy,
  onClearHistory,
  onClose,
  onDelete,
  onOpenHistory,
  onOpenSaved,
  onOpenScratch,
  onInsertIdentifier,
  onRename,
  onRenameStart,
  onRenameValue,
  onSearch,
  onTab,
  onRefreshTables,
  onToggleTable,
}: {
  activeSavedId: number | null;
  columnsBusy: string[];
  expandedTables: string[];
  historyItems: HistoryItem[];
  librarySearch: string;
  libraryTab: LibraryView;
  renameValue: string;
  renamingId: number | null;
  saved: SavedQuery[];
  scratch: string;
  tableColumns: Record<string, ColumnReference[]>;
  tableError: string | null;
  tables: TableReference[];
  tablesBusy: boolean;
  onClearHistory: () => void;
  onClose: () => void;
  onDelete: (id: number) => void;
  onOpenHistory: (item: HistoryItem) => void;
  onOpenSaved: (item: SavedQuery) => void;
  onOpenScratch: () => void;
  onInsertIdentifier: (value: string) => void;
  onRename: (id: number) => void;
  onRenameStart: (item: SavedQuery) => void;
  onRenameValue: (value: string) => void;
  onSearch: (value: string) => void;
  onTab: (value: LibraryView) => void;
  onRefreshTables: () => void;
  onToggleTable: (table: TableReference) => void;
}) {
  const [copied, setCopied] = useState<string | null>(null);
  const needle = librarySearch.trim().toLowerCase();
  const visibleSaved = saved.filter((item) => !needle || item.name.toLowerCase().includes(needle) || item.sql.toLowerCase().includes(needle));
  const visibleHistory = historyItems.filter((item) => !needle || item.summary.toLowerCase().includes(needle) || item.sql.toLowerCase().includes(needle));
  const visibleTables = tables.filter((item) => !needle || item.name.toLowerCase().includes(needle) || tableColumns[item.name]?.some((column) => column.name.toLowerCase().includes(needle)));
  const tableCount = tables.filter((item) => item.type === "table").length;
  const viewCount = tables.length - tableCount;
  const copyIdentifier = async (value: string) => {
    await navigator.clipboard.writeText(quoteIdentifier(value));
    setCopied(value);
    window.setTimeout(() => setCopied(null), 1200);
  };
  return <aside className="flex h-full w-72 shrink-0 flex-col border-r border-carbon-border bg-carbon-layer">
    <div className="flex h-12 items-center justify-between border-b border-carbon-border px-3">
      <span className="text-sm font-semibold">Library</span>
      <Button className="px-2" variant="ghost" aria-label="Close query library" onClick={onClose}>×</Button>
    </div>
    <div className="grid grid-cols-3 border-b border-carbon-border">
      <LibraryTab active={libraryTab === "tables"} onClick={() => onTab("tables")}>Tables</LibraryTab>
      <LibraryTab active={libraryTab === "saved"} onClick={() => onTab("saved")}>Saved</LibraryTab>
      <LibraryTab active={libraryTab === "history"} onClick={() => onTab("history")}>History</LibraryTab>
    </div>
    <div className="border-b border-carbon-border p-3"><TextInput aria-label={`Search ${libraryTab}`} placeholder={libraryTab === "tables" ? "Search tables" : "Search SQL"} value={librarySearch} onChange={(event) => onSearch(event.target.value)} /></div>
    <div className="min-h-0 flex-1 overflow-y-auto">
      {libraryTab === "saved" ? <>
        <button className={`w-full border-b border-carbon-border px-3 py-3 text-left ${activeSavedId === null ? "bg-carbon-layer2" : "hover:bg-carbon-layer2/60"}`} onClick={onOpenScratch}>
          <span className="block text-sm font-medium">Draft</span>
          <span className="mt-1 block truncate font-mono text-xs text-carbon-text-3">{oneLine(scratch)}</span>
        </button>
        {visibleSaved.length === 0 && <p className="p-4 text-xs text-carbon-text-3">No saved queries match.</p>}
        {visibleSaved.map((item) => <div key={item.id} className={`border-b border-carbon-border p-3 ${activeSavedId === item.id ? "bg-carbon-layer2" : "hover:bg-carbon-layer2/60"}`}>
          {renamingId === item.id ? <div className="flex gap-1"><input autoFocus className="min-w-0 flex-1 border-b border-carbon-blue bg-carbon-field px-2 py-1 text-sm outline-none" value={renameValue} onChange={(event) => onRenameValue(event.target.value)} onKeyDown={(event) => { if (event.key === "Enter") onRename(item.id); }} /><Button className="px-2 py-1" variant="ghost" onClick={() => onRename(item.id)}>✓</Button></div> : <button className="block w-full text-left" onClick={() => onOpenSaved(item)}><span className="block truncate text-sm font-medium">{item.name}</span><span className="mt-1 block truncate font-mono text-xs text-carbon-text-3">{oneLine(item.sql)}</span></button>}
          <div className="mt-2 flex gap-2 text-xs"><button className="text-carbon-blue" onClick={() => onRenameStart(item)}>Rename</button><button className="text-carbon-red" onClick={() => onDelete(item.id)}>Delete</button></div>
        </div>)}
      </> : libraryTab === "history" ? <>
        <div className="flex justify-end border-b border-carbon-border p-2"><Button className="px-2 py-1" variant="ghost" disabled={!historyItems.length} onClick={onClearHistory}>Clear history</Button></div>
        {visibleHistory.length === 0 && <p className="p-4 text-xs text-carbon-text-3">No SQL history matches.</p>}
        {visibleHistory.map((item) => <button key={item.id} disabled={!item.sql} className="block w-full border-b border-carbon-border p-3 text-left hover:bg-carbon-layer2/60 disabled:cursor-not-allowed disabled:opacity-50" onClick={() => onOpenHistory(item)}>
          <span className="flex items-center justify-between gap-2"><span className="truncate text-sm">{item.summary}</span><Tag tone={item.status === "ok" ? "green" : item.status === "queued" ? "yellow" : "red"}>{item.status}</Tag></span>
          <span className="mt-1 block truncate font-mono text-xs text-carbon-text-3">{item.sql ? oneLine(item.sql) : "SQL was not stored by the previous console version"}</span>
          <span className="mt-1 block text-xs text-carbon-text-3">{new Date(item.at).toLocaleString()} · {item.elapsedMs} ms</span>
        </button>)}
      </> : <>
        <div className="flex items-center justify-between border-b border-carbon-border px-3 py-2">
          <span className="text-xs text-carbon-text-3">{tableCount} table{tableCount === 1 ? "" : "s"} · {viewCount} view{viewCount === 1 ? "" : "s"}</span>
          <Button className="px-2 py-1" variant="ghost" disabled={tablesBusy} onClick={onRefreshTables}>{tablesBusy ? "Loading…" : "Refresh"}</Button>
        </div>
        {tableError && <div className="border-b border-carbon-red/50 bg-carbon-red/10 p-3 text-xs text-carbon-text">{tableError}</div>}
        {tablesBusy && !tables.length && <div className="p-4"><Spinner label="Reading tables…" /></div>}
        {!tablesBusy && visibleTables.length === 0 && <p className="p-4 text-xs text-carbon-text-3">No tables or views match.</p>}
        {visibleTables.map((table) => {
          const expanded = expandedTables.includes(table.name);
          const loadingColumns = columnsBusy.includes(table.name);
          const columns = tableColumns[table.name] ?? [];
          return <div key={`${table.type}:${table.name}`} className="border-b border-carbon-border">
            <div className="flex items-center gap-1 px-2 py-2 hover:bg-carbon-layer2/60">
              <button className="flex min-w-0 flex-1 items-center gap-2 text-left" aria-expanded={expanded} onClick={() => onToggleTable(table)}>
                <span className="w-3 text-xs text-carbon-text-3">{expanded ? "▾" : "▸"}</span>
                <span className="min-w-0 flex-1 truncate font-mono text-sm">{table.name}</span>
                <Tag tone={table.type === "table" ? "green" : "gray"}>{table.type}</Tag>
              </button>
              <button className="px-1 text-xs text-carbon-blue hover:text-white" title={`Insert ${table.name}`} aria-label={`Insert table ${table.name}`} onClick={() => onInsertIdentifier(table.name)}>＋</button>
              <button className="px-1 text-xs text-carbon-text-3 hover:text-white" title={`Copy ${table.name}`} aria-label={`Copy table ${table.name}`} onClick={() => void copyIdentifier(table.name)}>{copied === table.name ? "✓" : "⧉"}</button>
            </div>
            {expanded && <div className="border-t border-carbon-border/60 bg-carbon-bg/40 py-1">
              {loadingColumns && <div className="px-4 py-3"><Spinner label="Reading columns…" /></div>}
              {!loadingColumns && columns.length === 0 && <p className="px-4 py-3 text-xs text-carbon-text-3">No columns reported.</p>}
              {columns.map((column) => <div key={`${column.cid}:${column.name}`} className="group flex items-center gap-2 py-1.5 pl-7 pr-2 hover:bg-carbon-layer2/60">
                <button className="min-w-0 flex-1 truncate text-left font-mono text-xs" title={`Insert ${column.name}`} onClick={() => onInsertIdentifier(column.name)}>{column.name}</button>
                {column.primaryKey > 0 && <span className="text-[10px] text-carbon-yellow">PK</span>}
                {column.hidden > 0 && <span className="text-[10px] text-carbon-text-3">hidden</span>}
                <span className="max-w-16 truncate text-[10px] uppercase text-carbon-text-3" title={`${column.type || "any"}${column.notNull ? " · not null" : ""}`}>{column.type || "any"}</span>
                <button className="invisible px-1 text-xs text-carbon-text-3 hover:text-white group-hover:visible focus:visible" title={`Copy ${column.name}`} aria-label={`Copy column ${column.name}`} onClick={() => void copyIdentifier(column.name)}>{copied === column.name ? "✓" : "⧉"}</button>
              </div>)}
            </div>}
          </div>;
        })}
      </>}
    </div>
    <p className="border-t border-carbon-border p-3 text-xs text-carbon-text-3">{libraryTab === "tables" ? "Click a table to see columns. Use ＋ to insert a safely quoted name." : "Stored only in this browser. Parameter values and data keys are never saved."}</p>
  </aside>;
}

function AdvancedOptions({
  analysis,
  atLeastLsn,
  consistency,
  exportFormat,
  exportLimit,
  targetReads,
  onConsistency,
  onExplain,
  onExport,
  onExportFormat,
  onExportLimit,
  onLsn,
  onTargetReads,
}: {
  analysis: Analysis;
  atLeastLsn: number;
  consistency: "linearizable" | "stale" | "at_least_lsn";
  exportFormat: "ndjson" | "csv";
  exportLimit: string;
  targetReads: boolean;
  onConsistency: (value: "linearizable" | "stale" | "at_least_lsn") => void;
  onExplain: () => void;
  onExport: () => void;
  onExportFormat: (value: "ndjson" | "csv") => void;
  onExportLimit: (value: string) => void;
  onLsn: (value: number) => void;
  onTargetReads: (value: boolean) => void;
}) {
  const reads = analysis.plan?.kind === "reads";
  const routed = reads ? targetReads || analysis.statements.some((statement) => statement.parameterCount > 0) : analysis.plan?.kind === "write" || analysis.plan?.kind === "transaction";
  return <div className="flex max-h-48 shrink-0 flex-wrap items-end gap-3 overflow-auto border-b border-carbon-border bg-carbon-layer px-4 py-3">
    {reads && <label className="flex items-center gap-2 pb-2 text-sm"><input type="checkbox" checked={targetReads} onChange={(event) => onTargetReads(event.target.checked)} />Target one data key</label>}
    {routed && <span className="pb-2 text-xs text-carbon-text-3">ShardLite uses the data key to locate the data automatically.</span>}
    {reads && routed && <div className="w-44"><Select label="Read freshness" value={consistency} onChange={(event) => onConsistency(event.target.value as typeof consistency)}><option value="linearizable">Current data</option><option value="at_least_lsn">At least an LSN</option><option value="stale">Allow older data</option></Select></div>}
    {reads && routed && consistency === "at_least_lsn" && <div className="w-32"><TextInput label="Minimum LSN" type="number" min={0} value={atLeastLsn} onChange={(event) => onLsn(Number(event.target.value))} /></div>}
    {reads && analysis.statements.length === 1 && routed && <>
      <Button variant="secondary" onClick={onExplain}>Explain</Button>
      <div className="w-28"><Select label="Export" value={exportFormat} onChange={(event) => onExportFormat(event.target.value as "ndjson" | "csv")}><option value="ndjson">NDJSON</option><option value="csv">CSV</option></Select></div>
      <div className="w-36"><Select label="Rows" value={exportLimit} onChange={(event) => onExportLimit(event.target.value)}><option value="10000">10,000</option><option value="100000">100,000</option><option value="1000000">1,000,000</option><option value="">No limit</option></Select></div>
      <Button variant="ghost" onClick={onExport}>Download</Button>
    </>}
    {!routed && <span className="pb-2 text-xs text-carbon-text-3">ShardLite will run reads across the database and handle placement automatically.</span>}
  </div>;
}

function ParamEditor({ expected, params, onChange }: { expected: number; params: ParamDraft[]; onChange: (value: ParamDraft[]) => void }) {
  return <div className="space-y-2 p-3">
    <div className="flex items-center justify-between gap-3"><span className="text-xs text-carbon-text-3">Add {expected} value{expected === 1 ? "" : "s"} in placeholder order. Values are not saved in history.</span><Button className="py-1" variant="ghost" onClick={() => onChange([...params, emptyParam()])}>Add value</Button></div>
    {params.map((param, index) => <div key={param.id} className="grid grid-cols-[3rem_9rem_minmax(10rem,1fr)_auto] items-end gap-2">
      <span className="pb-2 text-xs text-carbon-text-3">?{index + 1}</span>
      <Select label={index === 0 ? "Type" : undefined} value={param.type} onChange={(event) => onChange(params.map((item) => item.id === param.id ? { ...item, type: event.target.value as ParamType } : item))}>{["null", "integer", "real", "text", "boolean", "blob"].map((type) => <option key={type}>{type}</option>)}</Select>
      <TextInput label={index === 0 ? param.type === "blob" ? "Hex bytes" : "Value" : undefined} disabled={param.type === "null"} placeholder={param.type === "blob" ? "89504e47" : param.type === "boolean" ? "true" : ""} value={param.value} onChange={(event) => onChange(params.map((item) => item.id === param.id ? { ...item, value: event.target.value } : item))} />
      <Button variant="ghost" aria-label={`Remove parameter ${index + 1}`} onClick={() => onChange(params.filter((item) => item.id !== param.id))}>×</Button>
    </div>)}
  </div>;
}

function ResultsPane({ activeResult, busy, busyLabel, error, message, results, schemaReview, onActiveResult, onApplySchema, onConfirmSchema }: {
  activeResult: number | null;
  busy: boolean;
  busyLabel: string;
  error: string | null;
  message: Message | null;
  results: ResultSet[];
  schemaReview: SchemaReview | null;
  onActiveResult: (id: number) => void;
  onApplySchema: () => void;
  onConfirmSchema: (value: boolean) => void;
}) {
  const selected = results.find((item) => item.id === activeResult) ?? results[results.length - 1];
  return <>
    <div className="flex h-10 shrink-0 items-center gap-1 overflow-x-auto border-b border-carbon-border bg-carbon-layer px-2">
      <span className="px-2 text-xs font-semibold text-carbon-text-3">RESULTS</span>
      {results.map((item) => <button key={item.id} className={`h-full max-w-52 truncate border-b-2 px-3 text-xs ${item.id === selected?.id ? "border-carbon-blue text-carbon-text" : "border-transparent text-carbon-text-3"}`} onClick={() => onActiveResult(item.id)}>{item.title}</button>)}
    </div>
    <div className="min-h-0 flex-1 overflow-auto p-3">
      <div className="space-y-3">
        {busy && <Spinner label={busyLabel} />}
        {error && <Banner tone="error">{error}</Banner>}
        {message && <Banner tone={message.tone}>{message.text}{message.operation && <> <a className="text-carbon-blue underline" href="operations">Open Operations</a>.</>}</Banner>}
        {schemaReview && <SchemaReviewPanel review={schemaReview} onApply={onApplySchema} onConfirm={onConfirmSchema} busy={busy} />}
      </div>
      {selected ? <div className="mt-3 h-[calc(100%-0.75rem)] min-h-64"><ResultTable result={selected.result} /></div> : !busy && !error && !message && !schemaReview && <div className="grid h-full min-h-48 place-items-center text-sm text-carbon-text-3">Run SQL to see results here.</div>}
    </div>
  </>;
}

function SchemaReviewPanel({ review, onApply, onConfirm, busy }: { review: SchemaReview; onApply: () => void; onConfirm: (value: boolean) => void; busy: boolean }) {
  const versions = [...new Set(review.value.versions.map((item) => item.schema_version))];
  return <div className="border border-carbon-border bg-carbon-layer p-4">
    <div className="flex flex-wrap items-center justify-between gap-3"><div><h3 className="text-sm font-semibold">Review schema change</h3><p className="mt-1 text-xs text-carbon-text-3">The current schema was checked at {new Date(review.value.observed_at_ms).toLocaleTimeString()}.</p></div><Tag tone={versions.length === 1 ? "green" : "yellow"}>{versions.length === 1 ? "Ready to apply" : "Schema differs across the database"}</Tag></div>
    <pre className="mt-3 max-h-32 overflow-auto bg-carbon-field p-3 font-mono text-xs">{review.sql}</pre>
    <details className="mt-3 border border-carbon-border"><summary className="cursor-pointer px-3 py-2 text-xs font-medium">Technical details</summary><div className="border-t border-carbon-border p-3 font-mono text-xs text-carbon-text-3"><p>Database-wide preflight complete</p><p className="mt-1">SQL fingerprint {review.value.sql_fingerprint.slice(0, 20)}…</p></div></details>
    <label className="mt-3 flex items-start gap-2 text-sm"><input className="mt-1" type="checkbox" checked={review.confirmed} onChange={(event) => onConfirm(event.target.checked)} /><span>I reviewed this schema change and approve applying it to the database.</span></label>
    <Button className="mt-3" variant="danger" disabled={!review.confirmed || busy} onClick={onApply}>Apply schema change</Button>
  </div>;
}

function ResultTable({ result }: { result: api.QueryResult }) {
  const rowHeight = 34;
  const viewportHeight = 520;
  const overscan = 10;
  const [scrollTop, setScrollTop] = useState(0);
  const [widths, setWidths] = useState<Record<number, number>>({});
  const [copied, setCopied] = useState<string | null>(null);
  const start = Math.max(0, Math.floor(scrollTop / rowHeight) - overscan);
  const end = Math.min(result.rows.length, start + Math.ceil(viewportHeight / rowHeight) + overscan * 2);
  const columns = result.columns.length ? result.columns : ["(no columns)"];
  const template = columns.map((_, index) => `${widths[index] ?? 180}px`).join(" ");
  const width = columns.reduce((sum, _, index) => sum + (widths[index] ?? 180), 0);
  const copy = async (text: string, label: string) => { await navigator.clipboard.writeText(text); setCopied(label); window.setTimeout(() => setCopied(null), 1200); };
  return <div className="flex h-full min-h-0 flex-col gap-2">
    <div className="flex shrink-0 items-center justify-between text-xs text-carbon-text-3"><span>{result.rows.length} displayed row{result.rows.length === 1 ? "" : "s"}{result.truncated ? " · display cap reached" : ""}</span><span>{copied ?? "Click a cell to copy; use ↔ to resize."}</span></div>
    <div className="min-h-0 flex-1 overflow-auto border border-carbon-border" onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}>
      <div style={{ width }}>
        <div className="sticky top-0 z-10 grid h-9 bg-carbon-layer2" style={{ gridTemplateColumns: template }}>{columns.map((column, index) => <div key={`${column}-${index}`} className="flex items-center justify-between border-r border-carbon-border px-3 py-2 text-xs font-semibold"><span className="truncate">{column}</span><button title="Resize column" className="text-carbon-text-3" onClick={() => setWidths((current) => ({ ...current, [index]: current[index] === 280 ? 400 : current[index] === 400 ? 180 : 280 }))}>↔</button></div>)}</div>
        {result.rows.length === 0 ? <div className="px-4 py-8 text-center text-sm text-carbon-text-3">No rows</div> : <div className="relative" style={{ height: result.rows.length * rowHeight }}>{result.rows.slice(start, end).map((row, offset) => { const rowIndex = start + offset; return <div key={rowIndex} className="absolute left-0 grid border-b border-carbon-border hover:bg-carbon-layer2/50" style={{ top: rowIndex * rowHeight, height: rowHeight, gridTemplateColumns: template }}>{row.map((cell, column) => <button key={column} className="truncate border-r border-carbon-border px-3 py-2 text-left font-mono text-xs" title={formatCell(cell)} onClick={() => void copy(copyCell(cell), `Copied row ${rowIndex + 1}, column ${column + 1}`)}>{renderCell(cell)}</button>)}</div>; })}</div>}
      </div>
    </div>
    {result.rows.length > 0 && <Button className="self-start py-1" variant="ghost" onClick={() => void copy(result.rows.map((row) => row.map(copyCell).join("\t")).join("\n"), `Copied ${result.rows.length} displayed rows`)}>Copy displayed rows</Button>}
  </div>;
}

function typedParams(params: ParamDraft[]): unknown[] {
  return params.map((param, index) => {
    if (param.type === "null") return null;
    if (param.type === "text") return param.value;
    if (param.type === "boolean") {
      if (!/^(true|false)$/i.test(param.value)) throw new Error(`parameter ${index + 1} must be true or false`);
      return param.value.toLowerCase() === "true";
    }
    if (param.type === "blob") {
      if (!/^(?:[0-9a-fA-F]{2})*$/.test(param.value)) throw new Error(`parameter ${index + 1} blob must use an even number of hex digits`);
      return { blob_hex: param.value };
    }
    const value = Number(param.value);
    if (!Number.isFinite(value)) throw new Error(`parameter ${index + 1} must be a finite number`);
    if (param.type === "integer" && (!Number.isSafeInteger(value) || !/^-?\d+$/.test(param.value))) throw new Error(`parameter ${index + 1} must be a safe integer`);
    return value;
  });
}

function readSaved(key: string): SavedQuery[] { return normalizeSaved(readJson(key)); }
function readHistory(key: string): HistoryItem[] { return normalizeHistory(readJson(key)); }

function toColumnReference(row: unknown[]): ColumnReference {
  return {
    cid: Number(row[0]),
    name: String(row[1]),
    type: row[2] === null ? "" : String(row[2]),
    notNull: Number(row[3]) !== 0,
    primaryKey: Number(row[5]),
    hidden: Number(row[6]),
  };
}

function databaseError(caught: unknown, fallback: string): string {
  const message = caught instanceof Error ? caught.message : fallback;
  return message
    .replace(/\bshard\s+\d+\b/gi, "part of the database")
    .replace(/\bshards?\b/gi, "database storage")
    .replace(/\s+/g, " ")
    .trim();
}

function readJson(key: string): unknown { try { const value = localStorage.getItem(key); return value ? JSON.parse(value) : null; } catch { return null; } }
function readText(key: string, fallback: string): string { try { return localStorage.getItem(key) ?? fallback; } catch { return fallback; } }
function readNumber(key: string, fallback: number): number { try { const value = Number(localStorage.getItem(key)); return Number.isFinite(value) && value > 0 ? value : fallback; } catch { return fallback; } }
function readBoolean(key: string, fallback: boolean): boolean { const value = readJson(key); return typeof value === "boolean" ? value : fallback; }
function clamp(value: number, min: number, max: number) { return Math.min(max, Math.max(min, value)); }
function consistencyValue(value: "linearizable" | "stale" | "at_least_lsn", lsn: number): api.ReadConsistency { return value === "at_least_lsn" ? { at_least_lsn: lsn } : value; }
function oneLine(value: string) { return value.replace(/\s+/g, " ").trim() || "Empty SQL"; }
function quoteIdentifier(value: string) { return `"${value.replace(/"/g, '""')}"`; }
function resultTitle(index: number, statement: SqlStatement) { return `${index + 1}: ${statement.keyword || "Result"}`; }
function planSummary(plan: ExecutionPlan) { if (plan.kind === "reads") return `${plan.statements.length} read${plan.statements.length === 1 ? "" : "s"}`; if (plan.kind === "write") return "Data change"; if (plan.kind === "transaction") return `${plan.statements.length}-statement transaction`; return "Schema change"; }
function planLabel(plan: ExecutionPlan) { if (plan.kind === "reads") return plan.statements.length === 1 ? "Read" : `${plan.statements.length} reads`; if (plan.kind === "write") return "Data change"; if (plan.kind === "transaction") return `${plan.statements.length} writes · atomic`; return "Schema change"; }
function planTone(plan: ExecutionPlan | null): "gray" | "blue" | "green" | "red" | "yellow" { if (!plan) return "red"; if (plan.kind === "reads") return "blue"; if (plan.kind === "schema") return "yellow"; return "green"; }
function LibraryTab({ active, onClick, children }: { active: boolean; onClick: () => void; children: React.ReactNode }) { return <button className={`border-b-2 px-3 py-2 text-sm ${active ? "border-carbon-blue text-carbon-text" : "border-transparent text-carbon-text-3"}`} onClick={onClick}>{children}</button>; }
function renderCell(value: unknown) { if (value === null) return <span className="italic text-carbon-text-3">NULL</span>; if (isBlob(value)) return <span className="text-carbon-yellow">BLOB · {value.length} bytes · {hexPreview(value)}</span>; return formatCell(value); }
function isBlob(value: unknown): value is number[] { return Array.isArray(value) && value.every((item) => Number.isInteger(item) && item >= 0 && item <= 255); }
function hexPreview(value: number[]) { const hex = value.slice(0, 12).map((item) => item.toString(16).padStart(2, "0")).join(""); return `${hex}${value.length > 12 ? "…" : ""}`; }
function formatCell(value: unknown): string { if (value === null) return "NULL"; if (isBlob(value)) return `<blob ${value.length} bytes: ${hexPreview(value)}>`; if (typeof value === "object") return JSON.stringify(value); return String(value); }
function copyCell(value: unknown): string { if (value === null) return "NULL"; if (isBlob(value)) return value.map((item) => item.toString(16).padStart(2, "0")).join(""); return typeof value === "object" ? JSON.stringify(value) : String(value); }
