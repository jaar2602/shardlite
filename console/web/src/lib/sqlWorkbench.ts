export type StatementKind = "read" | "write" | "schema" | "control" | "unknown";
export type RunTarget = "current" | "selection" | "all";

export interface SqlStatement {
  sql: string;
  from: number;
  to: number;
  kind: StatementKind;
  keyword: string;
  parameterCount: number;
}

export type ExecutionPlan =
  | { kind: "reads"; statements: SqlStatement[] }
  | { kind: "write"; statements: [SqlStatement] }
  | { kind: "transaction"; statements: SqlStatement[] }
  | { kind: "schema"; statements: [SqlStatement] };

export interface WorkbenchDriver<ReadResult, ChangeResult, SchemaResult> {
  route: () => Promise<number>;
  queryAll: (sql: string) => Promise<ReadResult>;
  query: (sql: string, shard: number, params: unknown[]) => Promise<ReadResult>;
  execute: (sql: string, shard: number, params: unknown[]) => Promise<ChangeResult>;
  transaction: (statements: { sql: string; params: unknown[] }[], shard: number) => Promise<ChangeResult>;
  preflight: (sql: string) => Promise<SchemaResult>;
}

export type DispatchResult<ReadResult, ChangeResult, SchemaResult> =
  | { kind: "reads"; values: ReadResult[] }
  | { kind: "write" | "transaction"; value: ChangeResult }
  | { kind: "schema"; value: SchemaResult };

type Word = { value: string; depth: number };

const READ = new Set(["SELECT", "VALUES", "EXPLAIN", "PRAGMA"]);
const WRITE = new Set(["INSERT", "UPDATE", "DELETE", "REPLACE"]);
const SCHEMA = new Set(["CREATE", "ALTER", "DROP", "REINDEX", "ANALYZE", "VACUUM", "ATTACH", "DETACH"]);
const CONTROL = new Set(["BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE"]);

export function splitStatements(source: string, offset = 0): SqlStatement[] {
  const statements: SqlStatement[] = [];
  let start = 0;
  let index = 0;
  let mode: "normal" | "single" | "double" | "backtick" | "bracket" | "line" | "block" = "normal";
  let trigger = false;
  let triggerBody = false;
  let triggerCaseDepth = 0;
  let lastWord = "";
  let lastWordClosesCase = false;
  const leadingWords: string[] = [];

  const reset = () => {
    trigger = false;
    triggerBody = false;
    triggerCaseDepth = 0;
    lastWord = "";
    lastWordClosesCase = false;
    leadingWords.length = 0;
  };
  const push = (end: number) => {
    const [from, to] = trimRange(source, start, end);
    if (from < to && meaningful(source.slice(from, to))) {
      const sql = source.slice(from, to);
      const classified = classifyStatement(sql);
      statements.push({ sql, from: from + offset, to: to + offset, ...classified, parameterCount: countParameters(sql) });
    }
    start = end;
    reset();
  };

  while (index < source.length) {
    const char = source[index];
    const next = source[index + 1];
    if (mode === "line") {
      if (char === "\n") mode = "normal";
      index += 1;
      continue;
    }
    if (mode === "block") {
      if (char === "*" && next === "/") { mode = "normal"; index += 2; } else index += 1;
      continue;
    }
    if (mode === "single" || mode === "double" || mode === "backtick") {
      const quote = mode === "single" ? "'" : mode === "double" ? '"' : "`";
      if (char === quote && next === quote) { index += 2; continue; }
      if (char === quote) mode = "normal";
      index += 1;
      continue;
    }
    if (mode === "bracket") {
      if (char === "]" && next === "]") { index += 2; continue; }
      if (char === "]") mode = "normal";
      index += 1;
      continue;
    }
    if (char === "-" && next === "-") { mode = "line"; index += 2; continue; }
    if (char === "/" && next === "*") { mode = "block"; index += 2; continue; }
    if (char === "'") { mode = "single"; index += 1; continue; }
    if (char === '"') { mode = "double"; index += 1; continue; }
    if (char === "`") { mode = "backtick"; index += 1; continue; }
    if (char === "[") { mode = "bracket"; index += 1; continue; }

    if (/[A-Za-z_]/.test(char)) {
      const wordStart = index;
      index += 1;
      while (index < source.length && /[A-Za-z0-9_$]/.test(source[index])) index += 1;
      const word = source.slice(wordStart, index).toUpperCase();
      lastWordClosesCase = false;
      if (leadingWords.length < 4) leadingWords.push(word);
      trigger = isCreateTrigger(leadingWords);
      if (trigger) {
        if (!triggerBody && word === "BEGIN") triggerBody = true;
        else if (triggerBody && word === "CASE") triggerCaseDepth += 1;
        else if (triggerBody && word === "END" && triggerCaseDepth > 0) {
          triggerCaseDepth -= 1;
          lastWordClosesCase = true;
        }
      }
      lastWord = word;
      continue;
    }

    if (char === ";") {
      const triggerEnd = trigger && triggerBody && lastWord === "END" && triggerCaseDepth === 0 && !lastWordClosesCase;
      if (!triggerBody || triggerEnd) push(index + 1);
    }
    index += 1;
  }
  push(source.length);
  return statements;
}

export function classifyStatement(sql: string): { kind: StatementKind; keyword: string } {
  const words = wordsOutsideLiterals(sql);
  let keyword = words[0]?.value ?? "";
  if (keyword === "WITH") {
    keyword = words.find((word, index) => index > 0 && word.depth === 0 && (READ.has(word.value) || WRITE.has(word.value)))?.value ?? "WITH";
  }
  if (READ.has(keyword)) return { kind: "read", keyword };
  if (WRITE.has(keyword)) return { kind: "write", keyword };
  if (SCHEMA.has(keyword)) return { kind: "schema", keyword };
  if (CONTROL.has(keyword)) return { kind: "control", keyword };
  return { kind: "unknown", keyword };
}

export function statementsForTarget(
  source: string,
  target: RunTarget,
  selection: { from: number; to: number },
): SqlStatement[] {
  if (target === "all") return splitStatements(source);
  if (target === "selection") {
    if (selection.from === selection.to) throw new Error("Select SQL before using Run selection.");
    const from = Math.min(selection.from, selection.to);
    const to = Math.max(selection.from, selection.to);
    return splitStatements(source.slice(from, to), from);
  }
  const statements = splitStatements(source);
  if (!statements.length) return [];
  const cursor = selection.to;
  return [
    statements.find((statement) => cursor >= statement.from && cursor <= statement.to)
      ?? statements.find((statement) => statement.from > cursor)
      ?? statements[statements.length - 1],
  ];
}

export function buildExecutionPlan(statements: SqlStatement[]): ExecutionPlan {
  if (!statements.length) throw new Error("Enter a SQL statement to run.");
  const control = statements.find((statement) => statement.kind === "control");
  if (control) throw new Error(`${control.keyword} is not run directly. Select the write statements and use Run all for an atomic transaction.`);
  const unknown = statements.find((statement) => statement.kind === "unknown");
  if (unknown) throw new Error(`Cannot safely classify statement${unknown.keyword ? ` starting with ${unknown.keyword}` : ""}. Run a supported read, write, or schema statement.`);
  const kinds = new Set(statements.map((statement) => statement.kind));
  if (kinds.size !== 1) throw new Error("Mixed read, write, and schema scripts are not run together. Select one category and run it separately.");
  const kind = statements[0].kind;
  if (kind === "read") return { kind: "reads", statements };
  if (kind === "write") return statements.length === 1
    ? { kind: "write", statements: [statements[0]] }
    : { kind: "transaction", statements };
  if (statements.length !== 1) throw new Error("Apply schema statements one at a time so every change receives its own safety check and operation record.");
  return { kind: "schema", statements: [statements[0]] };
}

export async function dispatchExecutionPlan<ReadResult, ChangeResult, SchemaResult>(
  plan: ExecutionPlan,
  params: unknown[][],
  targetedReads: boolean,
  driver: WorkbenchDriver<ReadResult, ChangeResult, SchemaResult>,
  onRead?: (value: ReadResult, statement: SqlStatement, index: number) => void,
): Promise<DispatchResult<ReadResult, ChangeResult, SchemaResult>> {
  if (params.length !== plan.statements.length) throw new Error("internal parameter grouping mismatch");
  if (plan.kind === "reads") {
    const shard = targetedReads ? await driver.route() : null;
    const values: ReadResult[] = [];
    for (let index = 0; index < plan.statements.length; index += 1) {
      const statement = plan.statements[index];
      const value = shard === null
        ? await driver.queryAll(statement.sql)
        : await driver.query(statement.sql, shard, params[index]);
      values.push(value);
      onRead?.(value, statement, index);
    }
    return { kind: "reads", values };
  }
  if (plan.kind === "write") {
    const shard = await driver.route();
    return { kind: "write", value: await driver.execute(plan.statements[0].sql, shard, params[0]) };
  }
  if (plan.kind === "transaction") {
    const shard = await driver.route();
    return {
      kind: "transaction",
      value: await driver.transaction(plan.statements.map((statement, index) => ({ sql: statement.sql, params: params[index] })), shard),
    };
  }
  return { kind: "schema", value: await driver.preflight(plan.statements[0].sql) };
}

export function countParameters(sql: string): number {
  let count = 0;
  scanNormal(sql, (char) => { if (char === "?") count += 1; });
  return count;
}

function isCreateTrigger(words: string[]): boolean {
  if (words[0] !== "CREATE") return false;
  const rest = words.slice(1).filter((word) => word !== "TEMP" && word !== "TEMPORARY");
  return rest[0] === "TRIGGER";
}

function trimRange(source: string, from: number, to: number): [number, number] {
  while (from < to && /\s/.test(source[from])) from += 1;
  while (to > from && /\s/.test(source[to - 1])) to -= 1;
  return [from, to];
}

function meaningful(sql: string): boolean {
  return wordsOutsideLiterals(sql).length > 0;
}

function wordsOutsideLiterals(sql: string): Word[] {
  const words: Word[] = [];
  let depth = 0;
  scanNormal(sql, (char, index) => {
    if (char === "(") { depth += 1; return; }
    if (char === ")") { depth = Math.max(0, depth - 1); return; }
    if (!/[A-Za-z_]/.test(char)) return;
    if (index > 0 && /[A-Za-z0-9_$]/.test(sql[index - 1])) return;
    let end = index + 1;
    while (end < sql.length && /[A-Za-z0-9_$]/.test(sql[end])) end += 1;
    words.push({ value: sql.slice(index, end).toUpperCase(), depth });
  });
  return words;
}

function scanNormal(sql: string, visit: (char: string, index: number) => void): void {
  let index = 0;
  let mode: "normal" | "single" | "double" | "backtick" | "bracket" | "line" | "block" = "normal";
  while (index < sql.length) {
    const char = sql[index];
    const next = sql[index + 1];
    if (mode === "line") { if (char === "\n") mode = "normal"; index += 1; continue; }
    if (mode === "block") { if (char === "*" && next === "/") { mode = "normal"; index += 2; } else index += 1; continue; }
    if (mode === "single" || mode === "double" || mode === "backtick") {
      const quote = mode === "single" ? "'" : mode === "double" ? '"' : "`";
      if (char === quote && next === quote) { index += 2; continue; }
      if (char === quote) mode = "normal";
      index += 1;
      continue;
    }
    if (mode === "bracket") { if (char === "]" && next === "]") index += 2; else { if (char === "]") mode = "normal"; index += 1; } continue; }
    if (char === "-" && next === "-") { mode = "line"; index += 2; continue; }
    if (char === "/" && next === "*") { mode = "block"; index += 2; continue; }
    if (char === "'") { mode = "single"; index += 1; continue; }
    if (char === '"') { mode = "double"; index += 1; continue; }
    if (char === "`") { mode = "backtick"; index += 1; continue; }
    if (char === "[") { mode = "bracket"; index += 1; continue; }
    visit(char, index);
    index += 1;
  }
}
