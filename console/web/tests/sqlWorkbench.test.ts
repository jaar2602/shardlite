import assert from "node:assert/strict";
import test from "node:test";
import { buildExecutionPlan, classifyStatement, countParameters, dispatchExecutionPlan, splitStatements, statementsForTarget } from "../src/lib/sqlWorkbench.ts";

test("splits ordinary SQL without splitting strings or comments", () => {
  const statements = splitStatements("SELECT ';' AS value; -- ; ignored\nUPDATE t SET v = 'a;b';");
  assert.equal(statements.length, 2);
  assert.equal(statements[0].kind, "read");
  assert.equal(statements[1].kind, "write");
});

test("keeps a trigger body together", () => {
  const sql = "CREATE TRIGGER tr AFTER INSERT ON t BEGIN UPDATE t SET v = CASE WHEN v = 1 THEN 2 ELSE 3 END; INSERT INTO log VALUES ('a;b'); END; SELECT 1;";
  const statements = splitStatements(sql);
  assert.equal(statements.length, 2);
  assert.match(statements[0].sql, /INSERT INTO log/);
  assert.equal(statements[0].kind, "schema");
  assert.equal(statements[1].kind, "read");
});

test("classifies CTE reads and writes by their top-level operation", () => {
  assert.equal(classifyStatement("WITH x AS (SELECT 1) SELECT * FROM x").kind, "read");
  assert.equal(classifyStatement("WITH x AS (SELECT 1) UPDATE t SET v = 2").kind, "write");
});

test("counts only parameters outside literals and comments", () => {
  assert.equal(countParameters("SELECT ?, '?', \"?\" -- ?\n WHERE id = ?12"), 2);
});

test("selects the statement at the cursor and exact selected SQL", () => {
  const sql = "SELECT 1;\nUPDATE t SET v = 2;";
  assert.equal(statementsForTarget(sql, "current", { from: 20, to: 20 })[0].kind, "write");
  assert.equal(statementsForTarget(sql, "selection", { from: 0, to: 8 })[0].sql, "SELECT 1");
});

test("builds safe endpoint-level execution plans", () => {
  assert.equal(buildExecutionPlan(splitStatements("SELECT 1; SELECT 2;")).kind, "reads");
  assert.equal(buildExecutionPlan(splitStatements("INSERT INTO t VALUES (1);")).kind, "write");
  assert.equal(buildExecutionPlan(splitStatements("INSERT INTO t VALUES (1); UPDATE t SET v = 2;")).kind, "transaction");
  assert.equal(buildExecutionPlan(splitStatements("CREATE TABLE t (id INTEGER);")).kind, "schema");
  assert.throws(() => buildExecutionPlan(splitStatements("SELECT 1; UPDATE t SET v = 2;")), /Mixed/);
  assert.throws(() => buildExecutionPlan(splitStatements("BEGIN;")), /not run directly/);
  assert.throws(() => buildExecutionPlan(splitStatements("CREATE TABLE a (id); CREATE TABLE b (id);")), /one at a time/);
});

test("dispatches plans through the matching client endpoints", async () => {
  const calls: string[] = [];
  const driver = {
    route: async () => { calls.push("route"); return 7; },
    queryAll: async (sql: string) => { calls.push(`query_all:${sql}`); return sql; },
    query: async (sql: string, shard: number) => { calls.push(`query:${shard}:${sql}`); return sql; },
    execute: async (sql: string, shard: number) => { calls.push(`execute:${shard}:${sql}`); return { rows: 1 }; },
    run: async (sql: string) => { calls.push(`run:${sql}`); return { rows: 1 }; },
    transaction: async (statements: { sql: string }[], shard: number) => { calls.push(`tx:${shard}:${statements.length}`); return { rows: statements.length }; },
    preflight: async (sql: string) => { calls.push(`preflight:${sql}`); return { checked: true }; },
  };

  let plan = buildExecutionPlan(splitStatements("SELECT 1; SELECT 2;"));
  await dispatchExecutionPlan(plan, [[], []], false, driver);
  assert.deepEqual(calls.splice(0), ["query_all:SELECT 1;", "query_all:SELECT 2;"]);

  await dispatchExecutionPlan(plan, [[], []], true, driver);
  assert.deepEqual(calls.splice(0), ["route", "query:7:SELECT 1;", "query:7:SELECT 2;"]);

  // A param-less write auto-routes through the server (POST /v1/run) — no data key needed.
  plan = buildExecutionPlan(splitStatements("UPDATE t SET v = 1;"));
  await dispatchExecutionPlan(plan, [[]], false, driver);
  assert.deepEqual(calls.splice(0), ["run:UPDATE t SET v = 1;"]);

  // A write WITH bound parameters cannot be auto-routed (the router reads only SQL text), so it
  // falls back to an explicit route + execute.
  await dispatchExecutionPlan(plan, [[42]], false, driver);
  assert.deepEqual(calls.splice(0), ["route", "execute:7:UPDATE t SET v = 1;"]);

  plan = buildExecutionPlan(splitStatements("INSERT INTO t VALUES (1); UPDATE t SET v = 2;"));
  await dispatchExecutionPlan(plan, [[], []], false, driver);
  assert.deepEqual(calls.splice(0), ["route", "tx:7:2"]);

  plan = buildExecutionPlan(splitStatements("CREATE TABLE t (id);"));
  await dispatchExecutionPlan(plan, [[]], false, driver);
  assert.deepEqual(calls.splice(0), ["preflight:CREATE TABLE t (id);"]);
});
