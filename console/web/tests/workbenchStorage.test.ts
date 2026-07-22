import assert from "node:assert/strict";
import test from "node:test";
import { normalizeHistory, normalizeSaved, removeLegacyPlacementSettings } from "../src/lib/workbenchStorage.ts";

test("keeps existing saved SQL and supplies missing update timestamps", () => {
  assert.deepEqual(normalizeSaved([{ id: 12, name: "Accounts", sql: "SELECT * FROM accounts" }]), [{
    id: 12,
    name: "Accounts",
    sql: "SELECT * FROM accounts",
    updatedAt: 12,
  }]);
});

test("migrates metadata-only history without inventing SQL", () => {
  assert.deepEqual(normalizeHistory([{
    id: 20,
    at: 21,
    mode: "query_all",
    target: "database",
    status: "ok",
    elapsedMs: 9,
    rowCount: 3,
  }]), [{
    id: 20,
    at: 21,
    sql: "",
    summary: "query all · database",
    status: "ok",
    elapsedMs: 9,
    rowCount: 3,
  }]);
});

test("retains new browser-local SQL history and drops invalid entries", () => {
  const result = normalizeHistory([
    { id: 1, at: 2, sql: "SELECT 1", summary: "Read", status: "ok", elapsedMs: 3 },
    { id: "bad", at: 2 },
  ]);
  assert.equal(result.length, 1);
  assert.equal(result[0].sql, "SELECT 1");
});

test("removes only obsolete physical-placement settings", () => {
  const removed: string[] = [];
  removeLegacyPlacementSettings({ removeItem: (key: string) => { removed.push(key); } }, "meshdb.workbench.alice.prod");
  assert.deepEqual(removed, [
    "meshdb.workbench.alice.prod.routeTarget",
    "meshdb.workbench.alice.prod.shard",
    "meshdb.workbench.alice.prod.placement",
  ]);
});
