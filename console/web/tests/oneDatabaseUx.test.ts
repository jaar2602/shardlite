import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const source = (path: string) => readFileSync(new URL(`../src/${path}`, import.meta.url), "utf8");

test("normal navigation exposes one database and isolates storage internals", () => {
  const app = source("App.tsx");
  assert.doesNotMatch(app, /label:\s*"Shards"/);
  assert.match(app, /Storage internals/);
  assert.match(app, /permission:\s*"operate"/);
});

test("SQL and schema workflows have no physical placement controls", () => {
  const editor = source("views/SqlEditor.tsx");
  const schema = source("views/Schema.tsx");
  assert.doesNotMatch(editor, /Physical shard|Reference shard|routeTarget/);
  assert.doesNotMatch(schema, /Reference shard|Physical shard/);
  assert.match(editor, /tenant, customer, account/);
  assert.match(schema, /schemaCatalog/);
});

test("database overview and topology avoid shard-facing language", () => {
  const overview = source("views/Overview.tsx");
  const topology = source("components/TopologyMap.tsx");
  assert.doesNotMatch(overview, />Shards?</);
  assert.doesNotMatch(topology, /primary shards|shard ownership/i);
  assert.match(topology, /ONE SHARDLITE DATABASE/);
});
