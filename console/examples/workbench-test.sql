-- meshdb console workbench test kit
--
-- This is a runbook, not one batch. Copy one statement at a time into the mode named by each
-- section. Start on a disposable cluster. Names are prefixed with console_test_ so cleanup is
-- predictable.
--
-- The workbench uses one editor. Run the statement at the cursor, select SQL, or run the complete
-- document. MeshDB recognizes reads, keyed writes, atomic write groups, and schema changes.
-- Physical shard controls appear only in Options and are needed only by the optional diagnostic
-- sections near the end of this file.


-- -----------------------------------------------------------------------------
-- 1. SCHEMA — run, review, and apply each statement separately
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS console_test_accounts (
    account_id TEXT PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    balance_cents INTEGER NOT NULL DEFAULT 0 CHECK (balance_cents >= 0),
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    profile BLOB,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
) STRICT;

CREATE TABLE IF NOT EXISTS console_test_events (
    event_id INTEGER PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES console_test_accounts(account_id),
    kind TEXT NOT NULL CHECK (kind IN ('credit', 'debit', 'note')),
    amount_cents INTEGER,
    metadata TEXT CHECK (metadata IS NULL OR json_valid(metadata)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
) STRICT;

CREATE INDEX IF NOT EXISTS console_test_events_account_time_idx
ON console_test_events(account_id, created_at DESC);

CREATE TRIGGER IF NOT EXISTS console_test_apply_credit
AFTER INSERT ON console_test_events
WHEN NEW.kind = 'credit'
BEGIN
    UPDATE console_test_accounts
       SET balance_cents = balance_cents + COALESCE(NEW.amount_cents, 0)
     WHERE account_id = NEW.account_id;
END;

CREATE VIEW IF NOT EXISTS console_test_active_accounts AS
SELECT account_id, email, balance_cents, created_at
  FROM console_test_accounts
 WHERE active = 1;

-- Optional export fixture. Create it only when testing larger downloads.
CREATE TABLE IF NOT EXISTS console_test_export (
    id INTEGER PRIMARY KEY,
    payload TEXT NOT NULL
) STRICT;


-- -----------------------------------------------------------------------------
-- 2. DATA KEY + TYPED PARAMETERS — run the statement at the cursor
-- -----------------------------------------------------------------------------
-- Enter Data key: acct:alice. MeshDB resolves placement automatically.
-- SQL parameters, in order:
--   1 text     acct:alice
--   2 text     alice@example.test
--   3 integer  10000
--   4 boolean  true
--   5 blob     89504e470d0a1a0a

INSERT INTO console_test_accounts
    (account_id, email, balance_cents, active, profile)
VALUES (?, ?, ?, ?, ?);

-- Repeat with other data keys. Change the bound values too:
--   acct:bob   / bob@example.test   / 2500  / true  / deadbeef
--   acct:carol / carol@example.test / 0     / false / (empty hex blob)
-- The data key and account_id should match. SQL does not infer the distribution key from a
-- column value.


-- -----------------------------------------------------------------------------
-- 3. ATOMIC TRANSACTION — select both statements and Run Selection, Data key acct:alice
-- -----------------------------------------------------------------------------
-- Add these as two separate transaction statements. The trigger should raise Alice's balance
-- from 10000 to 12500, and both statements commit or neither does.

-- Statement 1 parameters: integer 1001, text acct:alice, text credit, integer 2500,
--                         text {"source":"phase3-test"}
INSERT INTO console_test_events
    (event_id, account_id, kind, amount_cents, metadata)
VALUES (?, ?, ?, ?, ?);

-- Statement 2 parameters: text alice+updated@example.test, text acct:alice
UPDATE console_test_accounts
   SET email = ?
 WHERE account_id = ?;


-- -----------------------------------------------------------------------------
-- 4. TARGETED QUERY + FRESHNESS — Options → Target one data key, acct:alice
-- -----------------------------------------------------------------------------
-- Run this once with linearizable, once with stale allowed, and once with at-least-LSN = 0.

SELECT account_id,
       email,
       balance_cents,
       active,
       profile,
       created_at
  FROM console_test_accounts
 WHERE account_id = ?;

-- Parameter: text acct:alice

-- Confirm trigger output and JSON data.
SELECT e.event_id,
       e.kind,
       e.amount_cents,
       json_extract(e.metadata, '$.source') AS source,
       a.balance_cents
  FROM console_test_events AS e
  JOIN console_test_accounts AS a USING (account_id)
 WHERE e.account_id = ?
 ORDER BY e.event_id;

-- Parameter: text acct:alice


-- -----------------------------------------------------------------------------
-- 5. TYPED VALUE AND RESULT RENDERING — Options → Target one data key, render:test
-- -----------------------------------------------------------------------------
-- Parameters: null, integer 42, real 3.14159, text hello, boolean true,
--             blob 00ff102030

SELECT ? AS null_value,
       ? AS integer_value,
       ? AS real_value,
       ? AS text_value,
       ? AS boolean_value,
       ? AS blob_value;

-- Blob helper. Bind blob cafebabe.
SELECT hex(?) AS blob_hex, length(?) AS blob_bytes;
-- Bind the same blob twice because both ? placeholders are positional.


-- -----------------------------------------------------------------------------
-- 6. EXPLAIN QUERY PLAN — Options → Target one data key
-- -----------------------------------------------------------------------------
-- Data key acct:alice. Parameter: text alice+updated@example.test. Use Explain query.

SELECT account_id, balance_cents
  FROM console_test_accounts
 WHERE email = ?;

-- The plan should use the UNIQUE index SQLite created for email.


-- -----------------------------------------------------------------------------
-- 7. DATABASE-WIDE READS — default execution, with no bound parameters
-- -----------------------------------------------------------------------------
-- MeshDB plans and merges these reads across the database. Populate two or more data keys first
-- for more interesting results.

SELECT COUNT(*) AS account_count,
       COALESCE(SUM(balance_cents), 0) AS total_balance_cents
  FROM console_test_accounts;

SELECT active,
       COUNT(*) AS accounts,
       COALESCE(SUM(balance_cents), 0) AS balance_cents
  FROM console_test_accounts
 GROUP BY active
 ORDER BY active;

SELECT account_id, email, balance_cents
  FROM console_test_accounts
 ORDER BY balance_cents DESC, account_id
 LIMIT 10;

SELECT DISTINCT kind
  FROM console_test_events
 ORDER BY kind;

SELECT account_id
  FROM console_test_accounts
UNION
SELECT account_id
  FROM console_test_events
ORDER BY account_id;


-- -----------------------------------------------------------------------------
-- 8. EXPECTED FAILURES — use a disposable test cluster
-- -----------------------------------------------------------------------------
-- Run with Data key acct:alice. This should be rejected by the UNIQUE constraint and leave
-- the existing row unchanged. Bind: text acct:alice, text duplicate@example.test.

INSERT INTO console_test_accounts(account_id, email)
VALUES (?, ?);

-- Transaction rollback test, Data key acct:rollback. Select both statements and Run Selection.
-- Statement 1 binds: text acct:rollback, text rollback@example.test
INSERT INTO console_test_accounts(account_id, email)
VALUES (?, ?);

-- Statement 2 intentionally binds the same account_id again and should reject the transaction.
-- Bind: text acct:rollback, text another@example.test
INSERT INTO console_test_accounts(account_id, email)
VALUES (?, ?);

-- Verify rollback with Options → Target one data key, acct:rollback. Bind text acct:rollback;
-- expect 0.
SELECT COUNT(*) AS rows_after_failed_transaction
  FROM console_test_accounts
 WHERE account_id = ?;

-- Mixed-script safety test: select a read and this write together. The workbench must refuse it.
UPDATE console_test_accounts SET active = 0;


-- -----------------------------------------------------------------------------
-- 9. OPTIONAL LARGE STREAM/EXPORT TEST — use Options → Physical shard 0 (expert)
-- -----------------------------------------------------------------------------
-- This inserts 50,000 rows on one shard. Do not run it on a production cluster.

WITH RECURSIVE sequence(value) AS (
    SELECT 1
    UNION ALL
    SELECT value + 1 FROM sequence WHERE value < 50000
)
INSERT INTO console_test_export(id, payload)
SELECT value, printf('payload-%06d', value)
  FROM sequence;

-- Use Options → Physical shard 0. The display should stop at its bounded cap.
SELECT id, payload
  FROM console_test_export
 ORDER BY id;

-- Use Streamed export on the same SELECT for both CSV and NDJSON. Test a 10,000-row limit and
-- an uncapped download. Cancelling the browser download should close the upstream request.


-- -----------------------------------------------------------------------------
-- 10. OPTIONAL SCHEMA-DRIFT + PARTIAL-ROLLOUT TEST — multi-shard disposable cluster only
-- -----------------------------------------------------------------------------
-- First use Options → Physical shard 0. This intentionally creates schema drift.

CREATE INDEX console_test_events_drift_idx
ON console_test_events(kind, event_id);

-- In the Schema screen, Compare all shards should name shard 0 as the only shard with the index.
-- Query-all should refuse while schema versions disagree.

-- To exercise explicit PARTIAL reporting, run, review, and apply this exact statement
-- without IF NOT EXISTS. Shard 0 rejects it as already existing; other shards apply it. The
-- durable operation must finish PARTIAL and list shard 0's error instead of claiming success.

CREATE INDEX console_test_events_drift_idx
ON console_test_events(kind, event_id);

-- Compare all shards again. Definitions may now agree even though the operation was correctly
-- recorded as partial. This is why operation outcome and current schema state are separate facts.


-- -----------------------------------------------------------------------------
-- 11. STALE-APPROVAL TEST — two browser tabs, disposable cluster
-- -----------------------------------------------------------------------------
-- Tab A: run this statement and complete the schema check, but do not apply it.

CREATE INDEX IF NOT EXISTS console_test_accounts_balance_idx
ON console_test_accounts(balance_cents DESC);

-- Tab B: apply this different rollout, changing every schema version.

CREATE INDEX IF NOT EXISTS console_test_events_kind_idx
ON console_test_events(kind);

-- Return to Tab A and queue its already-approved operation. The worker must fail it before
-- execute_all with "schema changed after approval". Run a new preflight to approve the new state.


-- -----------------------------------------------------------------------------
-- 12. CLEANUP — run, review, and apply one statement at a time
-- -----------------------------------------------------------------------------

DROP VIEW IF EXISTS console_test_active_accounts;

DROP TRIGGER IF EXISTS console_test_apply_credit;

DROP TABLE IF EXISTS console_test_events;

DROP TABLE IF EXISTS console_test_accounts;

DROP TABLE IF EXISTS console_test_export;
