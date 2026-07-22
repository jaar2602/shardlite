# Console SQL test kit

Use [`workbench-test.sql`](./workbench-test.sql) against a disposable MeshDB cluster. It is a
guided collection of statements, not a script to paste and execute as one batch.

Recommended order:

1. Put one schema statement under the cursor, select **Current statement**, and click **Run**;
   review and apply it in the results pane.
2. Run each account insert with its account ID in the contextual **Data key** field.
3. Select both write statements, choose **Selection**, and click **Run** to test an atomic transaction.
4. Run aggregates, grouped reads, sorting, distinct, and union normally; MeshDB queries the database.
5. Open **Options** only for targeted reads, freshness, explain, export, or diagnostics.
6. Use the expected-failure sections to verify constraint and transaction behavior.
7. Run large export, drift, partial-rollout, and stale-approval sections only when wanted.
8. Run and approve each cleanup statement separately.

The drift, partial-rollout, stale-approval, large-insert, and cleanup sections modify data or schema
deliberately. They are marked optional and should not be used against production data.

## Generate synthetic data

[`generate_workbench_data.py`](./generate_workbench_data.py) creates reproducible, synthetic
`INSERT` and `UPDATE` statements using fixed word lists and Python's standard pseudo-random number
generator. It does not use AI, access the network, connect to MeshDB, or execute SQL.

```bash
python3 console/examples/generate_workbench_data.py \
  --accounts 100 \
  --events-per-account 2 \
  --updates 40 \
  --seed 42 > /tmp/meshdb-test-data.sql
```

Run the schema statements from `workbench-test.sql` first. The generated output labels every SQL
statement with its **Data key** so it can be run without choosing a shard.
Using the same seed and arguments produces the same output. Generated `INSERT` statements are meant
to be applied once; use another prefix or clean up the earlier data before applying them again.
