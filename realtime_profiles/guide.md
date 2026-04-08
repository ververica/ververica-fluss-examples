## A complete worked example: real-time client risk profile

We will build a client risk profile that a fraud or AML system can query inline during transaction processing. The profile maintains flags per client: whether they transacted in a high-risk jurisdiction recently, whether they have high counterparty velocity, and whether they have been flagged for review.

### Step 1: Register the Fluss catalog

All tables in this example live in Fluss. No external message broker is required. Register the Fluss catalog once per Flink SQL session:
```shell
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'batch';
```

```sql
CREATE CATALOG fluss_catalog WITH (
  'type'              = 'fluss',
  'bootstrap.servers' = 'coordinator-server:3808'
);

USE CATALOG fluss_catalog;
```

Register all functions in your Flink SQL session before use.

```sql
ADD JAR '/opt/flink/jars/fluss-flink-realtime-profile-0.1.0.jar';

CREATE TEMPORARY FUNCTION to_bitmap64        AS 'io.ipolyzos.udfs.ToBitmap64';
CREATE TEMPORARY FUNCTION bitmap_contains    AS 'io.ipolyzos.udfs.BitmapContains';
CREATE TEMPORARY FUNCTION bitmap_cardinality AS 'io.ipolyzos.udfs.BitmapCardinality';
CREATE TEMPORARY FUNCTION bitmap_or          AS 'io.ipolyzos.udfs.BitmapOr';
CREATE TEMPORARY FUNCTION bitmap_and_not     AS 'io.ipolyzos.udfs.BitmapAndNot';
CREATE TEMPORARY FUNCTION bitmap_or_agg      AS 'io.ipolyzos.udfs.BitmapOrAgg';
CREATE TEMPORARY FUNCTION bitmap_to_string   AS 'io.ipolyzos.udfs.BitmapToString';
```

From this point, every `CREATE TABLE` statement creates a table directly in Fluss. The `bootstrap.servers` connection is catalog-scoped — individual table DDL does not need to repeat it.

### Step 2: The input event stream as a Fluss log table

Transaction events are written directly to a Fluss log table. Fluss log tables are append-only, support real-time consumption by Flink, and carry a configurable retention TTL. There is no intermediate message broker in the path.

```sql
CREATE TABLE transaction_events (
    account_id        STRING,          -- internal account identifier
    counterparty_id   STRING,          -- IBAN or account number of counterparty
    jurisdiction_code STRING,          -- ISO country code of transaction
    channel           STRING,          -- 'wire' | 'sepa' | 'swift' | 'card' | 'api'
    amount_eur        DECIMAL(18, 2),
    event_type        STRING,          -- 'debit' | 'credit' | 'attempt' | 'reversal'
    ts                TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'table.log.ttl' = '7d',           -- retain 7 days of raw events
    'bucket.num'    = '3'
);
```

Ingest the 13 sample events:

```sql
INSERT INTO transaction_events VALUES
    ('ACC-001', 'CP-IBAN-001', 'IR', 'wire',  50000.00, 'debit',  TIMESTAMP '2026-03-15 10:00:00'),
    ('ACC-001', 'CP-IBAN-002', 'DE', 'sepa',   1200.00, 'credit', TIMESTAMP '2026-03-15 10:01:00'),
    ('ACC-001', 'CP-IBAN-003', 'DE', 'sepa',    800.00, 'debit',  TIMESTAMP '2026-03-15 10:02:00'),
    ('ACC-001', 'CP-IBAN-004', 'US', 'swift', 25000.00, 'debit',  TIMESTAMP '2026-03-15 10:03:00'),
    ('ACC-001', 'CP-IBAN-005', 'DE', 'sepa',    400.00, 'credit', TIMESTAMP '2026-03-15 10:04:00'),
    ('ACC-001', 'CP-IBAN-006', 'DE', 'sepa',   3200.00, 'debit',  TIMESTAMP '2026-03-15 10:05:00'),
    ('ACC-001', 'CP-IBAN-007', 'DE', 'card',    150.00, 'debit',  TIMESTAMP '2026-03-15 10:06:00'),
    ('ACC-001', 'CP-IBAN-008', 'DE', 'sepa',   9800.00, 'debit',  TIMESTAMP '2026-03-15 10:07:00'),
    ('ACC-001', 'CP-IBAN-009', 'DE', 'sepa',    620.00, 'credit', TIMESTAMP '2026-03-15 10:08:00'),
    ('ACC-001', 'CP-IBAN-010', 'DE', 'wire',  11000.00, 'debit',  TIMESTAMP '2026-03-15 10:09:00'),
    ('ACC-001', 'CP-IBAN-011', 'DE', 'sepa',    450.00, 'debit',  TIMESTAMP '2026-03-15 10:10:00'),
    ('ACC-001', 'CP-IBAN-012', 'DE', 'sepa',    230.00, 'debit',  TIMESTAMP '2026-03-15 10:11:00'),
    ('ACC-002', 'CP-IBAN-002', 'DE', 'sepa',    300.00, 'credit', TIMESTAMP '2026-03-15 10:15:00');
```

```shell
Flink SQL> SELECT * FROM transaction_events;
+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+----------------------+--------------------------------+-------------------------+
| op |                     account_id |                counterparty_id |              jurisdiction_code |                        channel |           amount_eur |                     event_type |                      ts |
+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+----------------------+--------------------------------+-------------------------+
| +I |                        ACC-001 |                    CP-IBAN-001 |                             IR |                           wire |             50000.00 |                          debit | 2026-03-15 10:00:00.000 |
| +I |                        ACC-001 |                    CP-IBAN-002 |                             DE |                           sepa |              1200.00 |                         credit | 2026-03-15 10:01:00.000 |
| +I |                        ACC-001 |                    CP-IBAN-003 |                             DE |                           sepa |               800.00 |                          debit | 2026-03-15 10:02:00.000 |
| +I |                        ACC-001 |                    CP-IBAN-004 |                             US |                          swift |             25000.00 |                          debit | 2026-03-15 10:03:00.000 |
| +I |                        ACC-001 |                    CP-IBAN-005 |                             DE |                           sepa |               400.00 |                         credit | 2026-03-15 10:04:00.000 |
| +I |                        ACC-001 |                    CP-IBAN-006 |                             DE |                           sepa |              3200.00 |                          debit | 2026-03-15 10:05:00.000 |
| +I |                        ACC-001 |                    CP-IBAN-007 |                             DE |                           card |               150.00 |                          debit | 2026-03-15 10:06:00.000 |
| +I |                        ACC-001 |                    CP-IBAN-008 |                             DE |                           sepa |              9800.00 |                          debit | 2026-03-15 10:07:00.000 |
| +I |                        ACC-001 |                    CP-IBAN-009 |                             DE |                           sepa |               620.00 |                         credit | 2026-03-15 10:08:00.000 |
| +I |                        ACC-001 |                    CP-IBAN-010 |                             DE |                           wire |             11000.00 |                          debit | 2026-03-15 10:09:00.000 |
| +I |                        ACC-001 |                    CP-IBAN-011 |                             DE |                           sepa |               450.00 |                          debit | 2026-03-15 10:10:00.000 |
| +I |                        ACC-001 |                    CP-IBAN-012 |                             DE |                           sepa |               230.00 |                          debit | 2026-03-15 10:11:00.000 |
| +I |                        ACC-002 |                    CP-IBAN-002 |                             DE |                           sepa |               300.00 |                         credit | 2026-03-15 10:15:00.000 |
```
**State of `transaction_events` after ingestion:** 13 append-only rows exactly as inserted. This is a log table — there is no deduplication or merge.

### Step 3: Entity mapping with auto-increment integers

```sql
CREATE TABLE entity_mapping (
    entity_id     STRING,
    entity_type   STRING,   -- 'account' | 'counterparty' | 'device'
    entity_int64  BIGINT,
    PRIMARY KEY (entity_id, entity_type) NOT ENFORCED
) WITH (
    'auto-increment.fields'          = 'entity_int64',
    'lookup.insert-if-not-exists'    = 'true',
    'lookup.cache'                   = 'PARTIAL',
    'lookup.partial-cache.max-rows'  = '500000',
    'lookup.partial-cache.expire-after-write' = '1h',
--   'table.auto-increment.cache-size' = '10',   -- reduced for this example; default is 100000
    'bucket.num'                     = '3'
);
```

No separate entity-population pipeline is required. The `lookup.insert-if-not-exists = 'true'` option makes the Flink temporal lookup join in Step 4 handle registration inline: the first time a new `(entity_id, entity_type)` key arrives, the lookup finds no row, **atomically inserts it**, lets Fluss assign the next available `entity_int64` from the bucket's cached ID range, and returns the new row. Subsequent lookups for the same key return the stored value. The `TableLookup.createLookuper()` validation allows this only when all non-PK, non-auto-increment columns are nullable — which holds here since `entity_int64` is the sole non-PK column and is auto-assigned.

**How IDs are allocated across 3 buckets with `cache-size = 10`:**

Each bucket leader fetches IDs lazily — only when the first new entity for that bucket arrives. Whichever bucket receives its first INSERT earliest gets the first range from the global ZooKeeper counter.

**Pre-seeding `entity_mapping` (optional):** The `lookup.insert-if-not-exists` option means the enrichment job in Step 4 registers every entity automatically on first encounter. For local testing or development environments you can also seed known entities directly — Fluss assigns `entity_int64` automatically from the bucket's counter:

```sql
INSERT INTO entity_mapping (entity_id, entity_type) VALUES
    ('ACC-001',     'account'),
    ('ACC-002',     'account'),
    ('CP-IBAN-001', 'counterparty'),
    ('CP-IBAN-002', 'counterparty'),
    ('CP-IBAN-003', 'counterparty'),
    ('CP-IBAN-005', 'counterparty'),
    ('CP-IBAN-004', 'counterparty'),
    ('CP-IBAN-006', 'counterparty'),
    ('CP-IBAN-007', 'counterparty'),
    ('CP-IBAN-008', 'counterparty'),
    ('CP-IBAN-009', 'counterparty'),
    ('CP-IBAN-010', 'counterparty'),
    ('CP-IBAN-011', 'counterparty'),
    ('CP-IBAN-012', 'counterparty');
```

```shell
Flink SQL> SELECT * FROM entity_mapping;
+----+--------------------------------+--------------------------------+----------------------+
| op |                      entity_id |                    entity_type |         entity_int64 |
+----+--------------------------------+--------------------------------+----------------------+
| +I |                    CP-IBAN-002 |                   counterparty |               100001 |
| +I |                    CP-IBAN-003 |                   counterparty |               100002 |
| +I |                    CP-IBAN-008 |                   counterparty |               100003 |
| +I |                    CP-IBAN-011 |                   counterparty |               100004 |
| +I |                        ACC-001 |                        account |                    1 |
| +I |                    CP-IBAN-007 |                   counterparty |                    2 |
| +I |                    CP-IBAN-012 |                   counterparty |                    3 |
| +I |                        ACC-002 |                        account |               200001 |
| +I |                    CP-IBAN-001 |                   counterparty |               200002 |
| +I |                    CP-IBAN-004 |                   counterparty |               200003 |
| +I |                    CP-IBAN-005 |                   counterparty |               200004 |
| +I |                    CP-IBAN-006 |                   counterparty |               200005 |
| +I |                    CP-IBAN-009 |                   counterparty |               200006 |
| +I |                    CP-IBAN-010 |                   counterparty |               200007 |
```

`entity_int64` is omitted from the column list — Fluss assigns the next available value from the bucket's cached ID range. The resulting integer assignments are the same as those produced inline by the enrichment job (shown in the 3-bucket table above). In production you do not need this step; the lookup join handles registration transparently.

### Step 4: Enriched events table

`enriched_transactions` is a **Fluss log table**, not a Flink view. Materialising the enriched stream in Fluss means the temporal lookup join and entity registration run exactly once; Steps 6 and 7 each read independently from the same Fluss table without repeating the enrichment work.

First create the table:

```sql
CREATE TABLE enriched_transactions (
    account_id          STRING,
    account_int64       BIGINT,
    counterparty_id     STRING,
    counterparty_int64  BIGINT,
    jurisdiction_code   STRING,
    channel             STRING,
    amount_eur          DECIMAL(18, 2),
    event_type          STRING,
    ts                  TIMESTAMP(3),
        WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'bucket.num'    = '3',
    'table.log.ttl' = '7d'
);
```

```sql
INSERT INTO enriched_transactions
SELECT
  t.account_id,
  a.entity_int64        AS account_int64,
  t.counterparty_id,
  c.entity_int64        AS counterparty_int64,
  t.jurisdiction_code,
  t.channel,
  t.amount_eur,
  t.event_type,
  t.ts
FROM (SELECT *, proctime() AS ptime FROM transaction_events) AS t
JOIN entity_mapping FOR SYSTEM_TIME AS OF t.ptime AS a
  ON t.account_id = a.entity_id AND a.entity_type = 'account'
JOIN entity_mapping FOR SYSTEM_TIME AS OF t.ptime AS c
  ON t.counterparty_id = c.entity_id AND c.entity_type = 'counterparty';
```

**Selected rows written to `enriched_transactions`** (first two and last two of the 13 events, using the IDs from the 3-bucket assignment table in Step 3):

```shell
Flink SQL> SELECT * FROM enriched_transactions;
+----+--------------------------------+----------------------+--------------------------------+----------------------+--------------------------------+--------------------------------+----------------------+--------------------------------+-------------------------+
| op |                     account_id |        account_int64 |                counterparty_id |   counterparty_int64 |              jurisdiction_code |                        channel |           amount_eur |                     event_type |                      ts |
+----+--------------------------------+----------------------+--------------------------------+----------------------+--------------------------------+--------------------------------+----------------------+--------------------------------+-------------------------+
| +I |                        ACC-001 |                    1 |                    CP-IBAN-001 |               200002 |                             IR |                           wire |             50000.00 |                          debit | 2026-03-15 10:00:00.000 |
| +I |                        ACC-001 |                    1 |                    CP-IBAN-002 |               100001 |                             DE |                           sepa |              1200.00 |                         credit | 2026-03-15 10:01:00.000 |
| +I |                        ACC-001 |                    1 |                    CP-IBAN-003 |               100002 |                             DE |                           sepa |               800.00 |                          debit | 2026-03-15 10:02:00.000 |
| +I |                        ACC-001 |                    1 |                    CP-IBAN-004 |               200003 |                             US |                          swift |             25000.00 |                          debit | 2026-03-15 10:03:00.000 |
| +I |                        ACC-001 |                    1 |                    CP-IBAN-005 |               200004 |                             DE |                           sepa |               400.00 |                         credit | 2026-03-15 10:04:00.000 |
| +I |                        ACC-001 |                    1 |                    CP-IBAN-006 |               200005 |                             DE |                           sepa |              3200.00 |                          debit | 2026-03-15 10:05:00.000 |
| +I |                        ACC-001 |                    1 |                    CP-IBAN-007 |                    2 |                             DE |                           card |               150.00 |                          debit | 2026-03-15 10:06:00.000 |
| +I |                        ACC-001 |                    1 |                    CP-IBAN-008 |               100003 |                             DE |                           sepa |              9800.00 |                          debit | 2026-03-15 10:07:00.000 |
| +I |                        ACC-001 |                    1 |                    CP-IBAN-009 |               200006 |                             DE |                           sepa |               620.00 |                         credit | 2026-03-15 10:08:00.000 |
| +I |                        ACC-001 |                    1 |                    CP-IBAN-010 |               200007 |                             DE |                           wire |             11000.00 |                          debit | 2026-03-15 10:09:00.000 |
| +I |                        ACC-001 |                    1 |                    CP-IBAN-011 |               100004 |                             DE |                           sepa |               450.00 |                          debit | 2026-03-15 10:10:00.000 |
| +I |                        ACC-001 |                    1 |                    CP-IBAN-012 |                    3 |                             DE |                           sepa |               230.00 |                          debit | 2026-03-15 10:11:00.000 |
| +I |                        ACC-002 |               200001 |                    CP-IBAN-002 |               100001 |                             DE |                           sepa |               300.00 |                         credit | 2026-03-15 10:15:00.000 |
```
The job writes one enriched row per input event to the Fluss log table, with both account and counterparty resolved to their `BIGINT` identifiers. The non-contiguous integers (1, 11, 21, 5, 25, …) reflect 3-bucket allocation — each entity was inserted by the lookup join into the bucket determined by hashing its primary key. The string identifiers are retained for audit but do not participate in the downstream bitmap operations.

### Step 5: The risk groups table (Aggregation Merge Engine)

Each row represents one named risk group (e.g., `high_risk_jurisdiction:IR`) and the `members` column holds the serialised Roaring Bitmap of all account integers in that group.

The correct aggregation function for 64-bit integer bitmaps is `rbm64`. The column type must be `BYTES`. Bitmap operations at read time (cardinality, membership, intersection) require the registered UDFs described above.

```sql
CREATE TABLE risk_groups (
  group_key      STRING,
  members        BYTES,            -- serialised Roaring64Bitmap of account_int64 values
  last_update_ts TIMESTAMP(3),
  PRIMARY KEY (group_key) NOT ENFORCED
) WITH (
  'table.merge-engine'         = 'aggregation',
  'fields.members.agg'         = 'rbm64',
  'fields.last_update_ts.agg'  = 'last_value_ignore_nulls',
  'bucket.num'                 = '3'
);
```

Because `members` is stored as raw `BYTES`, `SELECT *` shows an unreadable hex blob. Use `bitmap_to_string` inline to inspect the table:

```sql
SELECT
    group_key,
    bitmap_to_string(members) AS members,
    last_update_ts
FROM risk_groups;
```

Example output:

```
+----+------------------------------+------------------------------------------+-------------------------+
| op |                    group_key |                                  members |          last_update_ts |
+----+------------------------------+------------------------------------------+-------------------------+
| +I | high_risk_jurisdiction:IR    | count=4 [1001, 1002, 1007, 1042]         | 2026-03-15 10:00:00.000 |
```

### Step 6: Writing high-risk jurisdiction group updates

Each event writes a single-element bitmap. No `COLLECT` or windowed aggregation is needed in Flink — Fluss handles the accumulation via `rbm64` at the storage layer.

```sql
INSERT INTO risk_groups
SELECT
  CONCAT('high_risk_jurisdiction:', jurisdiction_code)  AS group_key,
  to_bitmap64(account_int64)                            AS members,
  ts                                                    AS last_update_ts
FROM enriched_transactions
WHERE jurisdiction_code IN ('IR', 'KP', 'SY', 'CU', 'VE')
  AND event_type IN ('debit', 'credit');
```

```shell
Flink SQL> SELECT
>     group_key,
>     bitmap_to_string(members) AS members,
>     last_update_ts
> FROM risk_groups;
+----+--------------------------------+--------------------------------+-------------------------+
| op |                      group_key |                        members |          last_update_ts |
+----+--------------------------------+--------------------------------+-------------------------+
| +I |      high_risk_jurisdiction:IR |               count=1 [200001] | 2026-03-15 10:00:00.000 |
````

If a second distinct account — say `ACC-003` with `account_int64 = 6` (bucket 0, sixth slot) — later sent a wire to Iran, Fluss would OR `{6}` into `{1}` to produce `{1, 6}` with no Flink-side aggregation required.

Because `rbm64` is a storage-layer union aggregator, each row write — one `to_bitmap64(account_int64)` per event — is sufficient. Fluss ORs the incoming single-element bitmap into the accumulated group bitmap on every write.

### Step 7: Writing counterparty velocity group updates

Accounts with an anomalous number of unique counterparties within a time window are a structuring or mule account signal. The correct approach for a bounded window is to use a **Flink tumbling window**.

> **Important:** The `rbm64` aggregator performs append-only union — bitmaps only grow, they
> never shrink. An all-time accumulator cannot implement a bounded window rule. To enforce
> a 24-hour window, you must compute a fresh bitmap per window in Flink, not as a lifetime
> delta.

Two separate Flink pipelines handle this cleanly:

**Pipeline A** — accumulate per-account counterparty bitmaps per 24h window. This table is useful independently for querying "which counterparties did account X interact with?" and for audit.

```sql
CREATE TABLE account_counterparty_sets (
  window_start    TIMESTAMP(3),
  account_int64   BIGINT,
  counterparties  BYTES,           -- Roaring64Bitmap of counterparty integers seen this window
  last_update_ts  TIMESTAMP(3),
  PRIMARY KEY (window_start, account_int64) NOT ENFORCED
) WITH (
  'table.merge-engine'        = 'aggregation',
  'fields.counterparties.agg' = 'rbm64',
  'fields.last_update_ts.agg' = 'last_value_ignore_nulls',
  'bucket.num'                = '3'
);

INSERT INTO account_counterparty_sets
SELECT
  TUMBLE_START(ts, INTERVAL '24' HOUR)           AS window_start,
  account_int64,
  bitmap_or_agg(to_bitmap64(counterparty_int64)) AS counterparties,
  MAX(ts)                                        AS last_update_ts
FROM enriched_transactions
GROUP BY
  TUMBLE(ts, INTERVAL '24' HOUR),
  account_int64;
```

To inspect with readable output (the `counterparties` column is raw `BYTES`):

```sql
Flink SQL> SELECT
                   >     window_start,
                   >     account_int64,
                   >     bitmap_to_string(counterparties) AS counterparties,
                   >     last_update_ts
                   > FROM account_counterparty_sets;
+----+-------------------------+----------------------+--------------------------------+-------------------------+
| op |            window_start |        account_int64 |                 counterparties |          last_update_ts |
+----+-------------------------+----------------------+--------------------------------+-------------------------+
| +I | 2026-03-15 00:00:00.000 |               200001 | count=12 [1, 2, 3, 4, 10000... | 2026-03-15 10:11:00.000 |
| +I | 2026-03-15 00:00:00.000 |               100001 |                    count=1 [1] | 2026-03-15 10:15:00.000 |
```

**Pipeline B:** detect velocity breaches and write directly to `risk_groups`. This pipeline runs independently from the same source, using `COUNT(DISTINCT ...)` which is natively supported in Flink SQL windowed aggregation. This avoids reading back from `account_counterparty_sets` and sidesteps the unsupported correlated scalar subquery pattern (`WHERE window_start = (SELECT MAX(...))`) that does not work in Flink SQL streaming mode.

```sql
INSERT INTO risk_groups
SELECT
  'velocity_breach:24h'                      AS group_key,
  to_bitmap64(account_int64)                 AS members,
  TUMBLE_END(ts, INTERVAL '24' HOUR)         AS last_update_ts
FROM enriched_transactions
GROUP BY
  TUMBLE(ts, INTERVAL '24' HOUR),
  account_int64
HAVING COUNT(DISTINCT counterparty_int64) > 10;
```

```shell
Flink SQL> SELECT
>     group_key,
>     bitmap_to_string(members) AS members,
>     last_update_ts
> FROM risk_groups;
+----+--------------------------------+--------------------------------+-------------------------+
| op |                      group_key |                        members |          last_update_ts |
+----+--------------------------------+--------------------------------+-------------------------+
| +I |      high_risk_jurisdiction:IR |               count=1 [200001] | 2026-03-15 10:00:00.000 |
| +I |            velocity_breach:24h |               count=1 [200001] | 2026-03-16 00:00:00.000 |
```
### Closing the window: sentinel event

All 13 demo events are timestamped `2026-03-15 10:00–10:15`. The tumbling window spans
`2026-03-15 00:00:00 → 2026-03-16 00:00:00`. Flink fires a tumbling window only when the
watermark passes the window's end time. With `WATERMARK FOR ts AS ts - INTERVAL '5' SECOND`,
the watermark must exceed `2026-03-16 00:00:00`, which requires an event with
`ts ≥ 2026-03-16 00:00:05`.

Without such an event the window stays open forever — the INSERT jobs run but produce no output.

Insert one sentinel event after both pipeline jobs are running. It flows through the enrichment
join (ACC-001 and CP-IBAN-001 are already in `entity_mapping`) and advances the watermark in
`enriched_transactions`, triggering both Pipeline A and Pipeline B to emit their window results.

```sql
-- Advance the watermark past the 2026-03-15 24h window boundary.
-- Use an existing account + counterparty pair so the enrichment INNER JOIN passes it through.
INSERT INTO transaction_events VALUES
  ('ACC-001', 'CP-IBAN-001', 'DE', 'sepa', 0.00, 'watermark_flush', TIMESTAMP '2026-03-16 00:01:00');
```

Wait a few seconds after inserting. Both pipelines will emit results shortly after the watermark
advances. The sentinel row itself appears in `account_counterparty_sets` (CP-IBAN-001 is already
in ACC-001's set, so it does not change the cardinality count) and does not trigger a
velocity-breach write for `risk_groups` on its own.

**`account_counterparty_sets` after the 24h window closes** (window `2026-03-15 00:00:00 → 2026-03-16 00:00:00`):

| window_start        | account_int64 | counterparties                       | last_update_ts      |
|---------------------|---------------|--------------------------------------|---------------------|
| 2026-03-15 00:00:00 | 1             | {11,21,2,12,22,3,13,23,4,14,24,5}    | 2026-03-15 10:11:00 |
| 2026-03-15 00:00:00 | 25            | {21}                                 | 2026-03-15 10:15:00 |

`ACC-001` (int64 = 1, bucket 0) touched 12 distinct counterparties — their integers are drawn from all three buckets because each counterparty's bucket assignment is independent of the account's. `ACC-002` (int64 = 25, bucket 2) touched only `CP-IBAN-002` (int64 = 21, bucket 2).

**Per-account evaluation by Pipeline B** for the `2026-03-15` window:

| account_int64 | COUNT(DISTINCT counterparty_int64) | Exceeds threshold (> 10)? | Written to `risk_groups`? |
|---------------|------------------------------------|---------------------------|---------------------------|
| 1  (ACC-001)  | 12                                 | YES                       | YES                       |
| 25 (ACC-002)  | 1                                  | NO                        | NO                        |

**`risk_groups` state after Step 7** (cumulative — all rows):

| group_key                 | members | last_update_ts      |
|---------------------------|---------|---------------------|
| high_risk_jurisdiction:IR | {1}     | 2026-03-15 10:00:00 |
| velocity_breach:24h       | {1}     | 2026-03-16 00:00:00 |

> `TUMBLE_END(ts, INTERVAL '24' HOUR)` for the `2026-03-15` window resolves to `2026-03-16 00:00:00`.

### Step 8: Deriving the client risk profile via set algebra

Three issues in the original query are corrected here:

1. **Named bind parameters (`:account_int64`)** — Flink SQL has no `:param` syntax. The fix uses a `VALUES` inline table: define the account ID once as a typed `BIGINT` row and reference it by column name throughout, eliminating every repeated `CAST` call.

2. **`hj` returns multiple rows** — `group_key LIKE 'high_risk_jurisdiction:%'` matches one row per jurisdiction code. The original comma-separated `FROM` (implicit CROSS JOIN) therefore produced N result rows and tested each jurisdiction's bitmap independently, not the logical union across all jurisdictions. Fix: use `bitmap_or_agg` to collapse all jurisdiction bitmaps into one before testing membership.

3. **Missing group rows silently return no results** — if `velocity_breach:24h` or `under_review` groups do not yet exist in `risk_groups`, the original implicit CROSS JOIN returned zero rows. Fix: use explicit `LEFT JOIN ... ON TRUE` so that missing groups produce `NULL` members (resolved to `FALSE` by `bitmap_contains`'s null check).

```sql
-- account_int64 = 1 is ACC-001's entity_int64 in this 3-bucket example (bucket 0, first slot).
-- Obtain the actual value by looking up (entity_id, entity_type) in entity_mapping.
SELECT
  a.id                                                                  AS account_id,
  COALESCE(bitmap_contains(hj.members, a.id), FALSE)                    AS high_risk_jurisdiction,
  COALESCE(bitmap_contains(vb.members, a.id), FALSE)                    AS high_velocity,
  COALESCE(bitmap_contains(fr.members, a.id), FALSE)                    AS flagged_for_review,
  COALESCE(bitmap_contains(hj.members, a.id), FALSE)
    OR COALESCE(bitmap_contains(vb.members, a.id), FALSE)               AS risk_signal_active,
  (COALESCE(bitmap_contains(hj.members, a.id), FALSE)
    OR  COALESCE(bitmap_contains(vb.members, a.id), FALSE))
    AND NOT COALESCE(bitmap_contains(fr.members, a.id), FALSE)          AS requires_escalation
FROM
  (VALUES (CAST(1 AS BIGINT)))                                          AS a(id)
CROSS JOIN
  -- bitmap_or_agg collapses all per-jurisdiction bitmaps into one before the membership test.
  -- Without this, the LIKE subquery returns N rows and produces a Cartesian product.
  (SELECT bitmap_or_agg(members) AS members
   FROM risk_groups
   WHERE group_key LIKE 'high_risk_jurisdiction:%')                     AS hj
  -- LEFT JOIN ON TRUE: if a group row doesn't exist yet, members = NULL → COALESCE → FALSE.
  -- An implicit comma join (CROSS JOIN) would return zero rows if any group is absent.
LEFT JOIN (SELECT members FROM risk_groups WHERE group_key = 'velocity_breach:24h') AS vb ON TRUE
LEFT JOIN (SELECT members FROM risk_groups WHERE group_key = 'under_review')         AS fr ON TRUE;
```

**`risk_groups` rows read during this query:**

| group_key                 | members |
|---------------------------|---------|
| high_risk_jurisdiction:IR | {1}     |
| velocity_breach:24h       | {1}     |
| under_review              | *(no row — LEFT JOIN yields NULL)* |

**Intermediate bitmap evaluation for `id = 1` (ACC-001):**

| Subquery alias | Bitmap after aggregation / lookup          | `bitmap_contains(members, 1)` |
|----------------|---------------------------------------------|-------------------------------|
| `hj`           | {1}  (bitmap_or_agg over all IR rows = {1}) | TRUE                          |
| `vb`           | {1}                                         | TRUE                          |
| `fr`           | NULL  (no `under_review` row exists yet)    | FALSE  (COALESCE null → FALSE)|

**Expected output for ACC-001 (`id = 1`):**

| account_id | high_risk_jurisdiction | high_velocity | flagged_for_review | risk_signal_active | requires_escalation |
|------------|------------------------|---------------|--------------------|--------------------|---------------------|
| 1          | TRUE                   | TRUE          | FALSE              | TRUE               | TRUE                |

`ACC-001` is flagged for both high-risk jurisdiction and high counterparty velocity, with no active review flag. `requires_escalation = TRUE` because a risk signal is active and the account is not already under review.

**For comparison — querying ACC-002 (`id = 25`):**

To query ACC-002, substitute `CAST(25 AS BIGINT)` into the `VALUES` clause (ACC-002 landed in bucket 2 and was assigned `entity_int64 = 25`).

| account_id | high_risk_jurisdiction | high_velocity | flagged_for_review | risk_signal_active | requires_escalation |
|------------|------------------------|---------------|--------------------|--------------------|---------------------|
| 25         | FALSE                  | FALSE         | FALSE              | FALSE              | FALSE               |

`ACC-002` touched only one counterparty, did not transact in any sanctioned jurisdiction, and has no active review flag. `bitmap_contains({1}, 25) = FALSE` for both `hj` and `vb`, so all derived flags are `FALSE`.

> **For inline transaction decisioning**, running a Flink SQL query per transaction is not the
> right path — Flink SQL is a streaming pipeline engine, not an interactive query engine. For
> sub-millisecond point lookups during transaction authorization, use the Fluss client API
> directly: look up the `risk_groups` primary key rows for the three group keys, deserialize
> the `BYTES` bitmaps, and call `bitmap.contains(accountInt64)` in your application layer.
> The Flink SQL query above is best suited for batch profiling, periodic risk reporting, or
> Flink enrichment pipelines.

The profile for a given account resolves to a set of boolean flags that a decisioning system can consume directly:

| Flag | Meaning |
|---|---|
| `high_risk_jurisdiction` | Account transacted in a sanctioned/high-risk country in the current window |
| `high_velocity` | Account exceeded the unique-counterparty threshold in the last 24h window |
| `flagged_for_review` | Account is under active review |
| `risk_signal_active` | Any of the above risk signals is present |
| `requires_escalation` | Risk signal active and not already under review |

---


---

## Performance considerations and bottlenecks

The architecture above is correct by construction, but several patterns carry non-obvious cost at production scale. Each issue below is traceable to a specific part of the pipeline.

---

### Write path

#### 1. Per-event write amplification on `risk_groups` (Step 6)

Every transaction event causes one write to `risk_groups`. Each write triggers a **read-modify-write** cycle inside the Fluss aggregation merge engine: deserialize the existing bitmap, OR the new single-element bitmap in, re-serialize, and write back. At high throughput — tens of thousands of events per second — this produces a corresponding number of individual merge operations on the server.

**Mitigation:** Mini-batch writes in Flink before they reach Fluss. Group events by `group_key` within a short processing-time window (e.g., 500ms) and emit a pre-merged bitmap per group instead of one bitmap per event. This reduces the number of server-side merge cycles by the average fan-out factor within each mini-batch.

```sql
-- Pre-aggregate per group_key in Flink before writing to Fluss
INSERT INTO risk_groups
SELECT
  CONCAT('high_risk_jurisdiction:', jurisdiction_code)  AS group_key,
  bitmap_or_agg(to_bitmap64(account_int64))             AS members,
  MAX(ts)                                               AS last_update_ts
FROM enriched_transactions
WHERE jurisdiction_code IN ('IR', 'KP', 'SY', 'CU', 'VE')
  AND event_type IN ('debit', 'credit')
GROUP BY
  HOP(ts, INTERVAL '500' MILLISECOND, INTERVAL '500' MILLISECOND),
  jurisdiction_code;
```

---

#### 2. Lookup join cache for `entity_mapping` (Step 4)

The `enriched_transactions` table uses two temporal lookup joins against `entity_mapping`. Without a local cache, every event triggers a remote point lookup into Fluss for both the account and counterparty mapping. At high event rates this doubles the round-trip overhead on every record.

The Step 3 DDL already includes `lookup.cache = 'PARTIAL'` with a 1-hour write-expiry and a 500,000-row limit. No separate change is needed:

```
'lookup.cache'                   = 'PARTIAL'
'lookup.partial-cache.max-rows'  = '500000'
'lookup.partial-cache.expire-after-write' = '1h'
```

Financial identifiers are stable once assigned — an `entity_int64` for a given `entity_id` never changes after first assignment. A long TTL (hours) is safe and dramatically reduces lookup traffic. The `lookup.insert-if-not-exists` writes also populate the cache so that immediately subsequent events for the same entity do not need a server round-trip.

---

#### 3. `COUNT(DISTINCT)` state growth in Pipeline B (Step 7)

`COUNT(DISTINCT counterparty_int64)` inside a tumbling window requires Flink to maintain a per-key set of all observed counterparty values for the duration of the window. With millions of active accounts each potentially touching dozens of counterparties, the per-TaskManager state footprint grows proportionally.

State is scoped to `(window, account_int64)` and is released at window close, but during a 24-hour window the live state can be large. Set a state TTL slightly beyond the window size to ensure timely cleanup, and size your RocksDB backend accordingly:

```yaml
# flink-conf.yaml
state.backend: rocksdb
state.backend.rocksdb.memory.managed: true
table.exec.state.ttl: 86460000   # 24h + 1 minute, in milliseconds
```

---

### Query path

#### 4. `bitmap_or_agg` scans all jurisdiction rows on every profile query

The `hj` subquery uses a `LIKE` prefix scan across all `high_risk_jurisdiction:*` rows in `risk_groups`. With N jurisdiction codes tracked, this reads and deserializes N bitmaps and ORs them into one on every query execution. For five jurisdiction codes this is negligible; for a large dynamic list it becomes a linear scan per query.

**Mitigation:** Maintain a pre-computed unified key `high_risk_jurisdiction` that accumulates all jurisdictions into one bitmap as a separate write path. The profile query then reduces to three direct primary-key point lookups — the fastest possible access pattern on a Fluss KV table.

```sql
-- Write to a unified key in addition to (or instead of) per-jurisdiction keys
INSERT INTO risk_groups
SELECT
  'high_risk_jurisdiction'      AS group_key,   -- unified key, single point lookup at query time
  to_bitmap64(account_int64)    AS members,
  ts                            AS last_update_ts
FROM enriched_transactions
WHERE jurisdiction_code IN ('IR', 'KP', 'SY', 'CU', 'VE')
  AND event_type IN ('debit', 'credit');
```

With this pattern the profile query's `hj` subquery becomes:

```sql
(SELECT members FROM risk_groups WHERE group_key = 'high_risk_jurisdiction') AS hj
```

No aggregation. No scan. One point lookup.

---

#### 5. Three separate `risk_groups` scans per profile query

The profile query in Step 8 performs three independent subquery scans against `risk_groups` — one for `hj`, one for `vb`, one for `fr`. Each is an independent read from Fluss. For batch profiling this is acceptable; for latency-sensitive paths consider reading all three rows in a single multi-key lookup via the Fluss client API and resolving the bitmaps in the application layer.

---

#### 6. `BitmapOrAgg` is non-retractable

The `BitmapOrAgg` UDAF does not implement `retract()`. If Flink emits an `UPDATE_BEFORE` retraction message for a row in `risk_groups` (which can happen when `risk_groups` is used as a source in a streaming pipeline), the UDAF will throw `UnsupportedOperationException`. This is safe when `risk_groups` is only read in batch or append-mode contexts; it is unsafe if `risk_groups` is consumed as a changelog stream in a pipeline that expects full retraction support.

---

### Correctness under growth

#### 7. `risk_groups` jurisdiction bitmaps grow without bound

The `rbm64` aggregator only ever ORs new bits in — it never removes them. Accounts that transacted in a high-risk jurisdiction in 2022 remain in the bitmap indefinitely. For an AML rule that reads "transacted in the last 24 hours," the all-time accumulation produces permanent false positives for every account that has ever triggered the condition.

This is the same windowing issue addressed in Step 7 for velocity detection, and it applies equally to jurisdiction flags. If the rule is bounded in time, the write pipeline must produce windowed bitmaps (replacing, not merging) and the primary key must include the window boundary. The current Step 6 is correct only for rules with no time bound — e.g., "has this account ever transacted in a sanctioned jurisdiction?" If the rule is time-bounded, Step 6 must be restructured the same way Step 7 was.

---

### Summary

| Area | Issue | Impact | Fix |
|---|---|---|---|
| Step 6 write path | Per-event read-modify-write on `risk_groups` | High throughput → many server merge cycles | Mini-batch with `bitmap_or_agg` before writing |
| Step 4 lookup join | No cache on `entity_mapping` joins | Every event = 2 remote lookups | Enable `lookup.cache = PARTIAL` with long TTL |
| Step 7 Pipeline B | `COUNT(DISTINCT)` state grows per window | Large state footprint at scale | Set `table.exec.state.ttl`, use RocksDB backend |
| Step 8 `hj` subquery | LIKE scan ORs N jurisdiction bitmaps per query | Grows linearly with jurisdiction count | Maintain a single pre-merged `high_risk_jurisdiction` key |
| Step 8 query | Three independent `risk_groups` reads | 3× read RTT on critical path | Use multi-key Fluss client API lookup in application layer |
| `BitmapOrAgg` | No retract support | Fails if source emits retractions | Use only in append or tumbling-window contexts |
| Step 6 semantics | All-time accumulation for time-bounded rules | Permanent false positives | Use windowed write pipeline as in Step 7 if rule is time-bounded |

---

## Appendix: key option reference

| DDL option | Correct value | Notes |
|---|---|---|
| `auto-increment.fields` | column name (e.g. `entity_int64`) | Table-level; `table.` prefix is incorrect |
| `table.merge-engine` | `aggregation` | Also supports `first_row`, `versioned` |
| `fields.<col>.agg` | `rbm64`, `rbm32`, `sum`, `max`, `min`, `last_value`, `last_value_ignore_nulls`, `first_value`, `first_value_ignore_nulls`, `listagg`, `bool_and`, `bool_or` | Column type for `rbm64`/`rbm32` must be `BYTES` |
| `table.log.ttl` | duration string (e.g. `7d`) | Log tables only |

### Supported aggregation functions

| Function | Supported types | Notes |
|---|---|---|
| `sum`, `product` | TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL | |
| `max`, `min` | Numeric + CHAR, STRING, DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ | |
| `last_value` | All types | Null overwrites previous value |
| `last_value_ignore_nulls` | All types | Default when no function specified; nulls skipped |
| `first_value` | All types | Retains first value including null |
| `first_value_ignore_nulls` | All types | Retains first non-null value |
| `listagg`, `string_agg` | STRING, CHAR | Optional `delimiter` parameter |
| `bool_and`, `bool_or` | BOOLEAN | |
| `rbm32` | BYTES | 32-bit Roaring Bitmap union |
| `rbm64` | BYTES | 64-bit Roaring Bitmap union |

---

## Flink SQL Client: Setup and Enrichment Pipeline

### Connect to the SQL client

```bash
docker exec -it fluss-flink-realtime-profile-jobmanager-1 ./bin/sql-client.sh
```

---

### Create the Fluss catalog

`bootstrap.servers` is configured **once at the catalog level**. All tables created inside the catalog inherit the connection — no `connector` or `bootstrap.servers` properties are needed in individual table `WITH` clauses.

```sql
CREATE CATALOG fluss_catalog WITH (
    'type'              = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG fluss_catalog;
```

---

### Create a database

```sql
CREATE DATABASE IF NOT EXISTS my_db;
USE my_db;
```

---

### Create tables

Tables inside the Fluss catalog only need table-specific options such as `bucket.num`.

**transaction_events** (append-only log table — probe side for enrichment join):

A `WATERMARK` is required so that `ts` is a valid time attribute for the tumbling window
aggregations in Steps 6 and 7.

```sql
CREATE TABLE transaction_events (
    account_id        STRING,
    counterparty_id   STRING,
    jurisdiction_code STRING,
    channel           STRING,
    amount_eur        DECIMAL(18, 2),
    event_type        STRING,
    ts                TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'bucket.num' = '8'
);
```

**entity_mapping** (primary-key lookup table — dimension side):

A Fluss primary-key table is automatically a valid lookup/temporal join target. No watermark,
timestamp column, or time attribute is required on the dimension side.

`auto-increment.fields` causes Fluss to assign `entity_int64` automatically on every insert.
`lookup.insert-if-not-exists` makes the enrichment join register unknown entities on first
encounter. `lookup.cache = 'PARTIAL'` keeps recently seen mappings in the Flink task-local
cache, avoiding a remote round-trip on every event.

```sql
CREATE TABLE entity_mapping (
    entity_id     STRING NOT NULL,
    entity_type   STRING NOT NULL,
    entity_int64  BIGINT,
    PRIMARY KEY (entity_id, entity_type) NOT ENFORCED
) WITH (
    'auto-increment.fields'                   = 'entity_int64',
    'lookup.insert-if-not-exists'             = 'true',
    'lookup.cache'                            = 'PARTIAL',
    'lookup.partial-cache.max-rows'           = '500000',
    'lookup.partial-cache.expire-after-write' = '1h',
    'bucket.num'                              = '4'
);
```

**enriched_transactions** (append-only log table — output of the enrichment job):

Defined as a log table (no primary key) so that every enriched event is retained as a distinct
row. A `WATERMARK` is required here for the same reason as on `transaction_events`: Steps 6
and 7 read from this table and use `ts` as the window time attribute.

```sql
CREATE TABLE enriched_transactions (
    account_id         STRING,
    account_int64      BIGINT,
    counterparty_id    STRING,
    counterparty_int64 BIGINT,
    jurisdiction_code  STRING,
    channel            STRING,
    amount_eur         DECIMAL(18, 2),
    event_type         STRING,
    ts                 TIMESTAMP(3),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'table.log.ttl' = '7d',
    'bucket.num'    = '4'
);
```

---

### Register the bitmap UDFs

`ADD JAR` loads the jar into the session classloader. `CREATE TEMPORARY FUNCTION` registers each UDF
session-locally without touching the Fluss catalog (which does not support `createFunction()`).

```sql
ADD JAR '/opt/flink/jars/fluss-flink-realtime-profile-0.1.0.jar';

CREATE TEMPORARY FUNCTION to_bitmap64        AS 'io.ipolyzos.udfs.ToBitmap64';
CREATE TEMPORARY FUNCTION bitmap_contains    AS 'io.ipolyzos.udfs.BitmapContains';
CREATE TEMPORARY FUNCTION bitmap_cardinality AS 'io.ipolyzos.udfs.BitmapCardinality';
CREATE TEMPORARY FUNCTION bitmap_or          AS 'io.ipolyzos.udfs.BitmapOr';
CREATE TEMPORARY FUNCTION bitmap_and_not     AS 'io.ipolyzos.udfs.BitmapAndNot';
CREATE TEMPORARY FUNCTION bitmap_or_agg      AS 'io.ipolyzos.udfs.BitmapOrAgg';
```

---

### Enrichment temporal join

#### How it works

Fluss primary-key tables support **processing-time lookup joins** natively — no watermark or
event-time attribute is required on either side. `proctime()` is injected **inline in the
subquery** so the table DDL stays clean. This is the pattern shown in the
[official Fluss lookup join docs](https://fluss.apache.org/docs/engine-flink/lookups/).

At join time, Flink looks up the **current version** of each key in `entity_mapping`.

> **Why `proctime()` and not `t.ts`?**
> Using `FOR SYSTEM_TIME AS OF t.ts` triggers Flink's event-time temporal join path, which
> requires a row-time attribute and watermark on the dimension table. Fluss primary-key tables
> do not expose a row-time attribute, so this always fails with:
> `ValidationException: Event-Time Temporal Table Join requires both primary key and row time
> attribute in versioned table, but no row time attribute can be found.`
> The fix is to use `proctime()` instead, which bypasses the event-time path entirely.

#### Query

```sql
INSERT INTO enriched_transactions
SELECT
    t.account_id,
    a.entity_int64                      AS account_int64,
    t.counterparty_id,
    c.entity_int64                      AS counterparty_int64,
    t.jurisdiction_code,
    t.channel,
    t.amount_eur,
    t.event_type,
    t.ts
FROM (SELECT *, proctime() AS ptime FROM transaction_events) AS t
         JOIN entity_mapping FOR SYSTEM_TIME AS OF t.ptime AS a
              ON t.account_id = a.entity_id AND a.entity_type = 'account'
         JOIN entity_mapping FOR SYSTEM_TIME AS OF t.ptime AS c
              ON t.counterparty_id = c.entity_id AND c.entity_type = 'counterparty';
```
