## A complete worked example: real-time client risk profile

We will build a client risk profile that a fraud or AML system can query inline during transaction processing. 

The profile maintains flags per client: whether they transacted in a high-risk jurisdiction recently, whether they have high counterparty velocity, and whether they have been flagged for review.

### Step 1: Register the Fluss catalog

All tables in this example live in Fluss. No external message broker is required. Register the Fluss catalog once per Flink SQL session:
```shell
SET 'sql-client.execution.result-mode' = 'tableau';
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

Each bucket leader fetches IDs lazily, only when the first new entity for that bucket arrives.
Whichever bucket receives its first INSERT earliest gets the first range from the global ZooKeeper counter.

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


### Step 4: Enriched events table

`enriched_transactions` is a **Fluss log table**, not a Flink view. 
Materialising the enriched stream in Fluss means the temporal lookup join and entity registration run exactly once; 
Steps 6 and 7 each read independently from the same Fluss table without repeating the enrichment work.

#### First create the table:

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

Because `members` is stored as raw `BYTES`, `SELECT *` shows an unreadable hex blob. 
Use `bitmap_to_string` inline to inspect the table:

```sql
SELECT
    group_key,
    bitmap_to_string(members) AS members,
    last_update_ts
FROM risk_groups;
```

#### Example output:

```
+----+------------------------------+------------------------------------------+-------------------------+
| op |                    group_key |                                  members |          last_update_ts |
+----+------------------------------+------------------------------------------+-------------------------+
| +I | high_risk_jurisdiction:IR    | count=4 [1001, 1002, 1007, 1042]         | 2026-03-15 10:00:00.000 |
```

### Step 6: Writing high-risk jurisdiction group updates

Each event writes a single-element bitmap. No `COLLECT` or windowed aggregation is needed in Flink,
Fluss handles the accumulation via `rbm64` at the storage layer.

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

If a second distinct account, say `ACC-003` with `account_int64 = 6` (bucket 0, sixth slot)  later sent a wire to Iran, 
Fluss would OR `{6}` into `{1}` to produce `{1, 6}` with no Flink-side aggregation required.

Because `rbm64` is a storage-layer union aggregator, each row write, one `to_bitmap64(account_int64)` per event is sufficient. 
Fluss ORs the incoming single-element bitmap into the accumulated group bitmap on every write.

### Step 7: Writing counterparty velocity group updates

Accounts with an anomalous number of unique counterparties within a time window are a structuring or mule account signal. The correct approach for a bounded window is to use a **Flink tumbling window**.

> **Important:** The `rbm64` aggregator performs append-only union — bitmaps only grow, they
> never shrink. An all-time accumulator cannot implement a bounded window rule. To enforce
> a 24-hour window, you must compute a fresh bitmap per window in Flink, not as a lifetime
> delta.

Two separate Flink pipelines handle this cleanly:

**Pipeline A:** Accumulate per-account counterparty bitmaps per 24h window. This table is useful independently for querying "which counterparties did account X interact with?" and for audit.

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

**Pipeline B:** Detect velocity breaches and write directly to `risk_groups`. This pipeline runs independently from the same source, using `COUNT(DISTINCT ...)` which is natively supported in Flink SQL windowed aggregation. This avoids reading back from `account_counterparty_sets` and sidesteps the unsupported correlated scalar subquery pattern (`WHERE window_start = (SELECT MAX(...))`) that does not work in Flink SQL streaming mode.

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


The profile for a given account resolves to a set of boolean flags that a decisioning system can consume directly:

| Flag | Meaning |
|---|---|
| `high_risk_jurisdiction` | Account transacted in a sanctioned/high-risk country in the current window |
| `high_velocity` | Account exceeded the unique-counterparty threshold in the last 24h window |
| `flagged_for_review` | Account is under active review |
| `risk_signal_active` | Any of the above risk signals is present |
| `requires_escalation` | Risk signal active and not already under review |

---