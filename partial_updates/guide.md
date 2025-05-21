```shell
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'parallelism.default' = '3';
```

```sql
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG fluss_catalog;
```

```sql
-- Recommendations – model scores
CREATE TABLE recommendations (
    user_id  STRING,
    item_id  STRING,
    rec_score DOUBLE,
    rec_ts   TIMESTAMP(3),
    PRIMARY KEY (user_id, item_id) NOT ENFORCED
) WITH ('bucket.num' = '3', 'table.datalake.enabled' = 'true');


-- Impressions – how often we showed something
CREATE TABLE impressions (
    user_id STRING,
    item_id STRING,
    imp_cnt INT,
    imp_ts  TIMESTAMP(3),
    PRIMARY KEY (user_id, item_id) NOT ENFORCED
) WITH ('bucket.num' = '3', 'table.datalake.enabled' = 'true');

-- Clicks – user engagement
CREATE TABLE clicks (
    user_id  STRING,
    item_id  STRING,
    click_cnt INT,
    clk_ts    TIMESTAMP(3),
    PRIMARY KEY (user_id, item_id) NOT ENFORCED
) WITH ('bucket.num' = '3', 'table.datalake.enabled' = 'true');

CREATE TABLE user_rec_wide (
    user_id   STRING,
    item_id   STRING,
    rec_score DOUBLE,   -- updated by recs stream
    imp_cnt   INT,      -- updated by impressions stream
    click_cnt INT,      -- updated by clicks stream
    PRIMARY KEY (user_id, item_id) NOT ENFORCED
) WITH ('bucket.num' = '3', 'table.datalake.enabled' = 'true');
```

```sql
-- Recommendations – model scores
INSERT INTO recommendations VALUES
    ('user_101','prod_501',0.92 , TIMESTAMP '2025-05-16 09:15:02'),
    ('user_101','prod_502',0.78 , TIMESTAMP '2025-05-16 09:15:05'),
    ('user_102','prod_503',0.83 , TIMESTAMP '2025-05-16 09:16:00'),
    ('user_103','prod_501',0.67 , TIMESTAMP '2025-05-16 09:16:20'),
    ('user_104','prod_504',0.88 , TIMESTAMP '2025-05-16 09:16:45');
```

```sql
-- Impressions – how often each (user,item) was shown
INSERT INTO impressions VALUES
    ('user_101','prod_501', 3, TIMESTAMP '2025-05-16 09:17:10'),
    ('user_101','prod_502', 1, TIMESTAMP '2025-05-16 09:17:15'),
    ('user_102','prod_503', 7, TIMESTAMP '2025-05-16 09:18:22'),
    ('user_103','prod_501', 4, TIMESTAMP '2025-05-16 09:18:30'),
    ('user_104','prod_504', 2, TIMESTAMP '2025-05-16 09:18:55');
```

```sql
-- Clicks – user engagement
INSERT INTO clicks VALUES
    ('user_101','prod_501', 1, TIMESTAMP '2025-05-16 09:19:00'),
    ('user_101','prod_502', 2, TIMESTAMP '2025-05-16 09:19:07'),
    ('user_102','prod_503', 1, TIMESTAMP '2025-05-16 09:19:12'),
    ('user_103','prod_501', 1, TIMESTAMP '2025-05-16 09:19:20'),
    ('user_104','prod_504', 1, TIMESTAMP '2025-05-16 09:19:25');
```


```sql
-- Apply recommendation scores
INSERT INTO user_rec_wide (user_id, item_id, rec_score)
SELECT
    user_id,
    item_id,
    rec_score
FROM recommendations;
```

```shell
Flink SQL> SELECT * FROM user_rec_wide;
+----+--------------------------------+--------------------------------+--------------------------------+-------------+-------------+
| op |                        user_id |                        item_id |                      rec_score |     imp_cnt |   click_cnt |
+----+--------------------------------+--------------------------------+--------------------------------+-------------+-------------+
| +I |                       user_101 |                       prod_501 |                           0.92 |      <NULL> |      <NULL> |
| +I |                       user_101 |                       prod_502 |                           0.78 |      <NULL> |      <NULL> |
| +I |                       user_104 |                       prod_504 |                           0.88 |      <NULL> |      <NULL> |
| +I |                       user_102 |                       prod_503 |                           0.83 |      <NULL> |      <NULL> |
| +I |                       user_103 |                       prod_501 |                           0.67 |      <NULL> |      <NULL> |
```

```sql
-- Apply impression counts
INSERT INTO user_rec_wide (user_id, item_id, imp_cnt)
SELECT
    user_id,
    item_id,
    imp_cnt
FROM impressions;
```

```shell
Flink SQL> SELECT * FROM user_rec_wide;
+----+--------------------------------+--------------------------------+--------------------------------+-------------+-------------+
| op |                        user_id |                        item_id |                      rec_score |     imp_cnt |   click_cnt |
+----+--------------------------------+--------------------------------+--------------------------------+-------------+-------------+
| +I |                       user_101 |                       prod_501 |                           0.92 |      <NULL> |      <NULL> |
| +I |                       user_101 |                       prod_502 |                           0.78 |      <NULL> |      <NULL> |
| +I |                       user_104 |                       prod_504 |                           0.88 |      <NULL> |      <NULL> |
| +I |                       user_102 |                       prod_503 |                           0.83 |      <NULL> |      <NULL> |
| +I |                       user_103 |                       prod_501 |                           0.67 |      <NULL> |      <NULL> |



| -U |                       user_101 |                       prod_501 |                           0.92 |      <NULL> |      <NULL> |
| +U |                       user_101 |                       prod_501 |                           0.92 |           3 |      <NULL> |
| -U |                       user_101 |                       prod_502 |                           0.78 |      <NULL> |      <NULL> |
| +U |                       user_101 |                       prod_502 |                           0.78 |           1 |      <NULL> |
| -U |                       user_104 |                       prod_504 |                           0.88 |      <NULL> |      <NULL> |
| +U |                       user_104 |                       prod_504 |                           0.88 |           2 |      <NULL> |
| -U |                       user_102 |                       prod_503 |                           0.83 |      <NULL> |      <NULL> |
| +U |                       user_102 |                       prod_503 |                           0.83 |           7 |      <NULL> |
| -U |                       user_103 |                       prod_501 |                           0.67 |      <NULL> |      <NULL> |
| +U |                       user_103 |                       prod_501 |                           0.67 |           4 |      <NULL> |
```

```sql
-- Apply click counts
INSERT INTO user_rec_wide (user_id, item_id, click_cnt)
SELECT
    user_id,
    item_id,
    click_cnt
FROM clicks;
```

```shell
Flink SQL> SELECT * FROM user_rec_wide;
+----+--------------------------------+--------------------------------+--------------------------------+-------------+-------------+
| op |                        user_id |                        item_id |                      rec_score |     imp_cnt |   click_cnt |
+----+--------------------------------+--------------------------------+--------------------------------+-------------+-------------+
| +I |                       user_101 |                       prod_501 |                           0.92 |      <NULL> |      <NULL> |
| +I |                       user_101 |                       prod_502 |                           0.78 |      <NULL> |      <NULL> |
| +I |                       user_104 |                       prod_504 |                           0.88 |      <NULL> |      <NULL> |
| +I |                       user_102 |                       prod_503 |                           0.83 |      <NULL> |      <NULL> |
| +I |                       user_103 |                       prod_501 |                           0.67 |      <NULL> |      <NULL> |



| -U |                       user_101 |                       prod_501 |                           0.92 |      <NULL> |      <NULL> |
| +U |                       user_101 |                       prod_501 |                           0.92 |           3 |      <NULL> |
| -U |                       user_101 |                       prod_502 |                           0.78 |      <NULL> |      <NULL> |
| +U |                       user_101 |                       prod_502 |                           0.78 |           1 |      <NULL> |
| -U |                       user_104 |                       prod_504 |                           0.88 |      <NULL> |      <NULL> |
| +U |                       user_104 |                       prod_504 |                           0.88 |           2 |      <NULL> |
| -U |                       user_102 |                       prod_503 |                           0.83 |      <NULL> |      <NULL> |
| +U |                       user_102 |                       prod_503 |                           0.83 |           7 |      <NULL> |
| -U |                       user_103 |                       prod_501 |                           0.67 |      <NULL> |      <NULL> |
| +U |                       user_103 |                       prod_501 |                           0.67 |           4 |      <NULL> |


| -U |                       user_103 |                       prod_501 |                           0.67 |           4 |      <NULL> |
| +U |                       user_103 |                       prod_501 |                           0.67 |           4 |           1 |
| -U |                       user_101 |                       prod_501 |                           0.92 |           3 |      <NULL> |
| +U |                       user_101 |                       prod_501 |                           0.92 |           3 |           1 |
| -U |                       user_101 |                       prod_502 |                           0.78 |           1 |      <NULL> |
| +U |                       user_101 |                       prod_502 |                           0.78 |           1 |           2 |
| -U |                       user_104 |                       prod_504 |                           0.88 |           2 |      <NULL> |
| +U |                       user_104 |                       prod_504 |                           0.88 |           2 |           1 |
| -U |                       user_102 |                       prod_503 |                           0.83 |           7 |      <NULL> |
| +U |                       user_102 |                       prod_503 |                           0.83 |           7 |           1 |
```

```shell
./bin/lakehouse.sh -D flink.rest.address=jobmanager -D flink.rest.port=8081 -D flink.execution.checkpointing.interval=30s -D flink.parallelism.default=2

SET 'execution.runtime-mode' = 'batch';

Flink SQL> SELECT * FROM user_rec_wide;
+----------+----------+-----------+---------+-----------+
|  user_id |  item_id | rec_score | imp_cnt | click_cnt |
+----------+----------+-----------+---------+-----------+
| user_102 | prod_503 |      0.83 |       7 |         1 |
| user_103 | prod_501 |      0.67 |       4 |         1 |
| user_101 | prod_501 |      0.92 |       3 |         1 |
| user_101 | prod_502 |      0.78 |       1 |         2 |
| user_104 | prod_504 |      0.88 |       2 |         1 |
+----------+----------+-----------+---------+-----------+
5 rows in set (2.63 seconds)
```
