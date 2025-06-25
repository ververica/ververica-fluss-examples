```shell
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'parallelism.default' = '3';
```

```sql
CREATE TABLE player_profiles (
    player_id STRING,
    username STRING,
    email STRING,
    signup_date TIMESTAMP(3),
    country STRING
) WITH (
    'connector' = 'faker',
    'fields.player_id.expression' = '#{regexify ''(PL)[0-9]{5}''}',
    'fields.username.expression' = '#{name.username}',
    'fields.email.expression' = '#{Internet.emailAddress}',
    'fields.signup_date.expression' = '#{date.past ''1000'',''DAYS''}',
    'fields.country.expression' = '#{Address.country}'
);

SELECT * FROM player_profiles LIMIT 10;

CREATE TABLE games (
    game_id STRING,
    title STRING,
    genre STRING,
    release_date TIMESTAMP(3),
    developer STRING
) WITH (
    'connector' = 'faker',
    'fields.game_id.expression' = '#{regexify ''(GM)[0-9]{5}''}',
    'fields.title.expression' = '#{GameOfThrones.house}',
    'fields.genre.expression' = '#{regexify ''(Action|Adventure|Puzzle|Strategy|Simulation){1}''}',
    'fields.release_date.expression' = '#{date.past ''1000'',''DAYS''}',
    'fields.developer.expression' = '#{Company.name}'
);

SELECT * FROM games LIMIT 10;

CREATE TABLE gameplay_events (
    event_id STRING,
    player_id STRING,
    game_id STRING,
    score INT,
    event_time TIMESTAMP(3),
    event_type STRING
) WITH (
    'connector' = 'faker',
    'fields.event_id.expression' = '#{Internet.uuid}',
    'fields.player_id.expression' = '#{regexify ''(PL)[0-9]{5}''}',
    'fields.game_id.expression' = '#{regexify ''(GM)[0-9]{5}''}',
    'fields.score.expression'     = '#{number.numberBetween ''0'',''5000''}',
    'fields.event_time.expression' = '#{date.past ''30'',''SECONDS''}',
    'fields.event_type.expression' = '#{regexify ''(start_session|end_session|achievement|level_up){1}''}'
);

SELECT * FROM gameplay_events LIMIT 10;
```

```sql
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG fluss_catalog;
```

```sql
CREATE TABLE player_profiles (
    player_id STRING,
    username STRING,
    email STRING,
    signup_date TIMESTAMP(3),
    country STRING,
    PRIMARY KEY (player_id) NOT ENFORCED
) WITH (
      'bucket.num' = '3',
      'table.datalake.enabled' = 'true',
      'table.datalake.freshness' = '30s'
);

INSERT INTO player_profiles SELECT * FROM default_catalog.default_database.player_profiles;

CREATE TABLE games (
    game_id STRING,
    title STRING,
    genre STRING,
    release_date TIMESTAMP(3),
    developer STRING,
    PRIMARY KEY (game_id) NOT ENFORCED
) WITH (
      'bucket.num' = '3',
      'table.datalake.enabled' = 'true',
      'table.datalake.freshness' = '30s'
);

INSERT INTO games SELECT * FROM default_catalog.default_database.games;

CREATE TABLE gameplay_events (
    event_id STRING,
    player_id STRING,
    game_id STRING,
    score INT,
    proc_time AS PROCTIME(),
    event_time TIMESTAMP(3),
    event_type STRING,
       WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'bucket.num' = '3',
    'bucket.key' = 'player_id',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

INSERT INTO gameplay_events SELECT * FROM default_catalog.default_database.gameplay_events;
```

```sql
CREATE TABLE top3_leaderboard (
    ranking BIGINT,
    game_id STRING,
    player_id STRING,
    max_score BIGINT,
    proc_time AS PROCTIME(),
    PRIMARY KEY (ranking, game_id) NOT ENFORCED
) WITH (
      'bucket.num' = '3',
      'table.datalake.enabled' = 'true',
      'table.datalake.freshness' = '30s'
);
```

```sql
INSERT INTO top3_leaderboard
WITH aggregated_scores AS (SELECT player_id,
                                  game_id,
                                  MAX(score)                                      AS max_score, 
                                  TUMBLE_START(event_time, INTERVAL '60' SECONDS) AS window_start
                           FROM gameplay_events
                           GROUP BY TUMBLE(event_time, INTERVAL '60' SECONDS ),
                                    player_id,
                                    game_id)
SELECT *
FROM (
    SELECT  
        ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY max_score DESC) AS ranking,
        game_id,
        player_id,
        max_score
    FROM aggregated_scores)
WHERE ranking <= 3;
```

```sql
CREATE TABLE top3_leaderboard_enriched (
    game_id STRING,
    ranking BIGINT,
    player_id STRING,
    max_score BIGINT,
    username STRING,
    email STRING,
    title STRING,
    genre STRING,
    PRIMARY KEY (game_id, ranking) NOT ENFORCED
) WITH (
    'bucket.num' = '3', 
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);
```

```sql
INSERT INTO top3_leaderboard_enriched
SELECT
    tl.game_id,
    tl.ranking,
    tl.player_id,
    tl.max_score,
    pp.username,
    pp.email,
    g.title,
    g.genre
FROM top3_leaderboard tl
 JOIN player_profiles FOR SYSTEM_TIME AS OF tl.proc_time AS pp
    ON tl.player_id = pp.player_id
 JOIN games FOR SYSTEM_TIME AS OF tl.proc_time AS g
    ON tl.game_id = g.game_id;
```


```shell
./bin/lakehouse.sh -D flink.rest.address=jobmanager -D flink.rest.port=8081 -D flink.execution.checkpointing.interval=30s -D flink.parallelism.default=2
```

```shell
docker compose exec jobmanager \
    /opt/flink/bin/flink run \
    /opt/flink/opt/fluss-flink-tiering-0.7.0.jar \
    --fluss.bootstrap.servers coordinator-server:9123 \
    --datalake.format paimon \
    --datalake.paimon.metastore filesystem \
    --datalake.paimon.warehouse /tmp/paimon
```
```sql
SET 'execution.runtime-mode' = 'batch';
SET 'table.display.max-column-width' = '100';

SELECT * FROM top3_leaderboard_enriched ORDER BY game_id, ranking LIMIT 20;

SELECT * FROM games LIMIT 10;

SELECT * FROM games WHERE game_id='GM00011';

UPDATE games SET `genre` = 'Simulation' WHERE game_id='GM00011';

SELECT * FROM player_profiles LIMIT 10;

SELECT * FROM player_profiles WHERE player_id='PL00002';

DELETE FROM player_profiles WHERE player_id='PL00002';

SET 'execution.runtime-mode' = 'streaming';
```