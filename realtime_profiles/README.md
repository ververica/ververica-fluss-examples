# Fluss + Flink: Real-time Client Risk Profile

A Flink SQL environment backed by [Apache Fluss](https://fluss.apache.org/) for real-time profile
aggregation using [Roaring64Bitmap](https://roaringbitmap.org/) UDFs. The example builds a
client risk profile that a fraud or AML system can query inline during transaction processing.

---

## Architecture

| Component | Image | Role |
|---|---|---|
| `zookeeper` | `zookeeper:3.9.2` | Coordination for the Fluss cluster |
| `coordinator-server` | `apache/fluss:0.9.0-incubating` | Fluss coordinator |
| `tablet-server-0/1/2` | `apache/fluss:0.9.0-incubating` | Fluss storage nodes (3 buckets) |
| `jobmanager` | `flink:1.20-java17` (custom) | Flink Job Manager |
| `taskmanager` | `flink:1.20-java17` (custom) | Flink Task Manager |

The custom Flink image ([`Dockerfile`](Dockerfile)) downloads the Fluss–Flink connector at build
time and places it in `/opt/flink/lib`. The `jars/` directory is bind-mounted into both the
JobManager and TaskManager at `/opt/flink/jars` so UDF jars are available to the SQL client
without restarting the cluster.

---

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/)

That is all. The UDF jar is pre-built and included in [`jars/`](jars/).

---

## Start the cluster

From the `realtime_profiles/` directory:

```bash
docker compose up -d
```

Docker builds the custom Flink image on first run (downloading the Fluss connector) and then
starts all six services. Wait until the JobManager and TaskManager are healthy before opening
the SQL client.

The Flink Web UI is available at [http://localhost:8083](http://localhost:8083).

To stop and remove all containers:

```bash
docker compose down
```

---

## Open the Flink SQL client

```bash
docker exec -it fluss-flink-realtime-profile-jobmanager-1 ./bin/sql-client.sh
```

---

## Set up the Fluss catalog

Run these two statements once at the start of every SQL client session:

```sql
CREATE CATALOG fluss_catalog WITH (
  'type'              = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG fluss_catalog;
```
---

## Register the bitmap UDFs

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

---

## Walkthrough

See [`guide.md`](guide.md) for the complete step-by-step example: creating the input event
stream, entity mapping with auto-increment integers, enrichment via temporal lookup join,
bitmap-based risk group aggregation, and the final set-algebra profile query.
