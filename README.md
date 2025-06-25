# Ververica Fluss Examples

<p align="center">
    <img src="assets/logo.png">
</p>

## Subprojects

### 1. fluss-java-client

A Java client for interacting with Ververica Fluss. This example demonstrates how to:

- Connect to a Fluss database
- Create databases and tables
- Write sensor data to Fluss using AppendWriter and UpsertWriter
- Read and process data from Fluss using LogScanner and Lookuper
- Perform lookups to enrich data

#### Usage

```bash
cd fluss-java-client
docker-compose up -d
```

The example uses a sensor data model with:
- Sensor readings (temperature, humidity, pressure, battery level)
- Sensor information (metadata about sensors)
- Enriched sensor readings (combined data)

### 2. getting_started

A comprehensive guide to getting started with Fluss using Flink SQL. This example demonstrates:

- Creating tables with test data using the Faker connector
- Setting up a Fluss catalog
- Creating tables in Fluss with various configurations
- Inserting data into Fluss tables
- Creating a gaming leaderboard using SQL queries
- Enriching the leaderboard with player and game information
- Running a Flink job for tiering data to a data lake
- Querying, updating, and deleting data in Fluss tables

#### Usage

```bash
cd getting_started
docker-compose up -d
```

Follow the step-by-step guide in `getting_started/guide.md` to explore Fluss SQL capabilities.

### 3. partial_updates

An example demonstrating how to use Fluss for partial updates to records. This is particularly useful for maintaining a denormalized view of data from multiple sources. The example shows:

- Creating tables for recommendations, impressions, and clicks
- Creating a wide table that combines data from all three sources
- Inserting data into the source tables
- Using partial updates to update specific columns in the wide table without affecting other columns
- Observing how the data evolves as updates are applied

#### Usage

```bash
cd partial_updates
docker-compose up -d
```

Follow the guide in `partial_updates/guide.md` to learn about partial updates in Fluss.

## Prerequisites

- Docker and Docker Compose
- Java 17 or later (for the Java client)
- Apache Flink (included in the Docker setup)

## Getting Started

1. Clone this repository:
   ```bash
   git clone https://github.com/ververica/fluss-examples.git
   cd fluss-examples
   ```

2. Choose one of the examples and follow its specific instructions.