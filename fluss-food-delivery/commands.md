```shell
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'parallelism.default' = '3';
```

```sql
-- Create DeliveryETAEvent table
CREATE TABLE delivery_eta_events (
    orderId STRING,
    courierId STRING,
    restaurantId STRING,
    destinationLatitude DOUBLE,
    destinationLongitude DOUBLE,
    estimatedPrepTimeSeconds BIGINT,
    estimatedTravelTimeSeconds BIGINT,
    eventTimestamp TIMESTAMP(3)
) WITH (
    'connector' = 'faker',
    'fields.orderId.expression' = '#{number.numberBetween ''1001'',''100000''}',
    'fields.courierId.expression' = '#{number.numberBetween ''1'',''100000''}',
    'fields.restaurantId.expression' = '#{number.numberBetween ''1'',''10000''}',
    'fields.destinationLatitude.expression' = '#{address.latitude}',
    'fields.destinationLongitude.expression' = '#{address.longitude}',
    'fields.estimatedPrepTimeSeconds.expression' = '#{number.numberBetween ''300'',''1200''}',
    'fields.estimatedTravelTimeSeconds.expression' = '#{number.numberBetween ''600'',''1800''}',
    'fields.eventTimestamp.expression' = '#{date.past ''30'',''SECONDS''}'
);

-- Create CourierLocationUpdate table
CREATE TABLE courier_locations (
    courierId STRING,
    orderId STRING,
    courierLatitude DOUBLE,
    courierLongitude DOUBLE,
    speedKph DOUBLE,
    isAvailable BOOLEAN,
    lastUpdatedTimestamp TIMESTAMP(3),
    PRIMARY KEY (courierId) NOT ENFORCED
) WITH (
    'connector' = 'faker',
    'fields.courierId.expression' = '#{number.numberBetween ''1'',''100000''}',
    'fields.orderId.expression' = '#{number.numberBetween ''1001'',''100000''}',
    'fields.courierLatitude.expression' = '#{address.latitude}',
    'fields.courierLongitude.expression' = '#{address.longitude}',
    'fields.speedKph.expression' = '#{number.randomDouble ''2'',''5'',''60''}',
    'fields.isAvailable.expression' = '#{options.option ''true'',''false''}',
    'fields.lastUpdatedTimestamp.expression' = '#{date.past ''30'',''SECONDS''}'
);

-- Create RestaurantPrepStatus table
CREATE TABLE restaurant_prep_status (
    restaurantId STRING,
    orderId STRING,
    currentQueueSize INT,
    actualPrepTimeSeconds BIGINT,
    estimatedPrepTimeSeconds BIGINT,
    lastUpdatedTimestamp TIMESTAMP(3),
    PRIMARY KEY (restaurantId) NOT ENFORCED
) WITH (
    'connector' = 'faker',
    'fields.restaurantId.expression' = '#{number.numberBetween ''1'',''10000''}',
    'fields.orderId.expression' = '#{number.numberBetween ''1001'',''100000''}',
    'fields.currentQueueSize.expression' = '#{number.numberBetween ''0'',''15''}',
    'fields.estimatedPrepTimeSeconds.expression' = '#{number.numberBetween ''300'',''1200''}',
    'fields.actualPrepTimeSeconds.expression' = '#{number.numberBetween ''120'',''1500''}',
    'fields.lastUpdatedTimestamp.expression' = '#{date.past ''30'',''SECONDS''}'
);

CREATE TABLE order_payment_info (
    orderId STRING,
    totalAmount DOUBLE,
    paymentMethod STRING,
    usedPromo BOOLEAN,
    isFirstOrder BOOLEAN,
    paymentTimestamp TIMESTAMP(3),
    PRIMARY KEY (orderId) NOT ENFORCED
) WITH (
    'connector' = 'faker',
    'fields.orderId.expression' = '#{number.numberBetween ''1001'',''100000''}',
    'fields.totalAmount.expression' = '#{number.randomDouble ''2'',''10'',''100''}',
    'fields.paymentMethod.expression' = '#{options.option ''Credit Card'',''Revolut'',''PayPal'',''Apple Pay''}',
    'fields.usedPromo.expression' = '#{options.option ''true'',''false''}',
    'fields.isFirstOrder.expression' = '#{options.option ''true'',''false''}',
    'fields.paymentTimestamp.expression' = '#{date.past ''30'',''SECONDS''}'
);
```

```sql
CREATE CATALOG fluss WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:3808'
);

USE CATALOG fluss;
```

```sql
-- Create DeliveryETAEvent table
CREATE TABLE delivery_eta_events (
    orderId STRING,
    courierId STRING,
    restaurantId STRING,
    destinationLatitude DOUBLE,
    destinationLongitude DOUBLE,
    estimatedPrepTimeSeconds BIGINT,
    estimatedTravelTimeSeconds BIGINT,
    eventTimestamp TIMESTAMP(3)
) WITH (
    'bucket.num' = '3',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

INSERT INTO delivery_eta_events SELECT * FROM default_catalog.default_database.delivery_eta_events;


-- Create CourierLocationUpdate table
CREATE TABLE courier_locations (
    courierId STRING,
    orderId STRING,
    courierLatitude DOUBLE,
    courierLongitude DOUBLE,
    speedKph DOUBLE,
    isAvailable BOOLEAN,
    lastUpdatedTimestamp TIMESTAMP(3),
    PRIMARY KEY (courierId) NOT ENFORCED
) WITH (
    'bucket.num' = '3',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

INSERT INTO courier_locations SELECT * FROM default_catalog.default_database.courier_locations;

-- Create RestaurantPrepStatus table
CREATE TABLE restaurant_prep_status (
    restaurantId STRING,
    orderId STRING,
    currentQueueSize INT,
    actualPrepTimeSeconds BIGINT,
    estimatedPrepTimeSeconds BIGINT,
    lastUpdatedTimestamp TIMESTAMP(3),
    PRIMARY KEY (restaurantId) NOT ENFORCED
) WITH (
    'bucket.num' = '3',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

INSERT INTO restaurant_prep_status SELECT * FROM default_catalog.default_database.restaurant_prep_status;


CREATE TABLE order_payment_info (
    orderId STRING,
    totalAmount DOUBLE,
    paymentMethod STRING,
    usedPromo BOOLEAN,
    isFirstOrder BOOLEAN,
    paymentTimestamp TIMESTAMP(3),
    PRIMARY KEY (orderId) NOT ENFORCED
) WITH (
    'bucket.num' = '3',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);

INSERT INTO order_payment_info SELECT * FROM default_catalog.default_database.order_payment_info;
```


```shell
docker compose exec jobmanager /opt/flink/bin/flink run -D parallelism.default=2 \
    /opt/flink/opt/fluss-flink-tiering-0.7.0.jar \
    --fluss.bootstrap.servers coordinator-server:3808 \
    --datalake.format paimon \
    --datalake.paimon.metastore filesystem \
    --datalake.paimon.warehouse /tmp/paimon
```






```sql
SET 'execution.runtime-mode' = 'batch';
```

```sql
SET 'execution.runtime-mode' = 'streaming';
```

-- Flink SQL CREATE TABLE statements using faker connector

```sql
-- Create the database
CREATE DATABASE IF NOT EXISTS food_delivery_db;
USE food_delivery_db;
```