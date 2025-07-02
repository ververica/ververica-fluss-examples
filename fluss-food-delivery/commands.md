```shell
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'parallelism.default' = '3';
```

```sql
CREATE CATALOG fluss WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:3808'
);

USE CATALOG fluss;
    
USE food_delivery_db;
```

```sql
CREATE TABLE IF NOT EXISTS orders_wide (
    -- OrderPaymentInfo fields
    orderId STRING,
    totalAmount DECIMAL(5, 2),
    paymentMethod STRING,
    usedPromo BOOLEAN,
    isFirstOrder BOOLEAN,
    paymentTimestamp TIMESTAMP(3),

    -- RestaurantPrepStatus fields
    restaurantId STRING,
    currentQueueSize INT,
    actualPrepTimeSeconds FLOAT,
    estimatedPrepTimeSeconds FLOAT,
    lastUpdatedTimestampR TIMESTAMP(3),

    -- CourierLocationUpdate fields
    courierId STRING,
    courierLatitude DECIMAL(15, 2),
    courierLongitude DECIMAL(15, 2),
    speedKph DECIMAL(5, 2),
    isAvailable BOOLEAN,
    lastUpdatedTimestampC TIMESTAMP(3),

    PRIMARY KEY (orderId) NOT ENFORCED
) WITH (
    'bucket.num' = '3',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);
```

```sql
INSERT INTO orders_wide (orderId, courierId, courierLatitude, courierLongitude, speedKph, isAvailable, lastUpdatedTimestampC)
SELECT
    orderId, 
    courierId, 
    courierLatitude, 
    courierLongitude, 
    speedKph, 
    isAvailable,
    lastUpdatedTimestamp AS lastUpdatedTimestampC
FROM courier_locations;

INSERT INTO orders_wide (orderId, restaurantId, currentQueueSize, actualPrepTimeSeconds, estimatedPrepTimeSeconds, lastUpdatedTimestampR)
SELECT
    orderId, 
    restaurantId, 
    currentQueueSize, 
    actualPrepTimeSeconds, 
    estimatedPrepTimeSeconds,
    lastUpdatedTimestamp AS lastUpdatedTimestampR
FROM restaurant_prep_status;

INSERT INTO orders_wide (orderId, totalAmount, paymentMethod, usedPromo, isFirstOrder, paymentTimestamp)
SELECT
    orderId,
    totalAmount,
    paymentMethod,
    usedPromo,
    isFirstOrder,
    paymentTimestamp
FROM order_payment_info;
```

```sql

INSERT INTO order_payment_info (orderId, totalAmount, paymentMethod, usedPromo, isFirstOrder, paymentTimestamp)
VALUES ('12345', 45.99, 'Credit Card', true, false, TIMESTAMP '2023-10-15 12:30:45');

-- Insert into restaurant_prep_status
INSERT INTO restaurant_prep_status (restaurantId, orderId, currentQueueSize, actualPrepTimeSeconds, estimatedPrepTimeSeconds, lastUpdatedTimestamp)
VALUES ('5678', '12345', 3, 850, 900, TIMESTAMP '2023-10-15 12:35:20');

-- Insert into delivery_eta_events
INSERT INTO courier_locations (orderId, courierId, courierLatitude, courierLongitude, speedKph, isAvailable, lastUpdatedTimestamp)
VALUES ('12345', '9012', 37.7749, -122.4194, 900, true, TIMESTAMP '2023-10-15 12:40:10');
```

```sql
SET 'execution.runtime-mode' = 'batch';
```

```sql
UPDATE order_payment_info SET totalAmount=95.0 WHERE orderId='1000424';

SELECT * FROM order_payment_info WHERE orderId='1000424';

SELECT * FROM restaurant_prep_status LIMIT 10;
SELECT * FROM restaurant_prep_status WHERE restaurantId='10';
DELETE FROM restaurant_prep_status WHERE restaurantId='10';

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