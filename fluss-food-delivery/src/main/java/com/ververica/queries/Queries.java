package com.ververica.queries;

import com.ververica.utils.AppUtils;

/**
 * This class contains Flink SQL create table statements for the record classes
 * defined in the FoodDeliveryDomain class.
 */
public class Queries {

    /**
     * SQL statement to create the DeliveryETAEvent table.
     */
    public static final String CREATE_DELIVERY_ETA_TABLE = """
            CREATE TABLE IF NOT EXISTS `%s`.`%s` (
                orderId STRING,
                courierId STRING,
                restaurantId STRING,
                destinationLatitude DECIMAL(15, 2),
                destinationLongitude DECIMAL(15, 2),
                estimatedPrepTimeSeconds FLOAT,
                estimatedTravelTimeSeconds FLOAT,
                eventTimestamp TIMESTAMP(3),
                PRIMARY KEY (orderId) NOT ENFORCED
            ) WITH (
                'connector' = 'fluss',
                'table.type' = 'APPEND_ONLY'
            )
            """.formatted(AppUtils.FOOD_DELIVERY_DB, AppUtils.DELIVERY_ETA_TBL);

    /**
     * SQL statement to create the CourierLocationUpdate table.
     */
    public static final String CREATE_COURIER_LOCATION_TABLE = """
            CREATE TABLE IF NOT EXISTS `%s`.`%s` (
                courierId STRING,
                orderId STRING,
                courierLatitude DECIMAL(15, 2),
                courierLongitude DECIMAL(15, 2),
                speedKph DECIMAL(5, 2),
                isAvailable BOOLEAN,
                lastUpdatedTimestamp TIMESTAMP(3),
                PRIMARY KEY (courierId) NOT ENFORCED
            ) WITH (
                'connector' = 'fluss',
                'table.type' = 'APPEND_ONLY'
            )
            """.formatted(AppUtils.FOOD_DELIVERY_DB, AppUtils.COURIER_LOCATION_TBL);

    /**
     * SQL statement to create the RestaurantPrepStatus table.
     */
    public static final String CREATE_RESTAURANT_PREP_STATUS_TABLE = """
            CREATE TABLE IF NOT EXISTS `%s`.`%s` (
                restaurantId STRING,
                orderId STRING,
                currentQueueSize INT,
                actualPrepTimeSeconds FLOAT,
                estimatedPrepTimeSeconds FLOAT,
                lastUpdatedTimestamp TIMESTAMP(3),
                PRIMARY KEY (restaurantId) NOT ENFORCED
            ) WITH (
                'connector' = 'fluss',
                'table.type' = 'APPEND_ONLY'
            )
            """.formatted(AppUtils.FOOD_DELIVERY_DB, AppUtils.RESTAURANT_PREP_STATUS_TBL);

    /**
     * SQL statement to create the OrderPaymentInfo table.
     */
    public static final String CREATE_ORDER_PAYMENT_INFO_TABLE = """
            CREATE TABLE IF NOT EXISTS `%s`.`%s` (
                orderId STRING,
                totalAmount DECIMAL(5, 2),
                paymentMethod STRING,
                usedPromo BOOLEAN,
                isFirstOrder BOOLEAN,
                paymentTimestamp TIMESTAMP(3),
                PRIMARY KEY (orderId) NOT ENFORCED
            ) WITH (
                'connector' = 'fluss',
                'table.type' = 'APPEND_ONLY'
            )
            """.formatted(AppUtils.FOOD_DELIVERY_DB, AppUtils.ORDER_PAYMENT_INFO_TBL);

    /**
     * SQL statement to create the Wide Table that combines all fields from
     * OrderPaymentInfo, RestaurantPrepStatus, and CourierLocationUpdate.
     */
    public static final String CREATE_WIDE_TABLE = """
            CREATE TABLE IF NOT EXISTS `%s`.`%s` (
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
                restaurantLastUpdatedTimestamp TIMESTAMP(3),

                -- CourierLocationUpdate fields
                courierId STRING,
                courierLatitude DECIMAL(15, 2),
                courierLongitude DECIMAL(15, 2),
                speedKph DECIMAL(5, 2),
                isAvailable BOOLEAN,
                courierLastUpdatedTimestamp TIMESTAMP(3),

                PRIMARY KEY (orderId) NOT ENFORCED
            ) WITH (
                'connector' = 'fluss',
                'table.type' = 'APPEND_ONLY'
            )
            """.formatted(AppUtils.FOOD_DELIVERY_DB, AppUtils.WIDE_TABLE_TBL);
}
