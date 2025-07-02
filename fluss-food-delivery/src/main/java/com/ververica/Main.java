package com.ververica;


import com.ververica.utils.AppUtils;
import com.ververica.utils.DataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        logger.info("Food Delivery Demo");
        logger.info("===================");

        logger.info("Generating data...");

        logger.info(AppUtils.toJson(DataGenerator.generateCourierLocationUpdate()));
        logger.info(AppUtils.toJson(DataGenerator.generateDeliveryETAEvent()));
        logger.info(AppUtils.toJson(DataGenerator.generateDeliveryETAEvent()));
        logger.info(AppUtils.toJson(DataGenerator.generateDeliveryETAEvent()));
        logger.info(AppUtils.toJson(DataGenerator.generateOrderPaymentInfo()));
        logger.info(AppUtils.toJson(DataGenerator.generateRestaurantPrepStatus()));
    }
}
