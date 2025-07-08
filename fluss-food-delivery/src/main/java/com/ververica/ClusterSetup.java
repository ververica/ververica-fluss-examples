package com.ververica;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.ververica.models.FoodDeliveryDomain;
import com.ververica.utils.AppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class ClusterSetup {
    private static final Logger logger = LoggerFactory.getLogger(ClusterSetup.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        logger.info("Starting {}", ClusterSetup.class.getSimpleName());
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), AppUtils.BOOTSTRAP_SERVERS);
        Connection connection = ConnectionFactory.createConnection(conf);

        Admin admin = connection.getAdmin();

        DatabaseDescriptor descriptor =
                DatabaseDescriptor.builder()
                        .comment("This is the food delivery database")
                        .customProperty("owner", "JE")
                        .build();

        admin.createDatabase(AppUtils.FOOD_DELIVERY_DB, descriptor, true).get();
        admin.listDatabases().get().forEach(System.out::println);
        logger.info("Tables in '{}' database:", AppUtils.FOOD_DELIVERY_DB);
        admin.listTables(AppUtils.FOOD_DELIVERY_DB).get()
                .forEach(tbl -> {   logger.info("\t{}", tbl);   });

        admin.createTable(FoodDeliveryDomain.DeliveryETAEvent.tablePath(), FoodDeliveryDomain.DeliveryETAEvent.getDescriptor(), true).get();
        admin.createTable(FoodDeliveryDomain.RestaurantPrepStatus.tablePath(), FoodDeliveryDomain.RestaurantPrepStatus.getDescriptor(), true).get();
        admin.createTable(FoodDeliveryDomain.CourierLocationUpdate.tablePath(), FoodDeliveryDomain.CourierLocationUpdate.getDescriptor(), true).get();
        admin.createTable(FoodDeliveryDomain.OrderPaymentInfo.tablePath(), FoodDeliveryDomain.OrderPaymentInfo.getDescriptor(), true).get();

        logger.info("Tables in '{}' database:", AppUtils.FOOD_DELIVERY_DB);
        admin.listTables(AppUtils.FOOD_DELIVERY_DB).get()
                .forEach(tbl -> {   logger.info("\t{}", tbl);   });
    }
}
