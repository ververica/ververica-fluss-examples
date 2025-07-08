package com.ververica.writer;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.row.GenericRow;
import com.ververica.utils.AppUtils;
import com.ververica.utils.DataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ververica.models.FoodDeliveryDomain.CourierLocationUpdate;
import com.ververica.models.FoodDeliveryDomain.DeliveryETAEvent;
import com.ververica.models.FoodDeliveryDomain.OrderPaymentInfo;
import com.ververica.models.FoodDeliveryDomain.RestaurantPrepStatus;


import java.util.concurrent.ExecutionException;

public class FoodDeliveryWriter {
    private static final Logger logger = LoggerFactory.getLogger(FoodDeliveryWriter.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        logger.info("Starting {}", FoodDeliveryWriter.class.getSimpleName());
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), AppUtils.BOOTSTRAP_SERVERS);
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = connection.getTable(DeliveryETAEvent.tablePath());
        AppendWriter writer = table.newAppend().createWriter();

        new Thread(() -> {
            logger.info("Generating DeliveryETAEvent events...");
            for (int i = 0; i <= 20_000_000; i++) {
                DeliveryETAEvent deliveryETAEvent = DataGenerator.generateDeliveryETAEvent();
                GenericRow genericRow = deliveryETAEvent.toGenericRow();
                writer.append(genericRow);
                if (i % 1_000_000 == 0) {
                    logger.info("Total DeliveryETAEvent events so far: {}.", i);
                }
            }
            writer.flush();
        }).start();

        Table courierLocationUpdateTable = connection.getTable(CourierLocationUpdate.tablePath());
        Table restaurantPrepStatusTable = connection.getTable(RestaurantPrepStatus.tablePath());
        Table orderPaymentInfoTable = connection.getTable(OrderPaymentInfo.tablePath());

        UpsertWriter courierUpsertWriter = courierLocationUpdateTable.newUpsert().createWriter();
        UpsertWriter restaurantUpsertWriter = restaurantPrepStatusTable.newUpsert().createWriter();
        UpsertWriter paymentsUpsertWriter = orderPaymentInfoTable.newUpsert().createWriter();

        new Thread(() -> {
            logger.info("Generating CourierLocationUpdate update events...");
            while (true) {
                CourierLocationUpdate courierLocationUpdate = DataGenerator.generateCourierLocationUpdate();
                GenericRow genericRow = courierLocationUpdate.toGenericRow();
                courierUpsertWriter.upsert(genericRow);
            }
        }).start();

        new Thread(() -> {
            logger.info("Generating OrderPaymentInfo update events...");
            while (true) {
                OrderPaymentInfo orderPaymentInfo = DataGenerator.generateOrderPaymentInfo();
                GenericRow genericRow = orderPaymentInfo.toGenericRow();
                paymentsUpsertWriter.upsert(genericRow);
            }
        }).start();

        new Thread(() -> {
            logger.info("Generating RestaurantPrepStatus update events...");
            while (true) {
                RestaurantPrepStatus restaurantPrepStatus = DataGenerator.generateRestaurantPrepStatus();
                GenericRow genericRow = restaurantPrepStatus.toGenericRow();
                restaurantUpsertWriter.upsert(genericRow);
            }
        }).start();
//        try {
//            admin.close();
//            connection.close();
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
    }
}
