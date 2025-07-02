package com.ververica.models;

import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataTypes;
import com.ververica.utils.AppUtils;
import java.time.LocalDateTime;

public class FoodDeliveryDomain {
    public record DeliveryETAEvent(
            String orderId,
            String courierId,
            String restaurantId,
            double destinationLatitude,
            double destinationLongitude,
            long estimatedPrepTimeSeconds,
            long estimatedTravelTimeSeconds,
            LocalDateTime eventTimestamp
    ) {
        public GenericRow toGenericRow() {
            GenericRow row = new GenericRow(DeliveryETAEvent.class.getDeclaredFields().length);
            row.setField(0, BinaryString.fromString(orderId));
            row.setField(1, BinaryString.fromString(courierId));
            row.setField(2, BinaryString.fromString(restaurantId));
            row.setField(3, destinationLatitude);
            row.setField(4, destinationLongitude);
            row.setField(5, estimatedPrepTimeSeconds);
            row.setField(6, estimatedTravelTimeSeconds);
            row.setField(7, TimestampNtz.fromLocalDateTime(eventTimestamp));
            return row;
        }
        public static TableDescriptor getDescriptor() {
            return TableDescriptor.builder()
                    .schema(getSchema())
                    .distributedBy(3, "orderId")
                    .comment("This is the DeliveryETAEvents table")
                    .property("table.datalake.enabled", "true")
                    .property("table.datalake.freshness", "30s")
                    .build();
        }

        public static TablePath tablePath() {
            return new TablePath(AppUtils.FOOD_DELIVERY_DB, AppUtils.DELIVERY_ETA_TBL);
        }

        private static Schema getSchema() {
            return Schema.newBuilder()
                    .column("orderId", DataTypes.STRING())
                    .column("courierId", DataTypes.STRING())
                    .column("restaurantId", DataTypes.STRING())
                    .column("destinationLatitude", DataTypes.DOUBLE())
                    .column("destinationLongitude", DataTypes.DOUBLE())
                    .column("estimatedPrepTimeSeconds", DataTypes.BIGINT())
                    .column("estimatedTravelTimeSeconds", DataTypes.BIGINT())
                    .column("eventTimestamp", DataTypes.TIMESTAMP())
                    .build();
        }
    }

    public record CourierLocationUpdate(
        String courierId,
        String orderId,
        double courierLatitude,
        double courierLongitude,
        double speedKph,
        boolean isAvailable,
        LocalDateTime lastUpdatedTimestamp
    ) {
        public GenericRow toGenericRow() {
            GenericRow row = new GenericRow(CourierLocationUpdate.class.getDeclaredFields().length);
            row.setField(0, BinaryString.fromString(courierId));
            row.setField(1, BinaryString.fromString(orderId));
            row.setField(2, courierLatitude);
            row.setField(3, courierLongitude);
            row.setField(4, speedKph);
            row.setField(5, isAvailable);
            row.setField(6, TimestampNtz.fromLocalDateTime(lastUpdatedTimestamp));
            return row;
        }

        public static TableDescriptor getDescriptor() {
            return TableDescriptor.builder()
                    .schema(getSchema())
                    .distributedBy(3, "courierId")
                    .comment("This is the CourierLocationUpdates table")
                    .property("table.datalake.enabled", "true")
                    .property("table.datalake.freshness", "30s")
                    .build();
        }

        public static TablePath tablePath() {
            return new TablePath(AppUtils.FOOD_DELIVERY_DB, AppUtils.COURIER_LOCATION_TBL);
        }

        private static Schema getSchema() {
            return Schema.newBuilder()
                    .column("courierId", DataTypes.STRING())
                    .column("orderId", DataTypes.STRING())
                    .column("courierLatitude", DataTypes.DOUBLE())
                    .column("courierLongitude", DataTypes.DOUBLE())
                    .column("speedKph", DataTypes.DOUBLE())
                    .column("isAvailable", DataTypes.BOOLEAN())
                    .column("lastUpdatedTimestamp", DataTypes.TIMESTAMP())
                    .primaryKey("courierId")
                    .build();
        }
    }

    public record RestaurantPrepStatus(
            String restaurantId,
            String orderId,
            int currentQueueSize,
            long actualPrepTimeSeconds,
            long estimatedPrepTimeSeconds,
            LocalDateTime lastUpdatedTimestamp
    ){
        public GenericRow toGenericRow() {
            GenericRow row = new GenericRow(RestaurantPrepStatus.class.getDeclaredFields().length);
            row.setField(0, BinaryString.fromString(restaurantId));
            row.setField(1, BinaryString.fromString(orderId));
            row.setField(2, currentQueueSize);
            row.setField(3, actualPrepTimeSeconds);
            row.setField(4, estimatedPrepTimeSeconds);
            row.setField(5, TimestampNtz.fromLocalDateTime(lastUpdatedTimestamp));
            return row;
        }
        public static TableDescriptor getDescriptor() {
            return TableDescriptor.builder()
                    .schema(getSchema())
                    .distributedBy(3, "restaurantId")
                    .comment("This is the RestaurantPrepStatus table")
                    .property("table.datalake.enabled", "true")
                    .property("table.datalake.freshness", "30s")
                    .build();
        }

        public static TablePath tablePath() {
            return new TablePath(AppUtils.FOOD_DELIVERY_DB, AppUtils.RESTAURANT_PREP_STATUS_TBL);
        }

        private static Schema getSchema() {
            return Schema.newBuilder()
                    .column("restaurantId", DataTypes.STRING())
                    .column("orderId", DataTypes.STRING())
                    .column("currentQueueSize", DataTypes.INT())
                    .column("actualPrepTimeSeconds", DataTypes.BIGINT())
                    .column("estimatedPrepTimeSeconds", DataTypes.BIGINT())
                    .column("lastUpdatedTimestamp", DataTypes.TIMESTAMP())
                    .primaryKey("restaurantId")
                    .build();
        }
    }

    public record OrderPaymentInfo(
            String orderId,
            double totalAmount,
            PaymentMethod paymentMethod,
            boolean usedPromo,
            boolean isFirstOrder,
            LocalDateTime paymentTimestamp
    ) {
        public GenericRow toGenericRow() {
            GenericRow row = new GenericRow(OrderPaymentInfo.class.getDeclaredFields().length);
            row.setField(0, BinaryString.fromString(orderId));
            row.setField(1, totalAmount);
            row.setField(2, BinaryString.fromString(paymentMethod.getName()));
            row.setField(3, usedPromo);
            row.setField(4, isFirstOrder);
            row.setField(5, TimestampNtz.fromLocalDateTime(paymentTimestamp));
            return row;
        }
        public static TableDescriptor getDescriptor() {
            return TableDescriptor.builder()
                    .schema(getSchema())
                    .distributedBy(3, "orderId")
                    .comment("This is the OrderPaymentInfo table")
                    .property("table.datalake.enabled", "true")
                    .property("table.datalake.freshness", "30s")
                    .build();
        }

        public static TablePath tablePath() {
            return new TablePath(AppUtils.FOOD_DELIVERY_DB, AppUtils.ORDER_PAYMENT_INFO_TBL);
        }

        private static Schema getSchema() {
            return Schema.newBuilder()
                    .column("orderId", DataTypes.STRING())
                    .column("totalAmount", DataTypes.DOUBLE())
                    .column("paymentMethod", DataTypes.STRING())
                    .column("usedPromo", DataTypes.BOOLEAN())
                    .column("isFirstOrder", DataTypes.BOOLEAN())
                    .column("paymentTimestamp", DataTypes.TIMESTAMP()   )
                    .primaryKey("orderId")
                    .build();
        }
    }
}
