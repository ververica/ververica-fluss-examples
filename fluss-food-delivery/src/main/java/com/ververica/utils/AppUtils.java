package com.ververica.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class AppUtils {

    public static final String BOOTSTRAP_SERVERS = "localhost:9123";

    public static final String FOOD_DELIVERY_DB = "food_delivery_db";

    public static final String COURIER_LOCATION_TBL = "courier_locations";
    public static final String DELIVERY_ETA_TBL = "delivery_eta_events";
    public static final String ORDER_PAYMENT_INFO_TBL = "order_payment_info";
    public static final String RESTAURANT_PREP_STATUS_TBL = "restaurant_prep_status";
    public static final String WIDE_TABLE_TBL = "wide_table";

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    /**
     * Converts an object to a JSON string.
     * 
     * @param object The object to convert
     * @return The JSON string representation of the object
     */
    public static String toJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            throw new RuntimeException("Error converting object to JSON", e);
        }
    }
}
