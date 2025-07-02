package com.ververica.utils;

import com.github.javafaker.Faker;
import com.ververica.models.FoodDeliveryDomain.CourierLocationUpdate;
import com.ververica.models.FoodDeliveryDomain.DeliveryETAEvent;
import com.ververica.models.FoodDeliveryDomain.OrderPaymentInfo;
import com.ververica.models.FoodDeliveryDomain.RestaurantPrepStatus;
import com.ververica.models.PaymentMethod;
import com.ververica.models.PaymentMethod.CreditCard;
import com.ververica.models.PaymentMethod.Revolut;
import com.ververica.models.PaymentMethod.PayPal;
import com.ververica.models.PaymentMethod.ApplePay;

import java.time.LocalDateTime;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class DataGenerator {
    private static final Random random = new Random();
    private static final Faker faker = new Faker();


    private static final int totalCouriers = 100_000;
    private static final int totalRestaurants = 10_000;
    private static final AtomicInteger totalOrders = new AtomicInteger(1001);

    public static DeliveryETAEvent generateDeliveryETAEvent() {
        return new DeliveryETAEvent(
                String.valueOf(totalOrders.getAndIncrement()),
                String.valueOf(random.nextInt(totalCouriers)),
                String.valueOf(random.nextInt(totalRestaurants)),
                Double.parseDouble(faker.address().latitude()),
                Double.parseDouble(faker.address().longitude()),
                faker.number().numberBetween(300, 1200),
                faker.number().numberBetween(600, 1800),
                LocalDateTime.now()
        );
    }

    /**
     * Generates a random payment method.
     * @return a random implementation of PaymentMethod
     */
    public static PaymentMethod generateRandomPaymentMethod() {
        int methodType = random.nextInt(4);
        return switch (methodType) {
            case 0 -> new CreditCard(faker.business().creditCardNumber(), faker.name().fullName());
            case 1 -> new Revolut(UUID.randomUUID().toString());
            case 2 -> new PayPal(faker.internet().emailAddress());
            case 3 -> new ApplePay(UUID.randomUUID().toString());
            default -> throw new IllegalStateException("Unexpected value: " + methodType);
        };
    }

    /**
     * Generates a random order payment info.
     * @return a random OrderPaymentInfo
     */
    public static OrderPaymentInfo generateOrderPaymentInfo() {
        return new OrderPaymentInfo(
                String.valueOf(random.nextInt(1000, totalOrders.get())),
                faker.number().randomDouble(2, 10, 100),
                generateRandomPaymentMethod(),
                random.nextBoolean(),
                random.nextBoolean(),
                LocalDateTime.now()
        );
    }

    /**
     * Generates a random courier location update.
     * @return a random CourierLocationUpdate
     */
    public static CourierLocationUpdate generateCourierLocationUpdate() {
        return new CourierLocationUpdate(
                String.valueOf(random.nextInt(totalCouriers)),
                String.valueOf(random.nextInt(1000, totalOrders.get())),
                Double.parseDouble(faker.address().latitude()),
                Double.parseDouble(faker.address().longitude()),
                faker.number().randomDouble(2, 5, 60), // speed in kph between 5 and 60
                random.nextBoolean(), // is available
                LocalDateTime.now()
        );
    }

    /**
     * Generates a random restaurant preparation status.
     * @return a random RestaurantPrepStatus
     */
    public static RestaurantPrepStatus generateRestaurantPrepStatus() {
        long estimatedPrepTime = faker.number().numberBetween(300, 1200); // 5-20 minutes in seconds
        long actualPrepTime = estimatedPrepTime + faker.number().numberBetween(-180, 300); // +/- 3-5 minutes variance
        if (actualPrepTime < 120) actualPrepTime = 120; // minimum 2 minutes

        return new RestaurantPrepStatus(
                String.valueOf(random.nextInt(totalRestaurants)),
                String.valueOf(random.nextInt(1000, totalOrders.get())),
                faker.number().numberBetween(0, 15), // current queue size
                actualPrepTime,
                estimatedPrepTime,
                LocalDateTime.now()
        );
    }
}
