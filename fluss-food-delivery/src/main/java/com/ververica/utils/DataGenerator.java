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


    private static final int totalCouriers = 1000;
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
     * Generates a random payment method with realistic distribution:
     * - Credit Card: ~50% (most common payment method)
     * - Apple Pay: ~30% (popular mobile payment)
     * - PayPal: ~15% (common online payment)
     * - Revolut: ~5% (less common)
     * @return a random implementation of PaymentMethod with realistic distribution
     */
    public static PaymentMethod generateRandomPaymentMethod() {
        int randomValue = random.nextInt(100);


        if (randomValue < 50) {
            return new CreditCard(faker.business().creditCardNumber(), faker.name().fullName());
        } else if (randomValue < 80) {
            return new ApplePay(UUID.randomUUID().toString());
        } else if (randomValue < 95) {
            return new PayPal(faker.internet().emailAddress());
        } else {
            return new Revolut(UUID.randomUUID().toString());
        }
    }

    public static OrderPaymentInfo generateOrderPaymentInfo() {
        int randomValue = random.nextInt(100);
        double totalAmount;


        if (randomValue < 70) {
            totalAmount = faker.number().randomDouble(2, 15, 50);
        }

        else if (randomValue < 95) {
            totalAmount = faker.number().randomDouble(2, 50, 80);
        }

        else {
            totalAmount = faker.number().randomDouble(2, 80, 120);
        }

        return new OrderPaymentInfo(
                String.valueOf(random.nextInt(1000, totalOrders.get())),
                totalAmount,
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
        if (actualPrepTime < 120) actualPrepTime = 120;

        return new RestaurantPrepStatus(
                String.valueOf(random.nextInt(totalRestaurants)),
                String.valueOf(random.nextInt(1000, totalOrders.get())),
                faker.number().numberBetween(0, 15),
                actualPrepTime,
                estimatedPrepTime,
                LocalDateTime.now()
        );
    }
}
