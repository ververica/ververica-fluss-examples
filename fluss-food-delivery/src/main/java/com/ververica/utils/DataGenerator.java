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

    /**
     * Generates a delivery ETA event with realistic preparation and travel times.
     * @return a DeliveryETAEvent with realistic time estimates
     */
    public static DeliveryETAEvent generateDeliveryETAEvent() {
        // Generate more realistic preparation time estimates
        long estimatedPrepTime;
        int prepTimeCategory = random.nextInt(100);

        if (prepTimeCategory < 10) {
            estimatedPrepTime = faker.number().numberBetween(180, 480);
        } else if (prepTimeCategory < 60) {
            estimatedPrepTime = faker.number().numberBetween(480, 900);
        } else if (prepTimeCategory < 90) {
            estimatedPrepTime = faker.number().numberBetween(900, 1500);
        } else {
            estimatedPrepTime = faker.number().numberBetween(1500, 2400);
        }

        long estimatedTravelTime;
        int travelCategory = random.nextInt(100);

        if (travelCategory < 20) {
            estimatedTravelTime = faker.number().numberBetween(300, 600);
        } else if (travelCategory < 60) {
            estimatedTravelTime = faker.number().numberBetween(600, 900);
        } else if (travelCategory < 85) {
            estimatedTravelTime = faker.number().numberBetween(900, 1500);
        } else if (travelCategory < 95) {
            estimatedTravelTime = faker.number().numberBetween(1500, 2100);
        } else {
            estimatedTravelTime = faker.number().numberBetween(2100, 2700);
        }

        return new DeliveryETAEvent(
                String.valueOf(totalOrders.getAndIncrement()),
                String.valueOf(random.nextInt(totalCouriers)),
                String.valueOf(random.nextInt(totalRestaurants)),
                Double.parseDouble(faker.address().latitude()),
                Double.parseDouble(faker.address().longitude()),
                estimatedPrepTime,
                estimatedTravelTime,
                LocalDateTime.now()
        );
    }

    public static PaymentMethod generateRandomPaymentMethod() {
        int randomValue = random.nextInt(100);

        if (randomValue < 45) {
            return new ApplePay(UUID.randomUUID().toString());
        }
        else if (randomValue < 80) {
            return new CreditCard(faker.business().creditCardNumber(), faker.name().fullName());
        }
        else if (randomValue < 90) {
            return new PayPal(faker.internet().emailAddress());
        }
        else {
            return new Revolut(UUID.randomUUID().toString());
        }
    }

    /**
     * Generates a random order payment info with realistic price distribution.
     * Most food delivery orders fall between $15-$45, with some outliers.
     * @return a random OrderPaymentInfo with realistic price
     */
    public static OrderPaymentInfo generateOrderPaymentInfo() {
        double price;
        int priceCategory = random.nextInt(100);

        if (priceCategory < 5) {
            price = faker.number().randomDouble(2, 8, 15);
        } else if (priceCategory < 70) {
            price = faker.number().randomDouble(2, 15, 30);
        } else if (priceCategory < 95) {
            price = faker.number().randomDouble(2, 30, 50);
        } else {
            price = faker.number().randomDouble(2, 50, 100);
        }

        price = Math.floor(price) + 0.99;

        return new OrderPaymentInfo(
                String.valueOf(random.nextInt(1000, totalOrders.get())),
                price,
                generateRandomPaymentMethod(),
                random.nextInt(100) < 30, // 30% chance of using a promo code
                random.nextInt(100) < 15, // 15% chance of being a first order
                LocalDateTime.now()
        );
    }

    public static CourierLocationUpdate generateCourierLocationUpdate() {
        double speed;
        int speedCategory = random.nextInt(100);

        if (speedCategory < 15) {
            speed = faker.number().randomDouble(1, 0, 5);
        } else if (speedCategory < 50) {
            speed = faker.number().randomDouble(1, 5, 15);
        } else if (speedCategory < 85) {
            speed = faker.number().randomDouble(1, 15, 30);
        } else {
            speed = faker.number().randomDouble(1, 30, 45);
        }

        return new CourierLocationUpdate(
                String.valueOf(random.nextInt(totalCouriers)),
                String.valueOf(random.nextInt(1000, totalOrders.get())),
                Double.parseDouble(faker.address().latitude()),
                Double.parseDouble(faker.address().longitude()),
                speed,
                random.nextInt(100) < 75, // 75% chance of being available (more realistic)
                LocalDateTime.now()
        );
    }

    public static RestaurantPrepStatus generateRestaurantPrepStatus() {
        long estimatedPrepTime;
        int prepTimeCategory = random.nextInt(100);

        if (prepTimeCategory < 10) {
            estimatedPrepTime = faker.number().numberBetween(180, 480);
        } else if (prepTimeCategory < 60) {
            estimatedPrepTime = faker.number().numberBetween(480, 900);
        } else if (prepTimeCategory < 90) {
            estimatedPrepTime = faker.number().numberBetween(900, 1500);
        } else {
            estimatedPrepTime = faker.number().numberBetween(1500, 2400);
        }

        int variance = random.nextInt(100);
        long actualPrepTime;

        if (variance < 20) {
            actualPrepTime = estimatedPrepTime - faker.number().numberBetween(60, 180);
        } else if (variance < 60) {
            actualPrepTime = estimatedPrepTime + faker.number().numberBetween(-60, 60);
        } else {
            actualPrepTime = estimatedPrepTime + faker.number().numberBetween(60, 600);
        }

        if (actualPrepTime < 120) actualPrepTime = 120; // minimum 2 minutes

        int queueSize;
        int queueCategory = random.nextInt(100);

        if (queueCategory < 30) {
            queueSize = random.nextInt(2);
        } else if (queueCategory < 70) {
            queueSize = random.nextInt(2, 5);
        } else if (queueCategory < 90) {
            queueSize = random.nextInt(5, 9);
        } else {
            queueSize = random.nextInt(9, 16);
        }

        return new RestaurantPrepStatus(
                String.valueOf(random.nextInt(totalRestaurants)),
                String.valueOf(random.nextInt(1000, totalOrders.get())),
                queueSize,
                actualPrepTime,
                estimatedPrepTime,
                LocalDateTime.now()
        );
    }
}
