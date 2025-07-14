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
            // 10% chance for very quick prep (fast food, pre-made items) - 3-8 minutes
            estimatedPrepTime = faker.number().numberBetween(180, 480);
        } else if (prepTimeCategory < 60) {
            // 50% chance for standard prep time - 8-15 minutes
            estimatedPrepTime = faker.number().numberBetween(480, 900);
        } else if (prepTimeCategory < 90) {
            // 30% chance for longer prep time (complex dishes) - 15-25 minutes
            estimatedPrepTime = faker.number().numberBetween(900, 1500);
        } else {
            // 10% chance for very long prep time (special orders, busy times) - 25-40 minutes
            estimatedPrepTime = faker.number().numberBetween(1500, 2400);
        }

        // Generate more realistic travel time estimates based on distance and urban conditions
        long estimatedTravelTime;
        int travelCategory = random.nextInt(100);

        if (travelCategory < 20) {
            // 20% chance for very short distance - 5-10 minutes
            estimatedTravelTime = faker.number().numberBetween(300, 600);
        } else if (travelCategory < 60) {
            // 40% chance for short distance - 10-15 minutes
            estimatedTravelTime = faker.number().numberBetween(600, 900);
        } else if (travelCategory < 85) {
            // 25% chance for medium distance - 15-25 minutes
            estimatedTravelTime = faker.number().numberBetween(900, 1500);
        } else if (travelCategory < 95) {
            // 10% chance for long distance - 25-35 minutes
            estimatedTravelTime = faker.number().numberBetween(1500, 2100);
        } else {
            // 5% chance for very long distance - 35-45 minutes
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

    /**
     * Generates a random payment method with realistic distribution.
     * Apple Pay and Credit Cards are more common (45% and 35% respectively),
     * while PayPal and Revolut are less common (10% each).
     * @return a random implementation of PaymentMethod with realistic distribution
     */
    public static PaymentMethod generateRandomPaymentMethod() {
        int randomValue = random.nextInt(100);

        // 45% chance for Apple Pay
        if (randomValue < 45) {
            return new ApplePay(UUID.randomUUID().toString());
        }
        // 35% chance for Credit Card
        else if (randomValue < 80) {
            return new CreditCard(faker.business().creditCardNumber(), faker.name().fullName());
        }
        // 10% chance for PayPal
        else if (randomValue < 90) {
            return new PayPal(faker.internet().emailAddress());
        }
        // 10% chance for Revolut
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
        // Generate a more realistic price distribution for food delivery
        double price;
        int priceCategory = random.nextInt(100);

        if (priceCategory < 5) {
            // 5% chance for small orders (snacks, coffee, etc.) - $8-$15
            price = faker.number().randomDouble(2, 8, 15);
        } else if (priceCategory < 70) {
            // 65% chance for medium orders (single meal) - $15-$30
            price = faker.number().randomDouble(2, 15, 30);
        } else if (priceCategory < 95) {
            // 25% chance for large orders (family meals) - $30-$50
            price = faker.number().randomDouble(2, 30, 50);
        } else {
            // 5% chance for very large orders (parties, catering) - $50-$100
            price = faker.number().randomDouble(2, 50, 100);
        }

        // Round to nearest $0.99 to make it look more like real prices
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

    /**
     * Generates a random courier location update with realistic speed distribution.
     * Speeds are distributed to reflect realistic urban delivery patterns.
     * @return a random CourierLocationUpdate with realistic speed
     */
    public static CourierLocationUpdate generateCourierLocationUpdate() {
        // Generate a more realistic speed distribution for food delivery couriers
        double speed;
        int speedCategory = random.nextInt(100);

        if (speedCategory < 15) {
            // 15% chance for very slow speed (traffic jam, waiting at restaurant/customer) - 0-5 kph
            speed = faker.number().randomDouble(1, 0, 5);
        } else if (speedCategory < 50) {
            // 35% chance for slow speed (dense urban areas, pedestrian zones) - 5-15 kph
            speed = faker.number().randomDouble(1, 5, 15);
        } else if (speedCategory < 85) {
            // 35% chance for medium speed (normal urban traffic) - 15-30 kph
            speed = faker.number().randomDouble(1, 15, 30);
        } else {
            // 15% chance for high speed (open roads, highways) - 30-45 kph
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

    /**
     * Generates a random restaurant preparation status with realistic preparation times.
     * Preparation times are distributed based on real-world restaurant operations.
     * @return a random RestaurantPrepStatus with realistic preparation times
     */
    public static RestaurantPrepStatus generateRestaurantPrepStatus() {
        // Generate a more realistic preparation time distribution
        long estimatedPrepTime;
        int prepTimeCategory = random.nextInt(100);

        if (prepTimeCategory < 10) {
            // 10% chance for very quick prep (fast food, pre-made items) - 3-8 minutes
            estimatedPrepTime = faker.number().numberBetween(180, 480);
        } else if (prepTimeCategory < 60) {
            // 50% chance for standard prep time - 8-15 minutes
            estimatedPrepTime = faker.number().numberBetween(480, 900);
        } else if (prepTimeCategory < 90) {
            // 30% chance for longer prep time (complex dishes) - 15-25 minutes
            estimatedPrepTime = faker.number().numberBetween(900, 1500);
        } else {
            // 10% chance for very long prep time (special orders, busy times) - 25-40 minutes
            estimatedPrepTime = faker.number().numberBetween(1500, 2400);
        }

        // Actual prep time has a bias toward being longer than estimated
        // (restaurants tend to underestimate rather than overestimate)
        int variance = random.nextInt(100);
        long actualPrepTime;

        if (variance < 20) {
            // 20% chance of being faster than estimated (by 1-3 minutes)
            actualPrepTime = estimatedPrepTime - faker.number().numberBetween(60, 180);
        } else if (variance < 60) {
            // 40% chance of being close to estimate (within 1 minute)
            actualPrepTime = estimatedPrepTime + faker.number().numberBetween(-60, 60);
        } else {
            // 40% chance of being slower than estimated (by 1-10 minutes)
            actualPrepTime = estimatedPrepTime + faker.number().numberBetween(60, 600);
        }

        // Ensure minimum prep time is reasonable
        if (actualPrepTime < 120) actualPrepTime = 120; // minimum 2 minutes

        // Generate a more realistic queue size distribution
        int queueSize;
        int queueCategory = random.nextInt(100);

        if (queueCategory < 30) {
            // 30% chance for no queue or very small queue (0-1 orders)
            queueSize = random.nextInt(2);
        } else if (queueCategory < 70) {
            // 40% chance for small queue (2-4 orders)
            queueSize = random.nextInt(2, 5);
        } else if (queueCategory < 90) {
            // 20% chance for medium queue (5-8 orders)
            queueSize = random.nextInt(5, 9);
        } else {
            // 10% chance for large queue (9-15 orders) - busy times
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
