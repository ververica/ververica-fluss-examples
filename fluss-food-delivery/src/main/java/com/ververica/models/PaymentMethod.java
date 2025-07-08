package com.ververica.models;

/**
 * A sealed interface representing different payment methods.
 * Only the specified implementations are allowed to implement this interface.
 */
public sealed interface PaymentMethod permits 
    PaymentMethod.CreditCard, 
    PaymentMethod.Revolut, 
    PaymentMethod.PayPal, 
    PaymentMethod.ApplePay {
    
    /**
     * Returns a string representation of the payment method.
     * @return the payment method name
     */
    String getName();
    
    /**
     * Credit card payment method implementation.
     */
    record CreditCard(String cardNumber, String cardholderName) implements PaymentMethod {
        @Override
        public String getName() {
            return "Credit Card";
        }
    }
    
    /**
     * Revolut payment method implementation.
     */
    record Revolut(String accountId) implements PaymentMethod {
        @Override
        public String getName() {
            return "Revolut";
        }
    }
    
    /**
     * PayPal payment method implementation.
     */
    record PayPal(String email) implements PaymentMethod {
        @Override
        public String getName() {
            return "PayPal";
        }
    }
    
    /**
     * Apple Pay payment method implementation.
     */
    record ApplePay(String deviceId) implements PaymentMethod {
        @Override
        public String getName() {
            return "Apple Pay";
        }
    }
}