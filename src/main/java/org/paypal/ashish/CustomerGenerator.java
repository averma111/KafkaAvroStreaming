package org.paypal.ashish;

import org.paypal.ashish.model.Customer;

public class CustomerGenerator {

    private CustomerGenerator() {

    }

    private static long counter = 1;

    public static Customer getNext() {
        final String customer = "customer" + counter++;
        return new Customer(customer, customer + "@virtualcompany.com", "+92 0333 325179" + customer);
    }
}
