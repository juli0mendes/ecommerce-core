package io.github.juli0mendes.ecommercecore;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var dipatcher = new KafkaDipatcher()) {

            for (int i = 0; i < 10; i++) {
                var key = UUID.randomUUID().toString();

                var value = key + ", 123456789, 987654321, 00002";
                dipatcher.send("ECOMMERCE_NEW_ORDER", key, value);

                var email = "Thank you for your order! We are processing your order";
                dipatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
            }
        }
    }
}