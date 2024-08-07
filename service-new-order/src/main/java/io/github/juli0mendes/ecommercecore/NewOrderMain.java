package io.github.juli0mendes.ecommercecore;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderDipatcher = new KafkaDispatcher<Order>()) {
            try (var emailDipatcher = new KafkaDispatcher<Email>()) {
                var email = Math.random() + "@gmail.com";
                for (int i = 0; i < 10; i++) {
                    var orderId = UUID.randomUUID().toString();
                    var amount = new BigDecimal(Math.random() * 5000 + 1);

                    var order = new Order(orderId, amount, email);
                    orderDipatcher.send("ECOMMERCE_NEW_ORDER",
                            email,
                            new CorrelationId(NewOrderMain.class.getSimpleName()),
                            order);

                    var emailCode = new Email("fernanda@live.com", "Thank you for your order! We are processing your order");
                    emailDipatcher.send("ECOMMERCE_SEND_EMAIL",
                            email,
                            new CorrelationId(NewOrderMain.class.getSimpleName()),
                            emailCode);
                }
            }
        }
    }
}