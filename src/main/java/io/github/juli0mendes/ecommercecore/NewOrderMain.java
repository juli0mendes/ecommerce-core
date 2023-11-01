package io.github.juli0mendes.ecommercecore;


import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderDipatcher = new KafkaDipatcher<Order>()) {
            try (var emailDipatcher = new KafkaDipatcher<Email>()) {

                for (int i = 0; i < 10; i++) {
                    var userId = UUID.randomUUID().toString();
                    var orderId = UUID.randomUUID().toString();
                    var amount = new BigDecimal(Math.random() * 5000 + 1);

                    var order = new Order(userId, orderId, amount);
                    orderDipatcher.send("ECOMMERCE_NEW_ORDER", userId, order);

                    var email = new Email("fernanda@live.com", "Thank you for your order! We are processing your order");
                    emailDipatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
                }
            }
        }
    }
}