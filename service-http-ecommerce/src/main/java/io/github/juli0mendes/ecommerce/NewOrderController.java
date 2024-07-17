package io.github.juli0mendes.ecommerce;

import io.github.juli0mendes.ecommercecore.CorrelationId;
import io.github.juli0mendes.ecommercecore.KafkaDispatcher;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.ServletException;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api")
public class NewOrderController {

    @GetMapping("/new")
    public String newOrder(@RequestParam(value = "email") String email,
                           @RequestParam(value = "amount") BigDecimal amount) throws ServletException, IOException {
        try (var orderDipatcher = new KafkaDispatcher<Order>()) {
            try (var emailDipatcher = new KafkaDispatcher<Email>()) {
                try {
                    var orderId = UUID.randomUUID().toString();
                    var order = new Order(orderId, amount, email);
                    var emailCode = new Email("fernanda@live.com", "Thank you for your order! We are processing your order");

                    orderDipatcher.send("ECOMMERCE_NEW_ORDER",
                            email,
                            new CorrelationId(NewOrderController.class.getSimpleName()),
                            order);

                    emailDipatcher.send("ECOMMERCE_SEND_EMAIL",
                            email,
                            new CorrelationId(NewOrderController.class.getSimpleName()),
                            emailCode);

                    System.out.println("New order sent successfully");

                    return "New order sent";
                } catch (ExecutionException e) {
                    throw new ServletException(e);
                } catch (InterruptedException e) {
                    throw new ServletException(e);
                }
            }
        }
    }
}
