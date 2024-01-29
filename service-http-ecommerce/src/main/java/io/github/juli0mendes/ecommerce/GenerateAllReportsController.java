package io.github.juli0mendes.ecommerce;

import io.github.juli0mendes.ecommercecore.KafkaDispatcher;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.ServletException;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/admin")
public class GenerateAllReportsController {

    @GetMapping("/generate-reports")
    public String generateReports() throws ServletException {
        try (var batchDispatcher = new KafkaDispatcher<String>()) {
            try {

                batchDispatcher.send("SEND_MESSAGE_TO_ALL_USERS", "USER_GENERATE_READING_REPORT", "USER_GENERATE_READING_REPORT");

                System.out.println("Sent generate report to all users");

                return "Reports requets generated";
            } catch (ExecutionException e) {
                throw new ServletException(e);
            } catch (InterruptedException e) {
                throw new ServletException(e);
            }
        }
    }
}
