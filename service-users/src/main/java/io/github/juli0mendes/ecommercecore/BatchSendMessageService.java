package io.github.juli0mendes.ecommercecore;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {

    private final Connection connection;

    public BatchSendMessageService() throws SQLException {

        String url = "jdbc:sqlite:service-users/target/users_database.db";

        this.connection = DriverManager.getConnection(url);

        try {
            connection.createStatement()
                    .execute("create table Users (" +
                            "uuid varchat(200) primary key," +
                            "email varchar(200))");
        } catch (SQLException ex) {
            // be careful, the sql could be wrong, be really careful
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {

        var batchService = new BatchSendMessageService();

        try (var service = new KafkaService<>(
                BatchSendMessageService.class.getSimpleName(),
                "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                batchService::parse,
                String.class,
                Map.of())) {

            service.run();
        }
    }

    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<User>();

    private void parse(ConsumerRecord<String, Message<String>> record) throws SQLException, ExecutionException, InterruptedException {

        System.out.println("-----------------------------");
        System.out.println("Procesing new batch");
        var message = record.value();
        System.out.println("Payload: " + message.getPayload());

        for (User user : getAllUsers()) {
            userDispatcher.send(message.getPayload(),
                    user.getUuid(),
                    message.getId().continueWith(new CorrelationId(BatchSendMessageService.class.getSimpleName())),
                    user);
        }
    }

    private List<User> getAllUsers() throws SQLException {

        var results = this.connection.prepareStatement("select uuid from Users").executeQuery();

        List<User> users = new ArrayList<>();
        while (results.next()) {
            users.add(new User(results.getString(1)));
        }

        return users;
    }
}
