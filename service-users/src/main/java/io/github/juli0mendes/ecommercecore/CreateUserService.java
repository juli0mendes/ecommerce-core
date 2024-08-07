package io.github.juli0mendes.ecommercecore;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

public class CreateUserService {

    private final Connection connection;

    public CreateUserService() throws SQLException {
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

        var createUserService = new CreateUserService();
        try (var service = new KafkaService<>(
                CreateUserService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                createUserService::parse,
                Order.class,
                Map.of())) {

            service.run();
        }
    }


    private void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
        System.out.println("-----------------------------");
        System.out.println("Procesing new order, checking for new user");
        System.out.println(record.value());

        var order = record.value().getPayload();

        if (this.isNewUser(order.getEmail())) {
            this.insertNewUser(order.getEmail());
        }
    }

    private void insertNewUser(String email) throws SQLException {
        var insert = this.connection.prepareStatement("insert into Users (uuid, email) " +
                "values (?, ?) ");
        insert.setString(1, UUID.randomUUID().toString());
        insert.setString(2, email);
        insert.execute();

        System.out.println("User uuid and " + email + " adicionado.");

    }

    private boolean isNewUser(String email) throws SQLException {
        var exists = this.connection.prepareStatement("select uuid from Users " +
                "where email = ? limit 1");
        exists.setString(1, email);

        var results = exists.executeQuery();
        return !results.next();
    }
}
