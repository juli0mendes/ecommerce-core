package io.github.juli0mendes.ecommercecore;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, Message<T>> consumer;
    private final ConsumerFunction parse;

    private KafkaService(String groupId, ConsumerFunction<T> parse, Class<T> type, Map<String, String> properties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(this.getProperties(type, groupId, properties));
    }

    public KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Class<T> type, Map<String, String> properties) {
        this(groupId, parse, type, properties);
        consumer.subscribe(Collections.singleton(topic));
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Class<T> type, Map<String, String> properties) {
        this(groupId, parse, type, properties);
        consumer.subscribe(topic);
    }

    public void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.println("Exists" + records.count() + " records");

                for (var record : records) {
                    try {
                        parse.consume(record);
                    } catch (Exception e) {
                        // only catches exception because no matter witch exception
                        // i want to recover and parse the next one
                        // so far, just logging the exception for this message
                        e.printStackTrace();
                    }
                }
                continue;
            }
        }
    }

    private Properties getProperties(Class<T> type, String groupId, Map<String, String> overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
