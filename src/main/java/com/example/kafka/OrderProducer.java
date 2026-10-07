package com.example.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

/** Java port of producer.py: sends one order event to the "orders" topic. */
public class OrderProducer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Order order = new Order(
                UUID.randomUUID().toString(),
                "Vincent",
                "Office table",
                1);

        String value = MAPPER.writeValueAsString(order);

        // try-with-resources closes the producer, which flushes pending messages (like producer.flush())
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConfig.ORDERS_TOPIC, value);

            // Callback = Python's delivery_report
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("❌ Delivery failed: " + exception.getMessage());
                } else {
                    System.out.println("✅ Delivered " + value);
                    System.out.printf("✅ Delivered to %s : partition %d : at offset %d%n",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });

            producer.flush();
        }
    }
}
