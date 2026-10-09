package com.example.kafka.producer;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.model.Order;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

/**
 * Java port of producer.py: sends order events to the "orders" topic.
 *
 * <p>Usage: {@code OrderProducer [posId] [count]} (defaults: POS-001, 1).
 * The POS id is used as the message key, so every order for the same terminal
 * goes to the same partition and keeps its order.
 */
public class OrderProducer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String posId = args.length > 0 ? args[0] : "POS-001";
        int count = args.length > 1 ? Integer.parseInt(args[1]) : 1;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // try-with-resources closes the producer, which flushes pending messages (like producer.flush())
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                Order order = new Order(
                        UUID.randomUUID().toString(),
                        posId,
                        "Vincent",
                        "New Windows Computer",
                        1);

                String value = MAPPER.writeValueAsString(order);

                // key = posId -> same POS, same partition, ordering preserved per terminal
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(KafkaConfig.ORDERS_TOPIC, posId, value);

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
            }

            producer.flush();
        }
    }
}
