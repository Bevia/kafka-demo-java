package com.example.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/** Java port of tracker.py: consumes order events from the "orders" topic. */
public class OrderTracker {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConfig.TRACKER_GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Ctrl+C handling (Python's KeyboardInterrupt): wakeup() makes poll() throw WakeupException
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n🔴 Stopping consumer");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        try {
            consumer.subscribe(List.of(KafkaConfig.ORDERS_TOPIC));
            System.out.println("🟢 Consumer is running and subscribed to " + KafkaConfig.ORDERS_TOPIC + " topic");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Order order = MAPPER.readValue(record.value(), Order.class);
                        System.out.printf("📦 Received order: %d x %s from %s%n",
                                order.quantity(), order.item(), order.user());
                    } catch (Exception e) {
                        System.out.println("❌ Error: could not parse message: " + e.getMessage());
                    }
                }
            }
        } catch (WakeupException e) {
            // expected on shutdown
        } finally {
            consumer.close();
        }
    }
}
