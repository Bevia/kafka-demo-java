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

/**
 * Consumes "order-status": shows what happened to each order after the gateway took it.
 * Stands in for any downstream service (order service, dashboard, alerting) that needs to know.
 */
public class OrderStatusMonitor {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConfig.STATUS_MONITOR_GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n🔴 Stopping status monitor");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        try {
            consumer.subscribe(List.of(KafkaConfig.ORDER_STATUS_TOPIC));
            System.out.println("🟢 Status monitor subscribed to " + KafkaConfig.ORDER_STATUS_TOPIC);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        OrderStatus s = MAPPER.readValue(record.value(), OrderStatus.class);
                        String icon = switch (s.status()) {
                            case QUEUED -> "🕓";
                            case DELIVERED -> "✅";
                            case FAILED -> "❌";
                        };
                        System.out.printf("%s %-9s order %s  pos %s  (%s)%n",
                                icon, s.status(), s.orderId(), s.posId(), s.detail());
                    } catch (Exception e) {
                        System.out.println("❌ Error: could not parse status: " + e.getMessage());
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
