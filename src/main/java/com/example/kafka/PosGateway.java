package com.example.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Consumes "orders" (group pos-gateway) and delivers each order to its POS terminal over WebSocket.
 *
 * <p>Key idea: the Kafka offset is committed as soon as the order is safely in the gateway's
 * pending store, NOT when the POS acks. So an offline POS never blocks other terminals that share
 * the partition. Delivery to the POS is tracked separately and reported on "order-status".
 */
public class PosGateway {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        KafkaProducer<String, String> statusProducer = new KafkaProducer<>(producerProps());
        DeliveryManager delivery = new DeliveryManager(statusProducer);

        PosWebSocketServer webSocketServer = new PosWebSocketServer(KafkaConfig.GATEWAY_WS_PORT, delivery);
        webSocketServer.setReuseAddr(true);
        webSocketServer.setConnectionLostTimeout(10); // ping POS every 10 s; drop dead connections
        webSocketServer.start();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                delivery.tick();
            } catch (Exception e) {
                System.out.println("⚠️  Retry tick failed: " + e.getMessage());
            }
        }, 1, 1, TimeUnit.SECONDS);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps());

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n🔴 Stopping POS gateway");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        try {
            consumer.subscribe(List.of(KafkaConfig.ORDERS_TOPIC));
            System.out.println("🟢 POS gateway consuming " + KafkaConfig.ORDERS_TOPIC
                    + " as group " + KafkaConfig.GATEWAY_GROUP_ID);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    handle(record, delivery);
                }
                if (!records.isEmpty()) {
                    // Every order in this batch is now in the pending store -> tell Kafka we're done with it.
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            // expected on shutdown
        } finally {
            consumer.close();
            scheduler.shutdownNow();
            webSocketServer.stop(1000);
            statusProducer.close();
        }
    }

    private static void handle(ConsumerRecord<String, String> record, DeliveryManager delivery) {
        Order order;
        try {
            order = MAPPER.readValue(record.value(), Order.class);
        } catch (Exception e) {
            System.out.printf("⚠️  Skipping unreadable message at partition %d offset %d: %s%n",
                    record.partition(), record.offset(), e.getMessage());
            return;
        }

        if (order.posId() == null || order.posId().isBlank()) {
            delivery.reject(order, "order has no pos_id (sent by an older producer?)");
            return;
        }
        delivery.accept(order);
    }

    private static Properties consumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConfig.GATEWAY_GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // We commit ourselves, after the order is stored (see commitSync above)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return props;
    }

    private static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }
}
