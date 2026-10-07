package com.example.kafka;

/** Shared settings. Override the broker address with the KAFKA_BOOTSTRAP_SERVERS env var. */
public final class KafkaConfig {

    /**
     * docker-compose.yaml exposes the EXTERNAL listener on localhost:19092 for clients
     * running on the host. Port 9092 is the INTERNAL listener (only reachable inside Docker).
     */
    public static final String BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092");

    public static final String ORDERS_TOPIC = "orders";
    public static final String TRACKER_GROUP_ID = "order-tracker";

    private KafkaConfig() {
    }
}
