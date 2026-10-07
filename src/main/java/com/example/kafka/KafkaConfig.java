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
    public static final String ORDER_STATUS_TOPIC = "order-status";

    public static final String TRACKER_GROUP_ID = "order-tracker";
    public static final String GATEWAY_GROUP_ID = "pos-gateway";
    public static final String STATUS_MONITOR_GROUP_ID = "order-status-monitor";

    /** WebSocket port the PosGateway listens on; POS terminals connect to ws://host:port/pos/{posId}. */
    public static final int GATEWAY_WS_PORT =
            Integer.parseInt(System.getenv().getOrDefault("POS_GATEWAY_PORT", "8887"));

    public static final String GATEWAY_WS_URL =
            System.getenv().getOrDefault("POS_GATEWAY_URL", "ws://localhost:" + GATEWAY_WS_PORT);

    private KafkaConfig() {
    }
}
