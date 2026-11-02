package com.example.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * A delivery-status event on the "order-status" topic, published by the PosGateway.
 * This is how the rest of the system learns what happened after Kafka handed the order over:
 * Kafka itself only knows the gateway's committed offset, not whether the POS got the order.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderStatus(
        @JsonProperty("order_id") String orderId,
        @JsonProperty("pos_id") String posId,
        @JsonProperty("status") Status status,
        @JsonProperty("detail") String detail,
        @JsonProperty("timestamp") String timestamp) {

    public enum Status {
        /** Gateway took responsibility for the order (stored as pending). */
        QUEUED,
        /** The POS acknowledged the order. */
        DELIVERED,
        /** Gave up: invalid order, POS never acked, or POS offline too long. */
        FAILED
    }

    public static OrderStatus of(Order order, Status status, String detail) {
        return new OrderStatus(order.orderId(), order.posId(), status, detail, Instant.now().toString());
    }
}
