package com.example.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * An order event on the "orders" topic. Serialized as JSON with snake_case names
 * so it stays wire-compatible with the Python producer/tracker.
 *
 * {@code pos_id} says which POS terminal the order is for. It is also used as the
 * Kafka message key, so all orders for one terminal land on the same partition (in order).
 * Older messages (e.g. from producer.py) have no pos_id; it is then null.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Order(
        @JsonProperty("order_id") String orderId,
        @JsonProperty("pos_id") String posId,
        @JsonProperty("user") String user,
        @JsonProperty("item") String item,
        @JsonProperty("quantity") int quantity) {
}
