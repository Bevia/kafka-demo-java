package com.example.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * An order event. Serialized as JSON with snake_case "order_id"
 * so it stays wire-compatible with the Python producer/tracker.
 */
public record Order(
        @JsonProperty("order_id") String orderId,
        @JsonProperty("user") String user,
        @JsonProperty("item") String item,
        @JsonProperty("quantity") int quantity) {
}
