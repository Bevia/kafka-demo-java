package com.example.kafka.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The tiny JSON protocol spoken over the WebSocket between PosGateway and a PosTerminal.
 *
 * <pre>
 * gateway -> POS : {"type":"ORDER","order":{...},"attempt":1}
 * POS -> gateway : {"type":"ACK","order_id":"..."}
 * </pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public record PosMessage(
        @JsonProperty("type") Type type,
        @JsonProperty("order") Order order,
        @JsonProperty("order_id") String orderId,
        @JsonProperty("attempt") Integer attempt) {

    public enum Type { ORDER, ACK }

    public static PosMessage order(Order order, int attempt) {
        return new PosMessage(Type.ORDER, order, null, attempt);
    }

    public static PosMessage ack(String orderId) {
        return new PosMessage(Type.ACK, null, orderId, null);
    }
}
