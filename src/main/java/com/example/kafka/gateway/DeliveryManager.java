package com.example.kafka.gateway;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.model.Order;
import com.example.kafka.model.OrderStatus;
import com.example.kafka.model.PosMessage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.java_websocket.WebSocket;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The heart of the PosGateway: owns the "last hop" from Kafka to the POS terminals.
 *
 * <ul>
 *   <li>Keeps a pending queue per POS. An order is pending until that POS sends an ACK.</li>
 *   <li>Pushes orders over the POS's WebSocket when it is connected; flushes the queue when it (re)connects.</li>
 *   <li>Retries unacked orders, and gives up (FAILED) after {@link #MAX_ATTEMPTS} or if the POS
 *       stays offline longer than {@link #OFFLINE_TTL}.</li>
 *   <li>Publishes every outcome to the "order-status" topic.</li>
 * </ul>
 *
 * <p><b>Demo shortcut:</b> the pending queues live in memory. The gateway commits the Kafka offset
 * once an order is in this store, so in production the store MUST be durable (database, Redis...)
 * or a gateway crash would lose orders that Kafka already considers handled.
 *
 * <p>All public methods are {@code synchronized}: they are called from the Kafka thread,
 * the WebSocket threads and the retry scheduler.
 */
public class DeliveryManager {

    static final Duration RETRY_AFTER = Duration.ofSeconds(5);
    static final int MAX_ATTEMPTS = 3;
    static final Duration OFFLINE_TTL = Duration.ofMinutes(2);

    private static final class Pending {
        final Order order;
        final Instant queuedAt = Instant.now();
        int attempts;
        Instant lastSentAt;

        Pending(Order order) {
            this.order = order;
        }
    }

    private final ObjectMapper mapper = new ObjectMapper();
    private final KafkaProducer<String, String> statusProducer;

    /** posId -> (orderId -> pending), insertion-ordered so orders reach the POS in Kafka order. */
    private final Map<String, Map<String, Pending>> pendingByPos = new HashMap<>();
    /** posId -> live WebSocket connection. */
    private final Map<String, WebSocket> connections = new HashMap<>();

    public DeliveryManager(KafkaProducer<String, String> statusProducer) {
        this.statusProducer = statusProducer;
    }

    /** Called by the Kafka consumer loop for each order read from "orders". */
    public synchronized void accept(Order order) {
        Map<String, Pending> queue = pendingByPos.computeIfAbsent(order.posId(), k -> new LinkedHashMap<>());
        if (queue.containsKey(order.orderId())) {
            // Kafka is at-least-once: after a gateway restart/rebalance we may read the same order again.
            log("♻️  Order %s for %s already pending, ignoring Kafka redelivery", order.orderId(), order.posId());
            return;
        }

        Pending pending = new Pending(order);
        queue.put(order.orderId(), pending);

        WebSocket conn = liveConnection(order.posId());
        publish(order, OrderStatus.Status.QUEUED,
                conn != null ? "POS online, delivering" : "POS offline, waiting for it to connect");
        log("📥 Order %s queued for %s (%s)", order.orderId(), order.posId(), conn != null ? "online" : "offline");

        if (conn != null) {
            send(conn, pending);
        }
    }

    /** Called for orders the gateway can never deliver (e.g. no pos_id). */
    public synchronized void reject(Order order, String reason) {
        publish(order, OrderStatus.Status.FAILED, reason);
        log("🚫 Rejected order %s: %s", order.orderId(), reason);
    }

    public synchronized void posConnected(String posId, WebSocket conn) {
        WebSocket previous = connections.put(posId, conn);
        if (previous != null && previous != conn && previous.isOpen()) {
            previous.close(1008, "replaced by a newer connection for " + posId);
        }

        Map<String, Pending> queue = pendingByPos.getOrDefault(posId, Map.of());
        log("🔌 %s connected, %d pending order(s) to deliver", posId, queue.size());
        for (Pending pending : queue.values()) {
            send(conn, pending);
        }
    }

    public synchronized void posDisconnected(String posId, WebSocket conn) {
        if (connections.get(posId) == conn) {
            connections.remove(posId);
            log("🔌 %s disconnected", posId);
        }
    }

    public synchronized void ack(String posId, String orderId) {
        Map<String, Pending> queue = pendingByPos.get(posId);
        Pending pending = queue == null ? null : queue.remove(orderId);
        if (pending == null) {
            log("🤷 ACK from %s for %s ignored (already delivered or failed)", posId, orderId);
            return;
        }
        publish(pending.order, OrderStatus.Status.DELIVERED,
                "acked by POS after " + pending.attempts + " attempt(s)");
        log("✅ %s acked order %s", posId, orderId);
    }

    /** Runs every second: retries unacked orders and expires hopeless ones. */
    public synchronized void tick() {
        Instant now = Instant.now();
        for (Map.Entry<String, Map<String, Pending>> entry : pendingByPos.entrySet()) {
            String posId = entry.getKey();
            WebSocket conn = liveConnection(posId);
            Iterator<Pending> it = entry.getValue().values().iterator();

            while (it.hasNext()) {
                Pending pending = it.next();

                if (conn == null) {
                    if (pending.queuedAt.plus(OFFLINE_TTL).isBefore(now)) {
                        it.remove();
                        publish(pending.order, OrderStatus.Status.FAILED,
                                "POS offline for more than " + OFFLINE_TTL.toSeconds() + "s");
                        log("❌ Order %s expired: %s stayed offline", pending.order.orderId(), posId);
                    }
                    continue;
                }

                boolean waitingForAck = pending.lastSentAt != null
                        && pending.lastSentAt.plus(RETRY_AFTER).isAfter(now);
                if (waitingForAck) {
                    continue;
                }

                if (pending.attempts >= MAX_ATTEMPTS) {
                    it.remove();
                    publish(pending.order, OrderStatus.Status.FAILED,
                            "no ACK from POS after " + MAX_ATTEMPTS + " attempts");
                    log("❌ Order %s failed: no ACK from %s after %d attempts",
                            pending.order.orderId(), posId, MAX_ATTEMPTS);
                } else {
                    send(conn, pending); // retry (or first send if it was never sent)
                }
            }
        }
    }

    private WebSocket liveConnection(String posId) {
        WebSocket conn = connections.get(posId);
        return conn != null && conn.isOpen() ? conn : null;
    }

    private void send(WebSocket conn, Pending pending) {
        pending.attempts++;
        pending.lastSentAt = Instant.now();
        try {
            conn.send(mapper.writeValueAsString(PosMessage.order(pending.order, pending.attempts)));
            log("📤 Sent order %s to %s (attempt %d/%d)",
                    pending.order.orderId(), pending.order.posId(), pending.attempts, MAX_ATTEMPTS);
        } catch (Exception e) {
            log("⚠️  Could not send order %s to %s: %s", pending.order.orderId(), pending.order.posId(), e.getMessage());
        }
    }

    private void publish(Order order, OrderStatus.Status status, String detail) {
        try {
            String json = mapper.writeValueAsString(OrderStatus.of(order, status, detail));
            // key = orderId -> all status events of one order stay in order on one partition
            statusProducer.send(new ProducerRecord<>(KafkaConfig.ORDER_STATUS_TOPIC, order.orderId(), json));
        } catch (JsonProcessingException e) {
            log("⚠️  Could not serialize status for %s: %s", order.orderId(), e.getMessage());
        }
    }

    private static void log(String format, Object... args) {
        System.out.printf("[gateway] " + format + "%n", args);
    }
}
