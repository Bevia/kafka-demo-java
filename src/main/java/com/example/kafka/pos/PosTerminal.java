package com.example.kafka.pos;

import com.example.kafka.config.KafkaConfig;
import com.example.kafka.model.Order;
import com.example.kafka.model.PosMessage;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Simulated POS terminal: connects to the PosGateway over WebSocket, prints orders and ACKs them.
 *
 * <p>Usage: {@code PosTerminal [posId] [--flaky | --no-ack]}
 * <ul>
 *   <li>default: ACK every order</li>
 *   <li>{@code --flaky}: "lose" the first ACK of each order, so the gateway retries and the
 *       terminal sees a duplicate (shows why the POS must be idempotent)</li>
 *   <li>{@code --no-ack}: never ACK, so the gateway gives up and publishes FAILED</li>
 * </ul>
 * Stop it with Ctrl+C to simulate an offline terminal; it reconnects automatically when restarted
 * (and keeps retrying if the gateway is down).
 */
public class PosTerminal {

    private enum AckMode { ALWAYS, FLAKY, NONE }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String posId = args.length > 0 && !args[0].startsWith("--") ? args[0] : "POS-001";
        List<String> flags = Arrays.asList(args);
        AckMode mode = flags.contains("--no-ack") ? AckMode.NONE
                : flags.contains("--flaky") ? AckMode.FLAKY
                : AckMode.ALWAYS;

        URI uri = URI.create(KafkaConfig.GATEWAY_WS_URL + "/pos/" + posId);

        // Idempotency: order_ids this terminal already processed. (A real POS would persist this.)
        Set<String> processed = ConcurrentHashMap.newKeySet();
        Set<String> ackDroppedOnce = ConcurrentHashMap.newKeySet();

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicReference<WebSocketClient> current = new AtomicReference<>();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n🔴 " + posId + " going offline");
            running.set(false);
            WebSocketClient client = current.get();
            if (client != null) {
                client.close();
            }
        }));

        System.out.printf("🧾 %s starting (ack mode: %s), connecting to %s%n", posId, mode, uri);

        while (running.get()) {
            CountDownLatch closed = new CountDownLatch(1);
            WebSocketClient client = new WebSocketClient(uri) {
                @Override
                public void onOpen(ServerHandshake handshake) {
                    System.out.println("🟢 " + posId + " connected to gateway");
                }

                @Override
                public void onMessage(String text) {
                    handleMessage(this, text, posId, mode, processed, ackDroppedOnce);
                }

                @Override
                public void onClose(int code, String reason, boolean remote) {
                    closed.countDown();
                }

                @Override
                public void onError(Exception ex) {
                    // connection refused etc.; onClose follows
                }
            };
            current.set(client);

            if (client.connectBlocking(5, TimeUnit.SECONDS)) {
                closed.await();
                if (running.get()) {
                    System.out.println("🔌 " + posId + " lost connection to gateway");
                }
            }
            if (running.get()) {
                System.out.println("⏳ " + posId + " retrying connection in 3 s...");
                Thread.sleep(3000);
            }
        }
    }

    private static void handleMessage(WebSocketClient ws, String text, String posId, AckMode mode,
                                      Set<String> processed, Set<String> ackDroppedOnce) {
        PosMessage msg;
        try {
            msg = MAPPER.readValue(text, PosMessage.class);
        } catch (Exception e) {
            System.out.println("⚠️  Bad message from gateway: " + e.getMessage());
            return;
        }
        if (msg.type() != PosMessage.Type.ORDER || msg.order() == null) {
            return;
        }

        Order order = msg.order();
        boolean firstTime = processed.add(order.orderId());
        if (firstTime) {
            System.out.printf("🧾 [%s] New order %s: %d x %s for %s (attempt %d)%n",
                    posId, order.orderId(), order.quantity(), order.item(), order.user(), msg.attempt());
        } else {
            System.out.printf("♻️  [%s] Duplicate of order %s (attempt %d): already processed, not printing again%n",
                    posId, order.orderId(), msg.attempt());
        }

        if (mode == AckMode.NONE) {
            System.out.println("🙊 [" + posId + "] --no-ack: not acknowledging");
            return;
        }
        if (mode == AckMode.FLAKY && ackDroppedOnce.add(order.orderId())) {
            System.out.println("📉 [" + posId + "] --flaky: pretending the ACK got lost");
            return;
        }

        try {
            ws.send(MAPPER.writeValueAsString(PosMessage.ack(order.orderId())));
            System.out.println("👍 [" + posId + "] ACK sent for " + order.orderId());
        } catch (Exception e) {
            System.out.println("⚠️  Could not send ACK: " + e.getMessage());
        }
    }
}
