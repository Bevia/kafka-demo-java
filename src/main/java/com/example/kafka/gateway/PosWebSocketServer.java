package com.example.kafka.gateway;

import com.example.kafka.model.PosMessage;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;

/**
 * WebSocket endpoint for POS terminals: {@code ws://localhost:8887/pos/{posId}}.
 * Translates WebSocket events into DeliveryManager calls.
 */
public class PosWebSocketServer extends WebSocketServer {

    private static final String PATH_PREFIX = "/pos/";

    private final ObjectMapper mapper = new ObjectMapper();
    private final DeliveryManager delivery;

    public PosWebSocketServer(int port, DeliveryManager delivery) {
        super(new InetSocketAddress(port));
        this.delivery = delivery;
    }

    @Override
    public void onStart() {
        System.out.printf("🟢 POS gateway WebSocket listening on ws://localhost:%d%s{posId}%n",
                getPort(), PATH_PREFIX);
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        String path = handshake.getResourceDescriptor(); // e.g. "/pos/POS-001"
        if (path == null || !path.startsWith(PATH_PREFIX) || path.length() == PATH_PREFIX.length()) {
            conn.close(1008, "expected path " + PATH_PREFIX + "{posId}");
            return;
        }
        String posId = path.substring(PATH_PREFIX.length());
        conn.setAttachment(posId);
        delivery.posConnected(posId, conn);
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        String posId = conn.getAttachment();
        if (posId != null) {
            delivery.posDisconnected(posId, conn);
        }
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        String posId = conn.getAttachment();
        try {
            PosMessage msg = mapper.readValue(message, PosMessage.class);
            if (msg.type() == PosMessage.Type.ACK && posId != null) {
                delivery.ack(posId, msg.orderId());
            }
        } catch (Exception e) {
            System.out.println("⚠️  Bad message from " + posId + ": " + e.getMessage());
        }
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        // conn is null when the error is about the server itself (e.g. port already in use)
        System.out.println("❌ WebSocket error" + (conn == null ? "" : " (" + conn.getAttachment() + ")")
                + ": " + ex.getMessage());
    }
}
