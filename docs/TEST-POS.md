# POS End-to-End Tests

This document records the POS end-to-end tests completed successfully against the Java Kafka demo.

## Scope

The tested flow is:

```text
OrderProducer
     │
     ▼
Kafka: orders
     │
     ▼
PosGateway
     │
     ├── DeliveryManager
     └── WebSocket Server
             │
             ▼
      PosTerminal POS-001
             │
             │ ACK
             ▼
         PosGateway
             │
             ▼
   Kafka: order-status
             │
             ▼
   OrderStatusMonitor
```

Supporting consumer:

```text
Kafka: orders
     │
     ▼
OrderTracker
```

## Test environment

Applications were started in this order:

```text
1. PosGateway
2. OrderStatusMonitor
3. OrderTracker
4. PosTerminal POS-001
5. OrderProducer
```

Kafka was already running in Docker Desktop before starting the Java applications.

---

## Test 1 — POS online / happy path

### Objective

Verify that an order published for a connected POS is consumed by the gateway, delivered over WebSocket, acknowledged by the terminal, and reported as delivered.

### Preconditions

- Kafka is running.
- `PosGateway` is running.
- `OrderStatusMonitor` is running.
- `OrderTracker` is running.
- `PosTerminal POS-001` is connected to the WebSocket server.

Expected terminal connection:

```text
POS-001 → CONNECTED
```

### Test input

Example order:

```json
{
  "orderId": "ORDER-001",
  "posId": "POS-001",
  "amount": 2500,
  "currency": "EUR"
}
```

Kafka key:

```text
POS-001
```

### Expected flow

```text
OrderProducer
     │
     │ publish ORDER-001
     ▼
Kafka: orders
     │
     ├──────────────▶ OrderTracker
     │
     ▼
PosGateway
     │
     ├── receive ORDER-001
     ├── store pending
     ├── publish QUEUED
     └── send ORDER-001 to POS-001
             │
             ▼
      PosTerminal POS-001
             │
             ├── receive ORDER-001
             └── ACK ORDER-001
                     │
                     ▼
                 PosGateway
                     │
                     ├── remove/complete pending delivery
                     └── publish DELIVERED
                             │
                             ▼
                    Kafka: order-status
                             │
                             ▼
                    OrderStatusMonitor
```

### Expected status sequence

```text
ORDER-001 → QUEUED
ORDER-001 → DELIVERED
```

### Result

**PASS**

The order was successfully:

- produced to Kafka,
- consumed by `PosGateway`,
- routed to `POS-001`,
- received by the POS over WebSocket,
- acknowledged by the POS,
- and reported as `DELIVERED`.

### What this proves

This proves the complete happy-path integration:

```text
Producer → Kafka → Consumer → WebSocket → POS → ACK → status
```

---

## Test 2 — POS offline and reconnect

### Objective

Verify that an order is not lost when the target POS is offline, and that the pending order is delivered when the POS reconnects.

### Preconditions

The following applications remain running:

```text
PosGateway
OrderStatusMonitor
OrderTracker
```

Stop:

```text
PosTerminal POS-001
```

Expected gateway state:

```text
POS-001 → DISCONNECTED
```

### Test input

Produce a new order for the disconnected terminal:

```json
{
  "orderId": "ORDER-002",
  "posId": "POS-001",
  "amount": 3500,
  "currency": "EUR"
}
```

Kafka key:

```text
POS-001
```

### Expected behavior while POS is offline

```text
Kafka: orders
     │
     ▼
PosGateway
     │
     ├── receive ORDER-002
     ├── detect POS-001 is offline
     ├── store ORDER-002 as pending
     └── publish QUEUED
```

Pending state:

```text
POS-001
 └── ORDER-002
```

At this point the order must **not** be reported as `DELIVERED`.

Expected status:

```text
ORDER-002 → QUEUED
```

### Reconnect step

Restart:

```text
PosTerminal POS-001
```

Expected flow:

```text
POS-001 connects
       │
       ▼
PosGateway detects connection
       │
       ▼
checks pending work for POS-001
       │
       ▼
finds ORDER-002
       │
       ▼
sends ORDER-002 over WebSocket
       │
       ▼
POS-001 receives ORDER-002
       │
       ▼
POS-001 sends ACK
       │
       ▼
PosGateway publishes DELIVERED
```

### Expected final status sequence

```text
ORDER-002 → QUEUED
ORDER-002 → DELIVERED
```

### Result

**PASS**

The order remained pending while `POS-001` was disconnected and was successfully delivered after the terminal reconnected.

### What this proves

This proves that:

```text
POS offline
     │
     ▼
order remains pending
     │
     │ POS reconnects
     ▼
order is delivered
```

It also proves that Kafka consumption does not need to remain blocked while waiting for the POS to reconnect.

---

## Confirmed behavior

The following behavior has now been verified end to end:

| Scenario | Result | Expected behavior |
|---|---|---|
| POS online | PASS | `QUEUED → ORDER → ACK → DELIVERED` |
| POS offline | PASS | Order remains pending |
| POS reconnects | PASS | Pending order is delivered |
| OrderTracker consumer | PASS | Independently observes records from `orders` |
| OrderStatusMonitor | PASS | Observes `QUEUED` and `DELIVERED` status events |
| WebSocket routing | PASS | Order for `POS-001` reaches `POS-001` |

---

## Reliability model validated so far

The tests confirm the following separation:

```text
Kafka offset / consumption
        │
        ▼
PosGateway assumes responsibility
        │
        ▼
pending delivery state
        │
        ▼
WebSocket delivery
        │
        ▼
POS ACK
```

The important distinction remains:

> A Kafka record being consumed does not mean the POS received the order.

For this demo:

```text
QUEUED
```

means the delivery layer has accepted the order.

```text
DELIVERED
```

means the POS has acknowledged receiving it.

---

## Next tests

The next useful tests are:

1. Two terminals (`POS-001` and `POS-002`) and routing isolation.
2. Lost ACK and retry using the same `orderId`.
3. Duplicate delivery and POS idempotency.
4. `POS-001` offline while `POS-002` continues normally.
5. Kill and restart `PosGateway` to verify whether the pending queue survives process failure.

The gateway restart test is especially important because it determines whether pending-delivery state is durable or only held in memory.
