# POS gateway: delivering orders beyond Kafka

Kafka only knows how far each consumer group has read (its committed offset). It never learns whether an order actually reached a POS terminal. The `PosGateway` owns that last hop and reports the outcome as events on the `order-status` topic.

## Components

```mermaid
flowchart LR
    PROD["OrderProducer<br/>key = pos_id"]

    subgraph Kafka["Kafka broker"]
        ORDERS[("topic: orders")]
        STATUS[("topic: order-status")]
    end

    subgraph GW["PosGateway"]
        CONS["Kafka consumer<br/>group: pos-gateway<br/>manual commit"]
        DM["DeliveryManager<br/>pending queue per POS<br/>retries, expiry"]
        WS["WebSocket server<br/>:8887/pos/{posId}"]
        CONS -->|"accept(order)"| DM
        DM <--> WS
    end

    POS1["PosTerminal POS-001"]
    POS2["PosTerminal POS-002"]

    TRK["OrderTracker<br/>group: order-tracker"]
    MON["OrderStatusMonitor<br/>group: order-status-monitor"]

    PROD -->|"order JSON"| ORDERS
    ORDERS -->|"poll()"| CONS
    ORDERS -->|"poll()"| TRK
    WS <-->|"ORDER / ACK"| POS1
    WS <-->|"ORDER / ACK"| POS2
    DM -->|"QUEUED / DELIVERED / FAILED"| STATUS
    STATUS -->|"poll()"| MON
```

- `orders` is read by **two independent consumer groups**: `order-tracker` and `pos-gateway`. Each group has its own offsets, so both see every order.
- Orders are keyed by `pos_id`, so all orders for one terminal land on the same partition, in order.
- The gateway **commits the offset once the order is in its pending store**, not when the POS acks. An offline POS therefore never blocks other terminals sharing the partition.
- The gateway publishes every outcome to `order-status`, keyed by `order_id`. Any service (here `OrderStatusMonitor`) can follow delivery from there.

> Demo shortcut: the pending store is in memory. Because the offset is committed when the order is stored, a real gateway must use a durable store (database, Redis...), or a crash would lose orders Kafka already considers handled.

## Message flow

```mermaid
sequenceDiagram
    autonumber
    participant P as OrderProducer
    participant O as Kafka: orders
    participant G as PosGateway
    participant POS as PosTerminal POS-001
    participant S as Kafka: order-status
    participant M as OrderStatusMonitor

    rect rgba(120, 120, 120, 0.08)
    Note over P,M: Happy path (POS online)
    P->>O: send(key=POS-001, order)
    O-->>G: poll() returns order
    G->>G: store as pending
    G->>S: QUEUED
    G->>POS: ORDER (attempt 1)
    G->>O: commitSync() after the poll batch (Kafka is done with it)
    POS->>POS: process, remember order_id
    POS->>G: ACK
    G->>S: DELIVERED
    S-->>M: poll() returns QUEUED, DELIVERED
    end

    rect rgba(120, 120, 120, 0.08)
    Note over P,M: POS offline
    P->>O: send(key=POS-001, order)
    O-->>G: poll() returns order
    G->>G: store as pending (POS not connected)
    G->>S: QUEUED (POS offline)
    G->>O: commitSync()
    Note over G: other terminals keep receiving orders meanwhile
    POS->>G: connect ws://.../pos/POS-001
    G->>POS: flush pending: ORDER (attempt 1)
    POS->>G: ACK
    G->>S: DELIVERED
    end

    rect rgba(120, 120, 120, 0.08)
    Note over P,M: ACK lost (PosTerminal --flaky)
    G->>POS: ORDER (attempt 1)
    POS--xG: ACK lost
    Note over G: no ACK within 5 s, retry
    G->>POS: ORDER (attempt 2)
    POS->>POS: order_id already seen, do not process twice
    POS->>G: ACK
    G->>S: DELIVERED
    end
```

## Life of an order inside the gateway

```mermaid
stateDiagram-v2
    [*] --> Rejected: no pos_id
    [*] --> Pending: read from orders, stored, offset committed

    state Pending {
        [*] --> WaitingForPOS: POS offline
        [*] --> Sent: POS online
        WaitingForPOS --> Sent: POS connects
        Sent --> WaitingForPOS: POS disconnects
        Sent --> Sent: no ACK in 5 s, resend
    }

    Pending --> Delivered: ACK received
    Pending --> Failed: 3 attempts without ACK
    Pending --> Failed: POS offline more than 2 min

    Rejected --> [*]: FAILED event
    Delivered --> [*]: DELIVERED event
    Failed --> [*]: FAILED event
```

Every arrow into a final state publishes the matching event on `order-status`. `QUEUED` is published when the order enters `Pending`.

## Delivery guarantees

- **Kafka to gateway: at-least-once.** If the gateway crashes before `commitSync()`, it re-reads the batch. Orders already pending are ignored (deduplicated by `order_id`).
- **Gateway to POS: at-least-once.** Retries mean the POS can receive the same order twice, so the POS must be **idempotent**: it remembers processed `order_id`s and only re-sends the ACK.
- Together these give *effectively once* processing at the POS, which is the realistic target end-to-end.
