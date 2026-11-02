# Java Kafka POS Demo — Running and End-to-End Testing Guide

This guide explains how to run and test the Java Kafka POS demo end to end.

The demo models this flow:

```text
OrderProducer
     │
     │ send(key = POS-001)
     ▼
Kafka: orders
     │
     │ poll()
     ▼
PosGateway
     │
     ├── store pending
     ├── publish QUEUED
     │
     ▼
WebSocket
     │
     ▼
PosTerminal POS-001
     │
     │ ACK(orderId)
     ▼
PosGateway
     │
     └── publish DELIVERED
             │
             ▼
      Kafka: order-status
             │
             ▼
      OrderStatusMonitor
```

The key reliability rule is:

> Kafka delivery is complete when `PosGateway` has safely assumed responsibility for the order.  
> POS delivery is complete only when the terminal acknowledges the order.

---

## 1. Prerequisites

You need:

- Java
- Maven
- Apache Kafka
- Docker, if you want Kafka UI
- IntelliJ IDEA or another Java IDE
- The demo applications:
  - `OrderProducer`
  - `PosGateway`
  - `PosTerminal`
  - `OrderStatusMonitor`

The examples below assume Kafka is reachable at:

```text
localhost:9092
```

---

## 2. Start Kafka locally

On macOS with Homebrew:

```bash
brew services start kafka
```

Verify:

```bash
brew services list
```

List topics:

```bash
kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```

---

## 3. Create the demo topics

The demo uses:

```text
orders
order-status
```

Create `orders`:

```bash
kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic orders \
  --partitions 3 \
  --replication-factor 1
```

Create `order-status`:

```bash
kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic order-status \
  --partitions 3 \
  --replication-factor 1
```

Using 3 partitions is useful because `posId` is used as the Kafka key and you can inspect partition routing.

Verify:

```bash
kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders
```

and:

```bash
kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic order-status
```

---

## 4. Optional: start Kafka UI

Kafka UI is useful for inspecting:

- topics
- partitions
- messages
- consumer groups
- committed offsets

Run:

```bash
docker run --rm \
  -p 8082:8080 \
  -e DYNAMIC_CONFIG_ENABLED=true \
  provectuslabs/kafka-ui
```

Open:

```text
http://localhost:8082
```

When configuring the cluster from Docker on macOS, Kafka may need to be referenced as:

```text
host.docker.internal:9092
```

instead of `localhost:9092`, because `localhost` inside the Docker container refers to the container itself.

---

## 5. Start `PosGateway`

`PosGateway` is the central component in this demo.

It should:

- consume from `orders`
- use consumer group `pos-gateway`
- store accepted orders as pending
- publish order-status events
- expose the WebSocket server
- route orders to the correct connected POS
- retry when an ACK is not received

If it is a Spring Boot application:

```bash
mvn spring-boot:run
```

Or run it directly from IntelliJ.

Expected startup output should conceptually look like:

```text
Kafka consumer started
group.id=pos-gateway

WebSocket server started
ws://localhost:8087/pos/{posId}
```

At this stage:

```text
Kafka ← poll() ← PosGateway
```

No POS needs to be connected yet.

---

## 6. Start `OrderStatusMonitor`

Start the order status consumer.

It should consume:

```text
topic = order-status
group = order-status-monitor
```

Its purpose is to make status transitions visible during testing.

Expected statuses include:

```text
QUEUED
DELIVERED
FAILED
```

For example:

```text
ORDER-001 → QUEUED
ORDER-001 → DELIVERED
```

---

## 7. Start one simulated POS

Start:

```text
PosTerminal POS-001
```

The simulated terminal should connect to:

```text
ws://localhost:8087/pos/POS-001
```

Expected gateway log:

```text
POS connected: POS-001
```

Conceptually, the gateway now has:

```text
POS-001 → active WebSocket session
```

For this demo, use the simplified rule:

```text
CONNECTED = READY
```

That means the terminal connects only when it is ready to accept work.

---

## 8. Test 1 — Happy path

Start `OrderProducer` and send:

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

Expected flow:

```text
OrderProducer
    │
    │ send(key=POS-001)
    ▼
Kafka: orders
    │
    ▼
PosGateway
    │
    ├── accept ORDER-001
    ├── store as pending
    ├── publish QUEUED
    └── send to POS-001
            │
            ▼
       PosTerminal
            │
            ├── receive ORDER-001
            ├── process
            └── ACK ORDER-001
                    │
                    ▼
               PosGateway
                    │
                    ├── mark delivered
                    └── publish DELIVERED
                            │
                            ▼
                    Kafka: order-status
                            │
                            ▼
                  OrderStatusMonitor
```

Expected logs may look like:

```text
OrderProducer:
Sending key=POS-001 order=ORDER-001

PosGateway:
Received ORDER-001
Stored pending ORDER-001
Status QUEUED
POS-001 connected
Sending ORDER-001 to POS-001

PosTerminal:
Received ORDER-001
Processing ORDER-001
Sending ACK ORDER-001

PosGateway:
ACK received ORDER-001
Status DELIVERED

OrderStatusMonitor:
ORDER-001 QUEUED
ORDER-001 DELIVERED
```

This proves the basic end-to-end path.

---

## 9. Test 2 — POS offline and reconnect

Stop:

```text
PosTerminal POS-001
```

Expected gateway state:

```text
POS-001 disconnected
```

Now produce:

```json
{
  "orderId": "ORDER-002",
  "posId": "POS-001",
  "amount": 3500,
  "currency": "EUR"
}
```

Expected behavior:

```text
Kafka
   │
   ▼
PosGateway

ORDER-002 received
POS-001 not connected

store pending:
POS-001 → ORDER-002

publish QUEUED
commit Kafka offset
```

The order must remain pending:

```text
pending:

POS-001
 └── ORDER-002
```

Restart:

```text
PosTerminal POS-001
```

Expected reconnect flow:

```text
POS-001 connects
       │
       ▼
Gateway checks pending
       │
       ▼
find ORDER-002
       │
       ▼
send ORDER-002
       │
       ▼
POS receives it
       │
       ▼
ACK
       │
       ▼
DELIVERED
```

This proves Kafka consumption does not need to stop while the POS is offline.

---

## 10. Test 3 — Two terminals

Run:

```text
POS-001
POS-002
```

Produce:

```text
ORDER-003
key = POS-001
```

Then:

```text
ORDER-004
key = POS-002
```

Expected:

```text
ORDER-003 → only POS-001
ORDER-004 → only POS-002
```

Conceptually:

```text
Kafka

key=POS-001 ─────┐
                 ▼
             PosGateway
                 │
                 └── WebSocket → POS-001


key=POS-002 ─────┐
                 ▼
             PosGateway
                 │
                 └── WebSocket → POS-002
```

If `POS-001` receives `ORDER-004`, routing is incorrect.

---

## 11. Test 4 — ACK lost and retry

Configure `POS-001` so it receives an order but intentionally does not send the first ACK.

```text
Gateway → ORDER-005 attempt=1 → POS
```

The POS processes it:

```text
POS receives ORDER-005
POS processes ORDER-005
```

But the ACK is lost:

```text
POS --X--> Gateway
```

After the retry timeout, for example 5 seconds:

```text
Gateway → ORDER-005 attempt=2 → POS
```

The POS must recognize:

```text
ORDER-005 already seen
```

and must not process it as a new payment.

Instead:

```text
POS → ACK ORDER-005
```

Expected final status:

```text
ORDER-005 → DELIVERED
```

Critical invariant:

```text
same orderId
    +
multiple deliveries
    =
ONE payment execution
```

Never:

```text
multiple deliveries
    =
multiple payments
```

---

## 12. Test 5 — One POS offline while another continues

Start:

```text
POS-001
POS-002
```

Then disconnect `POS-001`.

Produce:

```text
Order A → POS-001
Order B → POS-002
```

Expected:

```text
Order A → QUEUED / pending for POS-001
Order B → delivered immediately to POS-002
```

Reconnect `POS-001`.

Expected:

```text
Order A → delivered to POS-001
```

This proves:

```text
POS-001 offline
```

does not mean:

```text
Kafka consumption blocked
or
POS-002 blocked
```

Instead:

```text
POS-001 → pending
POS-002 → continues normally
POS-003 → continues normally
```

This is why Kafka offsets should not be used as per-terminal delivery state.

---

## 13. Test 6 — Gateway restart and pending-delivery durability

Run:

```text
1. POS-001 offline
2. Produce ORDER-006 for POS-001
3. PosGateway receives it
4. PosGateway stores it as pending
5. Kafka offset is committed
6. Kill PosGateway
7. Restart PosGateway
8. Reconnect POS-001
```

Correct behavior:

```text
ORDER-006 is still known
ORDER-006 is delivered after POS-001 reconnects
```

If instead:

```text
ORDER-006 disappears
```

then the pending queue is only in memory.

For example, this is not enough:

```java
Map<String, List<Order>> pending;
```

If Kafka has already been committed and the process crashes, the message may no longer be replayed automatically.

The production-grade requirement is:

```text
Kafka
   │
   ▼
PosGateway
   │
   ▼
DURABLE pending-delivery store
   │
   ▼
commit Kafka offset
```

The Kafka commit means:

> PosGateway has durably assumed responsibility for the order.

It does not mean:

> The POS received the order.

---

## 14. Kafka commit semantics

Bad mental model:

```text
commit Kafka offset
    =
POS received order
```

Correct mental model:

```text
commit Kafka offset
    =
delivery layer safely owns responsibility
```

Then:

```text
POS ACK
    =
terminal delivery completed
```

These are two independent reliability boundaries.

---

## 15. Batch commit caution

Kafka may return multiple records in one `poll()`:

```text
offset 100 → POS-001
offset 101 → POS-002
offset 102 → POS-003
offset 103 → POS-004
```

Before committing the batch, every record covered by that commit must have been safely handed off.

Good:

```text
100 persisted ✓
101 persisted ✓
102 persisted ✓
103 persisted ✓

THEN commit
```

Dangerous:

```text
100 persisted ✓
commit entire consumer position

101 not persisted
102 not persisted
103 not persisted

CRASH
```

Be explicit about which offsets are acknowledged when using manual commits.

---

## 16. Recommended consoles during testing

Keep these open:

```text
┌──────────────────────┐
│ OrderProducer        │
└──────────────────────┘

┌──────────────────────┐
│ PosGateway           │
└──────────────────────┘

┌──────────────────────┐
│ PosTerminal POS-001  │
└──────────────────────┘

┌──────────────────────┐
│ OrderStatusMonitor   │
└──────────────────────┘
```

Optionally also run:

```text
PosTerminal POS-002
```

Keep Kafka UI open beside them.

This lets you watch:

```text
produce
   ↓
Kafka
   ↓
poll
   ↓
pending
   ↓
WebSocket
   ↓
POS
   ↓
ACK
   ↓
status event
```

---

## 17. End-to-end test matrix

| Scenario | Expected result |
|---|---|
| POS online | `QUEUED → ORDER → ACK → DELIVERED` |
| POS offline | `QUEUED`, stored as pending |
| POS reconnects | Pending order is flushed |
| ACK lost | Same `orderId` is retried |
| Duplicate `ORDER` | POS does not process twice |
| POS-001 offline, POS-002 online | POS-002 remains unaffected |
| Wrong POS | Must never receive the order |
| Gateway restart | Pending order must survive if delivery state is durable |

---

## 18. What each test proves

### Happy path

Proves:

```text
Producer → Kafka → Consumer → WebSocket → POS → ACK → status
```

works end to end.

### POS offline

Proves the gateway can accept responsibility for an order even when the terminal is unavailable.

### Reconnect

Proves pending work can be delivered when the terminal becomes available again.

### Multiple terminals

Proves terminal routing using `posId`.

### ACK loss

Proves retry behavior.

### Duplicate delivery

Proves idempotency by `orderId`.

### One offline terminal

Proves an unavailable terminal does not block unrelated terminal traffic.

### Gateway restart

Proves whether the pending-delivery implementation is really durable.

---

## 19. Final architecture model

```text
OrderProducer
     │
     ▼
Kafka: orders
     │
     │ poll()
     ▼
PosGateway
     │
     ├── safely accept responsibility
     ├── store pending
     ├── publish QUEUED
     └── commit Kafka offset
             │
             ▼
        DeliveryManager
             │
      ┌──────┴──────┐
      │             │
 POS online     POS offline
      │             │
    send          wait
      │             │
     ACK       reconnect
      │             │
      │        flush pending
      │             │
      └──────┬──────┘
             ▼
         DELIVERED
             │
             ▼
     Kafka: order-status
             │
             ▼
     OrderStatusMonitor
```

The most important invariant is:

> Kafka offsets track stream-consumption progress.  
> Pending-delivery state tracks whether a particular POS still needs an order.  
> `orderId` idempotency prevents retries from becoming duplicate payment executions.
