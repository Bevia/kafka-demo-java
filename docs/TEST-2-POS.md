# TEST-2-POS.md

# POS Reliability Tests — Two Terminals, ACK Loss, Duplicate Delivery and Gateway Restart

This document contains the next set of end-to-end reliability tests for the Java Kafka POS demo.

These tests build on the already validated happy-path and offline/reconnect scenarios.

The focus now is on:

- routing isolation with two POS terminals,
- ACK loss and retries,
- duplicate delivery and POS idempotency,
- one terminal being offline while another continues,
- killing and restarting `PosGateway`,
- validating whether pending-delivery state survives process failure.

---

## Test environment

Keep the following running unless a specific test says otherwise:

```text
Kafka / Docker Desktop
PosGateway
OrderStatusMonitor
OrderTracker
```

For the terminal tests, use:

```text
PosTerminal POS-001
PosTerminal POS-002
```

For IntelliJ, create two run configurations for `PosTerminal`.

Example:

```text
PosTerminal-POS001
Program argument: POS-001
```

and:

```text
PosTerminal-POS002
Program argument: POS-002
```

If IntelliJ does not allow two instances of the same main class, enable:

```text
Allow multiple instances
```

in the run configuration.

---

# Test 1 — Two POS terminals and routing isolation

## Objective

Verify that an order addressed to one terminal is delivered only to that terminal.

The routing invariant is:

```text
posId = POS-001 → POS-001 only
posId = POS-002 → POS-002 only
```

## Setup

Start:

```text
PosTerminal POS-001
PosTerminal POS-002
```

Expected gateway state:

```text
POS connected: POS-001
POS connected: POS-002
```

Conceptually:

```text
                  PosGateway
                  /        \
                 /          \
        WebSocket            WebSocket
           │                    │
           ▼                    ▼
       POS-001                POS-002
```

---

## Test 1A — Send an order to POS-001

Produce:

```text
ORDER-003
posId = POS-001
```

Expected:

```text
OrderProducer
      │
      ▼
Kafka: orders
      │
      ▼
PosGateway
      │
      ▼
ONLY POS-001
```

Terminal behavior:

```text
POS-001 → receives ORDER-003
POS-002 → receives nothing
```

Expected status:

```text
ORDER-003 → QUEUED
ORDER-003 → DELIVERED
```

---

## Test 1B — Send an order to POS-002

Produce:

```text
ORDER-004
posId = POS-002
```

Expected:

```text
POS-001 → receives nothing
POS-002 → receives ORDER-004
```

Expected status:

```text
ORDER-004 → QUEUED
ORDER-004 → DELIVERED
```

---

## Expected result

```text
ORDER-003 → POS-001 only
ORDER-004 → POS-002 only
```

If `POS-001` receives `ORDER-004`, or `POS-002` receives `ORDER-003`, terminal routing is incorrect.

## What this proves

This test proves that `posId` is being used correctly by the gateway to resolve the active WebSocket session for the target terminal.

---

# Test 2 — ACK loss and retry

## Objective

Verify that `PosGateway` retries an order when the POS receives it but the ACK is lost.

Expected sequence:

```text
Gateway → ORDER-005 attempt=1 → POS
                     │
                     ├── process order
                     │
                     X ACK lost

... retry timeout ...

Gateway → ORDER-005 attempt=2 → POS
```

---

## Configure the POS to drop the first ACK

For the demo, modify `PosTerminal` so it can deliberately ignore the first ACK.

Conceptually:

```java
boolean dropFirstAck = true;
```

Then:

```java
if (dropFirstAck) {
    dropFirstAck = false;

    System.out.println(
        "Simulating lost ACK for " + order.getOrderId()
    );

    // Intentionally do not send the ACK.
    return;
}

sendAck(order.getOrderId());
```

A cleaner option is to enable it through a program argument:

```text
POS-001 --drop-first-ack
```

---

## Execute the test

Start:

```text
PosTerminal POS-001
```

with first-ACK loss enabled.

Produce:

```text
ORDER-005
posId = POS-001
```

Expected `PosGateway` behavior:

```text
Sending ORDER-005 attempt=1
```

Expected POS behavior:

```text
Received ORDER-005
Processing ORDER-005
Simulating lost ACK
```

After the retry timeout, for example 5 seconds:

```text
No ACK for ORDER-005
Retrying ORDER-005
Sending ORDER-005 attempt=2
```

## What this proves

This proves that the gateway does not consider WebSocket send alone to be terminal delivery.

Delivery remains pending until the expected ACK is received.

---

# Test 3 — Duplicate delivery and POS idempotency

## Objective

Verify that retrying the same `orderId` does not execute the order twice.

This test naturally follows the ACK-loss test.

The first delivery was processed by the POS:

```text
ORDER-005
```

but the ACK was intentionally lost.

The gateway therefore sends the same order again:

```text
ORDER-005
```

The POS must identify it as a duplicate.

---

## POS idempotency state

The simulated POS can keep track of already processed order IDs.

For example:

```java
Set<String> processedOrderIds =
        ConcurrentHashMap.newKeySet();
```

Conceptual handling:

```java
boolean firstTime =
        processedOrderIds.add(order.getOrderId());

if (firstTime) {

    System.out.println(
        "Processing " + order.getOrderId()
    );

    process(order);

} else {

    System.out.println(
        "Duplicate order " +
        order.getOrderId() +
        " - NOT processing again"
    );
}

sendAck(order.getOrderId());
```

---

## Expected sequence

```text
Gateway
   │
   │ ORDER-005 attempt=1
   ▼
POS
   │
   ├── process ORDER-005
   │
   X ACK lost


Gateway waits
   │
   │ ORDER-005 attempt=2
   ▼
POS
   │
   ├── detect ORDER-005 already processed
   ├── DO NOT execute again
   │
   └── ACK ORDER-005
           │
           ▼
       Gateway
           │
           ▼
       DELIVERED
```

Expected POS console:

```text
Received ORDER-005

Processing ORDER-005
Simulating ACK loss

...

Received ORDER-005

DUPLICATE ORDER-005
Already processed — skipping execution

Sending ACK ORDER-005
```

Expected final status:

```text
ORDER-005 → DELIVERED
```

---

## Critical invariant

```text
same orderId
    +
multiple deliveries
    =
ONE logical execution
```

Never:

```text
multiple deliveries
    =
multiple payment executions
```

This is the core relationship:

```text
at-least-once delivery
        +
idempotent consumer
        =
one logical execution
```

---

# Test 4 — POS-001 offline while POS-002 continues

## Objective

Verify that one unavailable POS does not block delivery to another terminal.

## Setup

Start:

```text
POS-001
POS-002
```

Then disconnect:

```text
POS-001
```

Expected:

```text
POS-001 → DISCONNECTED
POS-002 → CONNECTED
```

---

## Execute

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

The system must not behave like this:

```text
POS-001 offline
      ↓
Kafka / gateway blocked
      ↓
POS-002 cannot receive its order
```

Instead:

```text
POS-001 → pending
POS-002 → continues normally
POS-003 → continues normally
```

Now reconnect:

```text
POS-001
```

Expected:

```text
pending Order A
      │
      ▼
deliver to POS-001
      │
      ▼
ACK
      │
      ▼
DELIVERED
```

## What this proves

This proves that per-terminal delivery state is independent from Kafka partition-consumption progress.

An offline terminal must not become a head-of-line blocker for unrelated terminal traffic.

---

# Test 5 — Kill and restart `PosGateway`

## Objective

Determine whether pending delivery survives a `PosGateway` process failure.

This is the most important reliability test in this set.

---

## Initial state

Keep running:

```text
Kafka
OrderStatusMonitor
OrderTracker
PosGateway
```

Stop:

```text
POS-001
```

So:

```text
POS-001 → OFFLINE
```

---

## Produce an order while the POS is offline

Produce:

```text
ORDER-006
posId = POS-001
```

Expected gateway behavior:

```text
Received ORDER-006
POS-001 offline

Stored pending:
POS-001 → ORDER-006

Published:
QUEUED
```

At this point, verify that the Kafka record has been consumed and that the relevant offset has been committed.

Conceptually:

```text
Kafka
  │
  ▼
PosGateway
  │
  ├── store ORDER-006 pending
  └── commit Kafka offset
```

---

## Kill the gateway

In IntelliJ:

```text
Stop PosGateway
```

State now:

```text
Kafka
     ORDER-006 already consumed

PosGateway
     DEAD

POS-001
     OFFLINE
```

Restart:

```text
PosGateway
```

Then restart:

```text
PosTerminal POS-001
```

Watch what happens.

---

# Possible outcome A — ORDER-006 is restored

If `ORDER-006` is delivered after restart:

```text
PosGateway restart
      │
      ▼
POS-001 connects
      │
      ▼
ORDER-006 recovered
      │
      ▼
POS receives ORDER-006
      │
      ▼
ACK
      │
      ▼
DELIVERED
```

Then determine where the state was restored from.

Possible sources include:

```text
Kafka replay
database
file
other durable storage
```

This is a successful durability result only if the recovery behavior is deliberate and understood.

---

# Possible outcome B — ORDER-006 disappears

If the POS reconnects and nothing happens, the likely reason is that pending state exists only in memory.

For example:

```java
Map<String, List<Order>> pendingOrders;
```

Before the crash:

```text
Kafka
  │
  ▼
PosGateway
  │
  ├── store pending in RAM
  └── commit Kafka offset
```

Then:

```text
PosGateway crashes
```

Now:

```text
RAM pending state = lost
Kafka offset       = already committed
```

After restart:

```text
Kafka does not automatically resend ORDER-006
+
DeliveryManager no longer knows ORDER-006
=
ORDER-006 is lost from the delivery pipeline
```

This is the failure mode this test is intended to expose.

---

# Required production-grade boundary

The safe sequence should eventually be:

```text
Kafka
  │
  ▼
PosGateway
  │
  ▼
DURABLE pending-delivery state
  │
  ▼
commit Kafka offset
```

Only after durable handoff can the gateway safely tell Kafka:

> I have taken responsibility for this order.

Then later:

```text
POS connects
     │
     ▼
Gateway reads durable pending state
     │
     ▼
ORDER-006
     │
     ▼
POS
     │
     ▼
ACK
```

---

# Kafka commit vs POS delivery

This distinction is central to all of these tests.

```text
Kafka commit
    =
PosGateway has safely assumed responsibility
```

It does not mean:

```text
Kafka commit
    =
POS received the order
```

POS delivery completes separately:

```text
WebSocket ORDER
      │
      ▼
POS
      │
      ▼
ACK
      │
      ▼
DELIVERED
```

---

# Test matrix

| Test | Scenario | Expected result |
|---|---|---|
| 1A | Order for POS-001 | Only POS-001 receives it |
| 1B | Order for POS-002 | Only POS-002 receives it |
| 2 | First ACK lost | Gateway retries same order |
| 3 | Same order delivered twice | POS processes it once and ACKs duplicate |
| 4 | POS-001 offline, POS-002 online | POS-002 continues normally |
| 5 | Gateway restarted with pending order | Test reveals whether pending state is durable |

---

# Expected status examples

For normal successful delivery:

```text
QUEUED
DELIVERED
```

For an ACK-loss retry:

```text
QUEUED
ORDER attempt 1
ACK lost
ORDER attempt 2
ACK
DELIVERED
```

For offline delivery:

```text
QUEUED
POS offline
pending
POS reconnect
ORDER
ACK
DELIVERED
```

---

# What these tests validate

Together, these tests validate four important architectural properties.

## 1. Routing isolation

```text
terminalId determines the target terminal
```

An order intended for one POS must never reach another POS.

## 2. At-least-once delivery

The gateway may retry when delivery outcome is unknown.

```text
send
   ↓
no ACK
   ↓
retry
```

## 3. Idempotent POS execution

Retries must not create duplicate payment execution.

```text
same orderId
      ↓
same logical payment
```

## 4. Durable responsibility transfer

The gateway must not commit Kafka progress and then rely only on volatile memory for pending work.

The desired reliability boundary is:

```text
Kafka
  │
  ▼
durable gateway responsibility
  │
  ▼
commit offset
  │
  ▼
eventual POS delivery
```

---

# Recommended execution order

Run the tests in this order:

```text
1. POS-001 + POS-002 routing
        ↓
2. ACK loss
        ↓
3. Duplicate delivery / idempotency
        ↓
4. POS-001 offline while POS-002 continues
        ↓
5. Kill and restart PosGateway
```

The final gateway-restart test is especially valuable because it moves the demo from network-failure testing into process-failure testing.
