# Running One POS with Several Transactions (from the classes)

This guide runs the real classes, with no extra code, to send several orders to one POS terminal and follow each one from Kafka to the POS and back.

Use four terminals or four IntelliJ Run configurations. Start the consumers before the producer so you can watch every order move through the system.

## 0) Start Kafka

```bash
docker compose up -d
```

## 1) Run `OrderStatusMonitor` (terminal 1)

```bash
mvn -q compile exec:java@status
```

It prints the `QUEUED`, `DELIVERED` and `FAILED` events from the `order-status` topic. It is optional, but it is the easiest place to read the outcome of each order.

## 2) Run `PosGateway` (terminal 2)

```bash
mvn -q exec:java@gateway
```

It consumes `orders` and serves WebSocket connections on `ws://localhost:8887/pos/{posId}`.

## 3) Run `PosTerminal` as POS-001 (terminal 3)

```bash
mvn -q exec:java@pos -Dexec.args="POS-001"
```

Wait until you see:

```text
🟢 POS-001 connected to gateway
```

## 4) Run `OrderProducer` with several transactions for that POS (terminal 4)

```bash
mvn -q exec:java@producer -Dexec.args="POS-001 5"
```

The first argument is the `posId` and the second is how many orders to send. All five orders use the key `POS-001`, so they land on the same partition and keep their order.

## What you should see

| Process | Output |
|---|---|
| `OrderProducer` | 5× `✅ Delivered to orders : partition X : at offset N`, all on the same partition |
| `PosGateway` | For each order: `📥 queued` → `📤 Sent (attempt 1/3)` → `✅ POS-001 acked` |
| `PosTerminal` | 5× `🧾 New order ...` and `👍 ACK sent` |
| `OrderStatusMonitor` | 5× `🕓 QUEUED` followed by 5× `✅ DELIVERED` |

## Optional: `OrderTracker`

```bash
mvn -q exec:java@tracker
```

`OrderTracker` reads `orders` in its own consumer group (`order-tracker`). It shows that a second consumer group receives the same messages independently of the gateway.

## Variations with the same POS

### POS offline

1. Stop the POS with Ctrl+C.
2. Send more orders: `mvn -q exec:java@producer -Dexec.args="POS-001 3"`
3. Restart the POS: `mvn -q exec:java@pos -Dexec.args="POS-001"`

The gateway logs `3 pending order(s) to deliver` and flushes them when the POS reconnects. If the POS stays offline for more than 2 minutes, those orders are marked `FAILED`.

### Lost ACK and retry

```bash
mvn -q exec:java@pos -Dexec.args="POS-001 --flaky"
```

The POS "loses" the first ACK of each order. The gateway retries after 5 seconds, and the POS prints `♻️ Duplicate ... already processed`, which shows the idempotency check working.

### POS never ACKs

```bash
mvn -q exec:java@pos -Dexec.args="POS-001 --no-ack"
```

After 3 attempts the status monitor shows `❌ FAILED ... no ACK from POS after 3 attempts`.

## Testing two POS terminals

The gateway keeps a separate pending queue and connection for each `posId`, so two terminals are delivered to independently. This section adds `POS-002` alongside `POS-001`.

### Optional: give `orders` more than one partition

If Kafka auto-creates `orders` when the first client uses it, the topic gets a single partition and both terminals share it. To see the key-based routing, create the topics with 3 partitions before running anything:

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic orders --partitions 3 --replication-factor 1

docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic order-status --partitions 3 --replication-factor 1
```

With 3 partitions, each `posId` always maps to the same partition. `POS-001` and `POS-002` may still hash to the same one, which is fine: ordering is only guaranteed per key.

### Steps

Use five terminals.

1. **`OrderStatusMonitor`** (terminal 1)

   ```bash
   mvn -q compile exec:java@status
   ```

2. **`PosGateway`** (terminal 2)

   ```bash
   mvn -q exec:java@gateway
   ```

3. **`PosTerminal` as POS-001** (terminal 3)

   ```bash
   mvn -q exec:java@pos -Dexec.args="POS-001"
   ```

4. **`PosTerminal` as POS-002** (terminal 4)

   ```bash
   mvn -q exec:java@pos -Dexec.args="POS-002"
   ```

   Wait until both terminals print `🟢 ... connected to gateway`.

5. **`OrderProducer`, once per POS** (terminal 5)

   ```bash
   mvn -q exec:java@producer -Dexec.args="POS-001 3"
   mvn -q exec:java@producer -Dexec.args="POS-002 2"
   ```

### What you should see

| Process | Output |
|---|---|
| `OrderProducer` | 3 deliveries with key `POS-001` and 2 with key `POS-002`; each key always reports the same partition |
| `PosGateway` | `🔌 POS-001 connected` and `🔌 POS-002 connected`, then `📥 queued` → `📤 Sent` → `✅ acked` for each order, with the right `posId` |
| `PosTerminal` POS-001 | 3× `🧾 [POS-001] New order ...` and nothing for POS-002 |
| `PosTerminal` POS-002 | 2× `🧾 [POS-002] New order ...` and nothing for POS-001 |
| `OrderStatusMonitor` | 5× `🕓 QUEUED` and 5× `✅ DELIVERED`, with the `pos` column showing which terminal each order went to |

Each terminal only receives its own orders. The gateway routes by the `posId` in the WebSocket path (`/pos/POS-001`, `/pos/POS-002`).

### Variation: one POS offline, the other keeps working

1. Stop `POS-002` with Ctrl+C.
2. Send orders to both:

   ```bash
   mvn -q exec:java@producer -Dexec.args="POS-001 2"
   mvn -q exec:java@producer -Dexec.args="POS-002 2"
   ```

3. `POS-001` receives and ACKs its 2 orders straight away. The monitor shows them as `DELIVERED`, and `POS-002`'s orders stay `QUEUED` with `POS offline, waiting for it to connect`.
4. Restart `POS-002` within 2 minutes:

   ```bash
   mvn -q exec:java@pos -Dexec.args="POS-002"
   ```

   The gateway logs `2 pending order(s) to deliver` and flushes them.

This works because the gateway commits the Kafka offset once an order is in its pending store, not when the POS ACKs. An offline terminal therefore never blocks another terminal, even when both share a partition.

### Variation: different behavior per POS

Run each terminal in a different ACK mode to see that retries and failures stay per terminal:

```bash
mvn -q exec:java@pos -Dexec.args="POS-001"           # normal
mvn -q exec:java@pos -Dexec.args="POS-002 --no-ack"  # never ACKs
```

After sending orders to both, `POS-001`'s orders become `DELIVERED`. `POS-002`'s orders are retried 3 times and then marked `FAILED`.

## Running from IntelliJ instead of Maven

Right-click each class and choose **Run**, then edit its Run configuration and set **Program arguments**:

| Class | Program arguments |
|---|---|
| `OrderStatusMonitor` | none |
| `PosGateway` | none |
| `PosTerminal` | `POS-001` (optionally add `--flaky` or `--no-ack`) |
| `OrderProducer` | `POS-001 5` |
| `OrderTracker` | none |

For two terminals, duplicate the `PosTerminal` configuration and set its arguments to `POS-002`, and enable **Allow multiple instances** if you reuse one configuration. Do the same for `OrderProducer` with `POS-002 2`.

## Notes

- The gateway keeps pending orders in memory. If you restart it in the middle of a test, any order that has not been ACKed is lost.
- The status monitor and the gateway start from `earliest` on their first run, so they also pick up older messages already in the topics.
