Here's how to run it, based on your `README.md`, `pom.xml`, `docker-compose.yaml` and the Java code. Each component runs in its own terminal, all from the project folder.

### 0. One-time prerequisites

You need JDK 17+, Maven 3.9+ and Docker Desktop (running).

### 1. Start Kafka

```bash
cd ~/IdeaProjects/kafka-demo-java
docker compose up -d
docker ps          # you should see a container named "kafka"
```

The first run downloads the `cp-kafka` image. Your data is kept in the `kafka_kraft` volume between restarts.

### 2. Create the topics (recommended)

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic orders --partitions 3 --replication-factor 1
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic order-status --partitions 3 --replication-factor 1
```

Kafka would probably create them automatically on first use, but only with 1 partition. With 3 partitions you can see that each POS id always lands on the same partition. Inside the container you use port **9092**; your apps on your Mac use **19092** (already set in `KafkaConfig`).

### 3. Terminal A: status monitor

this is the OrderStatusMonitor box in the diagram!

```bash
mvn -q compile exec:java@status
```

It waits and prints 🕓 QUEUED, ✅ DELIVERED and ❌ FAILED as they happen.

### 4. Terminal B: gateway

this is the PosGateway box in the diagram!

```bash
mvn -q compile exec:java@gateway
```

You should see that it's listening on `ws://localhost:8887/pos/{posId}` and consuming `orders` as group `pos-gateway`.

### 5. Terminal C: a POS terminal

```bash
mvn -q compile exec:java@pos -Dexec.args="POS-001"
```

Look for `🟢 POS-001 connected to gateway`. The gateway logs the connection too.

### 6. Terminal D: send orders

```bash
mvn -q compile exec:java@producer -Dexec.args="POS-001 3"
```

This sends 3 orders for POS-001 and prints the partition and offset for each.

What should happen:

- **Terminal C (POS):** prints 3 new orders and sends an ACK for each.
- **Terminal B (gateway):** order queued → sent → ACK received.
- **Terminal A (monitor):** QUEUED then DELIVERED for each order.

### 7. Optional: Terminal E, the tracker

```bash
mvn -q compile exec:java@tracker
```

It prints every order too, because it's in its own consumer group.

### 8. Things to try

| Scenario | How | What you'll see |
|---|---|---|
| Offline POS | Ctrl+C the POS, send orders, restart it | QUEUED (POS offline), then DELIVERED when it reconnects |
| Other terminals aren't blocked | Keep POS-001 off, start `POS-002` and send to it | POS-002 gets its orders immediately |
| Lost ACK | Start the POS with `-Dexec.args="POS-001 --flaky"` | Gateway resends after 5 s, the POS spots the duplicate, then DELIVERED |
| POS never answers | `-Dexec.args="POS-001 --no-ack"` | 3 attempts, then FAILED |

Here's the offline-POS test. **You only stop the POS terminal**; everything else stays running.

### Before you start

Have these running, each in its own terminal:

| Terminal | Command | Keep running? |
|---|---|---|
| A: monitor | `mvn -q compile exec:java@status` | ✅ yes |
| B: gateway | `mvn -q compile exec:java@gateway` | ✅ yes, **don't stop it** |
| C: POS | `mvn -q compile exec:java@pos -Dexec.args="POS-001"` | ❌ this is the one you stop |
| D: free | used to send orders | |

Don't stop the gateway: its pending orders are only in memory, so restarting it loses them.

### Step 1: take the POS offline

In **terminal C**, press **Ctrl+C**.

- C shows: `🔴 POS-001 going offline`
- B (gateway) shows: `[gateway] 🔌 POS-001 disconnected`

### Step 2: send orders while it's offline

In **terminal D**:

```bash
mvn -q compile exec:java@producer -Dexec.args="POS-001 2"
```

- B (gateway): `[gateway] 📥 Order … queued for POS-001 (offline)`, twice
- A (monitor): `🕓 QUEUED order …  pos POS-001  (POS offline, waiting for it to connect)`, twice

### Step 3 (optional): show that Kafka is already done with the orders

```bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group pos-gateway
```

**LAG is 0**. The gateway committed its offset as soon as it stored the orders, even though nothing has been delivered. From Kafka's point of view, the gateway now owns them.

### Step 4: bring the POS back (within 2 minutes)

In **terminal C**:

```bash
mvn -q compile exec:java@pos -Dexec.args="POS-001"
```

- B (gateway): `[gateway] 🔌 POS-001 connected, 2 pending order(s) to deliver`, then `📤 Sent order … (attempt 1/3)` and `✅ POS-001 acked order …`
- C (POS): `🧾 [POS-001] New order …` and `👍 [POS-001] ACK sent for …`, twice
- A (monitor): `✅ DELIVERED order …  (acked by POS after 1 attempt(s))`, twice

**That's a pass:** the orders waited in the gateway and were delivered as soon as the terminal reconnected.

### Variation: stay offline too long

Repeat steps 1 and 2, but **wait more than 2 minutes** before restarting the POS.

- B (gateway): `[gateway] ❌ Order … expired: POS-001 stayed offline`
- A (monitor): `❌ FAILED order …  (POS offline for more than 120s)`

If you restart the POS after that, nothing is delivered, because the orders were given up.

### Bonus: other terminals aren't blocked

While POS-001 is still offline (after step 2), start a second terminal and send to it:

```bash
mvn -q compile exec:java@pos -Dexec.args="POS-002"
mvn -q compile exec:java@producer -Dexec.args="POS-002 1"
```

POS-002 gets its order **immediately**, even though POS-001's orders are still waiting.

### 9. Look inside Kafka

```bash
# The gateway's position: lag goes back to 0 once orders are stored, even if a POS is offline
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group pos-gateway

# Raw status events
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic order-status --from-beginning
```

### 10. Stop everything

Press Ctrl+C in each terminal, then:

```bash
docker compose down      # stops Kafka, keeps the data
docker compose down -v   # stops Kafka and deletes all topics and data
```

One heads-up: `docs/RUNNING.md` in your repo describes a different setup. It uses Kafka from Homebrew on `localhost:9092`, mentions Spring Boot, and lists Kafka UI. None of that matches the current project (Docker Compose, port 19092, plain `mvn exec:java`). If you follow that file, it won't work as written. I can rewrite it to match the steps above if you'd like.