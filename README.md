# kafka-demo-java

Java port of the Kafka crash-course project: `OrderProducer` sends an order event to the `orders` topic, and `OrderTracker` consumes and prints it. A `PosGateway` extension delivers orders to POS terminals over WebSocket (see below).

Requirements: JDK 17+, Maven 3.9+, Docker.

See [docs/architecture.md](docs/architecture.md) for the high-level and message-flow diagrams (Mermaid), and [docs/pos-gateway.md](docs/pos-gateway.md) for the POS gateway flow.

## Project structure

```
src/main/java/com/example/kafka/
├── config/     KafkaConfig          shared settings: broker address, topic and group names, ports
├── model/      Order                event on the "orders" topic
│               OrderStatus          event on the "order-status" topic
│               PosMessage           WebSocket messages between gateway and POS (ORDER / ACK)
├── producer/   OrderProducer        publishes orders, keyed by POS id
├── consumer/   OrderTracker         reads "orders" (group order-tracker)
│               OrderStatusMonitor   reads "order-status" (group order-status-monitor)
├── gateway/    PosGateway           Kafka consumer + WebSocket server: delivers orders to POS terminals
│               DeliveryManager      pending queue per POS, retries, publishes order-status
│               PosWebSocketServer   accepts POS connections on :8887/pos/{posId}
└── pos/        PosTerminal          simulated POS terminal (WebSocket client)
```

`model` and `config` are shared; the other packages only depend on them, never on each other.

## Run

```bash
# 1. Start Kafka (KRaft, single node)
docker compose up -d

# 2. In one terminal, start the tracker (consumer)
mvn -q compile exec:java@tracker

# 3. In another terminal, send an order
mvn -q compile exec:java@producer
```

Clients connect to `localhost:19092` (the EXTERNAL listener in `docker-compose.yaml`). Override with `KAFKA_BOOTSTRAP_SERVERS=host:port`.

In IntelliJ you can also run `OrderTracker` / `OrderProducer` directly from their `main` methods.

## POS gateway demo

Extends the demo with a WebSocket gateway that delivers orders to POS terminals and reports what happened on an `order-status` topic. Read [docs/pos-gateway.md](docs/pos-gateway.md) for the design and diagrams.

| Class                                     | Role                                                                                                                |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `OrderProducer [posId] [count]`           | Sends orders keyed by POS id (default `POS-001`, 1 order)                                                           |
| `PosGateway`                              | Consumes `orders` (group `pos-gateway`), pushes to POS over WebSocket on `:8887`, retries, publishes `order-status` |
| `PosTerminal [posId] [--flaky\|--no-ack]` | Simulated POS: prints orders and ACKs them                                                                          |
| `OrderStatusMonitor`                      | Consumes `order-status` and prints QUEUED / DELIVERED / FAILED                                                      |

Run each in its own terminal (or from IntelliJ; program arguments go in the run configuration):

```bash
mvn -q compile exec:java@status                                 # 1. watch delivery status
mvn -q compile exec:java@gateway                                # 2. start the gateway
mvn -q compile exec:java@pos -Dexec.args="POS-001"              # 3. start a terminal
mvn -q compile exec:java@producer -Dexec.args="POS-001 3"       # 4. send 3 orders to POS-001
```

Things to try:

- **Offline POS:** stop the terminal (Ctrl+C), send orders, restart it. The queued orders are delivered on reconnect, and the monitor shows `QUEUED (POS offline)` followed by `DELIVERED`.
- **Offline POS doesn't block others:** with `POS-001` stopped, start `POS-002` and send to it. Its orders are delivered straight away.
- **Lost ACK:** run the terminal with `--flaky`. The gateway retries after 5 s, the terminal spots the duplicate `order_id`, doesn't print it twice, and re-sends the ACK.
- **POS never ACKs:** run with `--no-ack`. After 3 attempts the gateway gives up and publishes `FAILED`.
- **Old messages:** orders without a `pos_id` (from `producer.py` or the earlier producer) are published as `FAILED`.

Check the gateway's offsets (lag returns to 0 as soon as orders are stored, even while a POS is offline):

```bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group pos-gateway
```

Optionally give `orders` more partitions so several POS ids are spread out (existing keys may move partition):

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --alter --topic orders --partitions 3
```

## Useful Kafka commands

```bash
# List topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe the topic and its partitions
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic orders

# View all events in the topic
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning
```

(Commands run inside the container use the INTERNAL listener on 9092.)

## Python → Java mapping

| Python (confluent-kafka)                          | Java (kafka-clients)                                  |
| ------------------------------------------------- | ----------------------------------------------------- |
| `Producer(config)`                                | `KafkaProducer<String, String>`                       |
| `producer.produce(..., callback=delivery_report)` | `producer.send(record, (metadata, exception) -> ...)` |
| `producer.flush()`                                | `producer.flush()` / `close()`                        |
| `Consumer(config)` + `subscribe`                  | `KafkaConsumer<String, String>` + `subscribe`         |
| `consumer.poll(1.0)`                              | `consumer.poll(Duration.ofSeconds(1))`                |
| `json.dumps` / `json.loads`                       | Jackson `ObjectMapper` + `Order` record               |
| `KeyboardInterrupt`                               | shutdown hook + `consumer.wakeup()`                   |

## generate docs

example:
/anthropic-skills:docs @src/main/java/com/example/kafka/consumer/OrderTracker.java
