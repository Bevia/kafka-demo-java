# kafka-demo-java

Java port of the Kafka crash-course project: `OrderProducer` sends an order event to the `orders` topic, and `OrderTracker` consumes and prints it.

Requirements: JDK 17+, Maven 3.9+, Docker.

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

| Python (confluent-kafka) | Java (kafka-clients) |
|---|---|
| `Producer(config)` | `KafkaProducer<String, String>` |
| `producer.produce(..., callback=delivery_report)` | `producer.send(record, (metadata, exception) -> ...)` |
| `producer.flush()` | `producer.flush()` / `close()` |
| `Consumer(config)` + `subscribe` | `KafkaConsumer<String, String>` + `subscribe` |
| `consumer.poll(1.0)` | `consumer.poll(Duration.ofSeconds(1))` |
| `json.dumps` / `json.loads` | Jackson `ObjectMapper` + `Order` record |
| `KeyboardInterrupt` | shutdown hook + `consumer.wakeup()` |
