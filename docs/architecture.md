# Architecture

For the POS gateway extension (WebSocket delivery, `order-status` topic), see [pos-gateway.md](pos-gateway.md).

## High-level overview

```mermaid
flowchart LR
    subgraph Host["Host machine"]
        P["OrderProducer<br/>KafkaProducer"]
        T["OrderTracker<br/>KafkaConsumer<br/>group: order-tracker"]
    end

    subgraph Docker["Docker container: kafka (cp-kafka 7.8, KRaft)"]
        EXT["EXTERNAL listener<br/>localhost:19092"]
        INT["INTERNAL listener<br/>kafka:9092"]
        TOPIC[("topic: orders")]
        CLI["CLI tools via docker exec<br/>kafka-topics / kafka-console-consumer"]
    end

    VOL[("volume: kafka_kraft")]

    P -->|"send(): Order as JSON"| EXT
    EXT --> TOPIC
    EXT -->|"poll(): records"| T
    CLI --> INT --> TOPIC
    TOPIC -.->|"log segments"| VOL
```

- **OrderProducer** builds an `Order` (random UUID), serializes it to JSON with Jackson and sends it to `orders`.
- **Kafka** runs as a single KRaft node (broker + controller) in Docker. Host apps use the EXTERNAL listener on `19092`; anything inside Docker (including `docker exec` CLI commands) uses the INTERNAL listener on `9092`.
- **OrderTracker** subscribes to `orders` as consumer group `order-tracker`, calls `poll()` in a loop and prints each order. The `poll()` timeout (1 s) is only the maximum wait when there is no data, not a polling interval.

## Message flow

```mermaid
sequenceDiagram
    autonumber
    participant P as OrderProducer
    participant K as Kafka (topic: orders)
    participant T as OrderTracker

    T->>K: subscribe("orders"), group order-tracker
    Note over T,K: auto.offset.reset = earliest

    P->>P: new Order(uuid, user, item, quantity) to JSON
    P->>K: send(record)
    K-->>P: callback: topic, partition, offset

    loop continuously
        T->>K: poll(timeout 1 s)
        K-->>T: ConsumerRecords (JSON)
        T->>T: JSON to Order, print "Received order"
    end

    Note over T,K: poll() returns as soon as records arrive,<br/>the 1 s is only the max wait when the topic is idle
    Note over T: Ctrl+C: shutdown hook calls wakeup(), then close()
```
