# KafkaConfig: Settings Reference

Oct 27, 2026 · @Vincent

`KafkaConfig` is the single place for the demo's broker address, topic names, consumer group ids and gateway WebSocket address; three values can be overridden with environment variables.

## Settings

All constants are `public static final` and read once, when the class loads; env overrides must be set before the JVM starts.

| Constant | Default | Env override | Meaning |
| --- | --- | --- | --- |
| `BOOTSTRAP_SERVERS` | `localhost:19092` | `KAFKA_BOOTSTRAP_SERVERS` | Broker address. 19092 is the EXTERNAL listener published by `docker-compose.yaml` for apps on the host; 9092 is INTERNAL, reachable only inside Docker |
| `ORDERS_TOPIC` | `orders` | none | Order events, keyed by `pos_id` |
| `ORDER_STATUS_TOPIC` | `order-status` | none | Delivery status events, keyed by `order_id` |
| `TRACKER_GROUP_ID` | `order-tracker` | none | Consumer group of `OrderTracker` |
| `GATEWAY_GROUP_ID` | `pos-gateway` | none | Consumer group of `PosGateway` |
| `STATUS_MONITOR_GROUP_ID` | `order-status-monitor` | none | Consumer group of `OrderStatusMonitor` |
| `GATEWAY_WS_PORT` | `8887` | `POS_GATEWAY_PORT` | Port the gateway's WebSocket server listens on |
| `GATEWAY_WS_URL` | `ws://localhost:` + `GATEWAY_WS_PORT` | `POS_GATEWAY_URL` | Base URL terminals connect to; `/pos/{posId}` is appended |

`GATEWAY_WS_URL` is built from `GATEWAY_WS_PORT`, so setting only `POS_GATEWAY_PORT` moves both the server and the terminals' default URL.

## Who uses what

Every Kafka client reads `BOOTSTRAP_SERVERS`; `PosTerminal` is the only runnable app that never talks to Kafka.

| Constant | Used by |
| --- | --- |
| `BOOTSTRAP_SERVERS` | `OrderProducer`, `OrderTracker`, `PosGateway` (consumer and status producer), `OrderStatusMonitor` |
| `ORDERS_TOPIC` | `OrderProducer` (writes), `OrderTracker` and `PosGateway` (read) |
| `ORDER_STATUS_TOPIC` | `DeliveryManager` (writes), `OrderStatusMonitor` (reads) |
| `TRACKER_GROUP_ID` | `OrderTracker` |
| `GATEWAY_GROUP_ID` | `PosGateway` |
| `STATUS_MONITOR_GROUP_ID` | `OrderStatusMonitor` |
| `GATEWAY_WS_PORT` | `PosGateway` (starts `PosWebSocketServer` on it) |
| `GATEWAY_WS_URL` | `PosTerminal` |

## Observations and possible improvements

| Area | Current behaviour | Possible improvement |
| --- | --- | --- |
| Client properties | Each of the five Kafka clients builds its own `Properties` (serializers, `auto.offset.reset`), so shared settings are repeated | Add factory methods such as `KafkaConfig.consumerProps(groupId)` and `producerProps()` |
| Producer guarantees | `acks`, idempotence and retries are left at client defaults everywhere | Set them once in a shared producer factory |
| Overridable values | Topics and group ids are fixed; only broker, port and URL come from the environment | Allow env overrides for topics and groups to run parallel test setups |
| Invalid port | A non-numeric `POS_GATEWAY_PORT` throws during class loading, surfacing as `ExceptionInInitializerError` | Validate and fail with a clear message |
| Log text | `PosWebSocketServer` prints `ws://localhost:<port>` regardless of `POS_GATEWAY_URL` or the bind address | Print the actual bind address |
| Remote terminals | `GATEWAY_WS_URL` defaults to `localhost`, so a terminal on another machine must set `POS_GATEWAY_URL` | Document this in `RUNNING.md` |
