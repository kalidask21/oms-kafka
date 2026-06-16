# Event-Driven Order Management System (Kafka skeleton)

A runnable Spring Boot skeleton showing an Order Management System built on Apache
Kafka: an event log as the source of truth, a saga orchestrator, retry topics + a
DLT, and a Kafka Streams topology that folds the event stream into a queryable
read model (the stream-table duality).

## Architecture at a glance

```
POST /orders
     |
     v
OrderService --(OrderPlaced)--> order.events  <-----------------------------+
                                   |                                         |
            +----------------------+----------------------+                  |
            |                      |                      |                  |
   order-saga group         inventory-service       order-projection         |
   (orchestrator)           / payment-service        (Kafka Streams)         |
            |                      |                      |                  |
   issues commands ->     inventory.commands      folds events into          |
   inventory.commands     payments.commands       KTable<orderId,State> ->   |
   payments.commands             |                 order.state (compacted)   |
                                 +--(outcome events: Reserved / Completed / Failed)
```

Every record is keyed by `orderId`, so each order's lifecycle stays strictly
ordered while different orders spread across partitions for parallelism.

## Topics

| Topic                | Key       | cleanup.policy | Purpose                                  |
|----------------------|-----------|----------------|------------------------------------------|
| `order.events`       | `orderId` | delete         | Lifecycle event log (source of truth)    |
| `order.state`        | `orderId` | compact        | Latest state per order (read model)      |
| `inventory.commands` | `orderId` | delete         | Commands to the inventory service        |
| `payments.commands`  | `orderId` | delete         | Commands to the payment service          |

Retry and dead-letter topics (e.g. `inventory.commands-retry-0`, `...-dlt`) are
created automatically by `@RetryableTopic`.

## Run it

Prerequisites: Java 21, Maven, Docker.

```bash
# 1. Start a single-node Kafka (KRaft mode, no ZooKeeper)
docker compose up -d

# 2. Run the app (creates topics on startup)
mvn spring-boot:run
```

### Try the happy path

```bash
# Place an order
curl -s -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-1","items":[{"sku":"SHOE-42","qty":1,"unitPriceCents":12999}]}'
# -> {"orderId":"...."}

# Query its state (folded by the Streams topology)
curl -s http://localhost:8080/orders/<orderId>
# -> {"status":"SHIPPED", ...}   (NEW -> RESERVED -> PAID -> SHIPPED)
```

### Trigger the failure / compensation paths

```bash
# Out of stock: any SKU ending in -OOS -> InventoryFailed -> CANCELLED
curl -s -X POST http://localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"c","items":[{"sku":"HAT-OOS","qty":1,"unitPriceCents":500}]}'

# Payment declined: order total >= $10,000 -> PaymentFailed -> release + CANCELLED
curl -s -X POST http://localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"c","items":[{"sku":"TV-1","qty":1,"unitPriceCents":1500000}]}'
```

## What maps to which concept

- **Ordering** — keying by `orderId` keeps each order's events on one partition.
- **Durability** — `acks=all` + idempotent producer (see `application.yml`); set
  `replication.factor=3` and `min.insync.replicas=2` in production.
- **Fan-out** — `order-saga`, `inventory-service`, `payment-service`, and the
  Streams projection are separate consumer groups, so each sees every event.
- **Reliability** — `@RetryableTopic` + `@DltHandler` isolate poison messages.
- **CQRS / event sourcing** — `OrderStateTopology` folds events into a KTable;
  the compacted `order.state` topic and the queryable store are the read model.
- **Exactly-once** — the Streams app runs `processing.guarantee=exactly_once_v2`.

## Production hardening (not in this skeleton)

- Replace the direct `kafkaTemplate.send()` in `OrderService` with a
  **transactional outbox** + Debezium CDC to avoid the dual-write problem.
- A **Schema Registry** (Avro/Protobuf) with enforced compatibility instead of
  plain JSON.
- Topic sizing: 30 partitions, RF=3, `min.insync.replicas=2`, rack awareness.
- **Tiered storage** for long retention without large local disks.
- Static membership + cooperative-sticky assignor to tame rebalances.
