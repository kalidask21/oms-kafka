# OMS Service — Event-Driven Order Management (Kafka)

A Spring Boot application demonstrating an Order Management System built on Apache Kafka:
event log as source of truth, saga orchestrator, retry topics + DLT, Kafka Streams
read model, H2 in-memory order database, and a browser UI for placing and monitoring orders.

## Architecture

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
   issues commands         inventory.commands      folds events into          |
                           payments.commands       KTable<orderId,State>     |
                                   |                 -> order.state topic    |
                                   +--(outcome events: Reserved/Completed/Failed)

                                   +-- order-db-sync group
                                          mirrors every event + command
                                          into H2 customer_orders table
```

## Kafka Topics

| Topic | Key | Cleanup | Purpose |
|---|---|---|---|
| `order.events` | `orderId` | delete | Lifecycle event log (source of truth) |
| `order.state` | `orderId` | compact | Latest state per order (read model) |
| `inventory.commands` | `orderId` | delete | Commands to the inventory service |
| `payments.commands` | `orderId` | delete | Commands to the payment service |

Retry and dead-letter topics (`inventory.commands-retry-0`, `...-dlt`) are created
automatically by `@RetryableTopic`.

## Tech Stack

| Layer | Technology |
|---|---|
| Runtime | Java 21, Spring Boot 3.3.4 |
| Messaging | Apache Kafka 3.8.0 (KRaft, no ZooKeeper) |
| Streams | Kafka Streams (`exactly_once_v2`) |
| Database | H2 in-memory (Spring Data JPA) |
| Build | Maven |
| Container | Docker / Docker Compose |

## Run Locally

**Prerequisites:** Java 21, Maven, Docker Desktop running.

```bash
# 1. Start single-node Kafka (KRaft mode)
docker compose up -d

# 2. Start the app (auto-creates topics on startup)
mvn spring-boot:run
```

App starts on **http://localhost:8080**.

## REST Endpoints

### Orders

| Method | Path | Description |
|---|---|---|
| `POST` | `/orders` | Place a new order |
| `GET` | `/orders/{id}` | Query live order state from Kafka Streams store |
| `GET` | `/orders/export` | Browser UI — orders table with search and order form |

### Kafka Monitor

| Method | Path | Description |
|---|---|---|
| `GET` | `/events/produced` | Last 100 messages sent to Kafka (JSON) |
| `GET` | `/events/consumed` | Last 100 messages received from Kafka (JSON) |

### H2 Console (dev only)

```
http://localhost:8080/h2-console
JDBC URL: jdbc:h2:mem:omsdb
Username: sa   Password: (empty)
```

## Browser UI — `/orders/export`

Open **http://localhost:8080/orders/export** for the OMS Service dashboard:

- **Place Order button** — inline form with customer name, SKU / qty / unit price rows.
  Submits to `POST /orders` via fetch and auto-refreshes the table.
- **Search bar** — filter by order ID or item name (partial, case-insensitive).
  Matching terms are highlighted in the results.
- **Orders table** — all orders from H2, newest first, with colour-coded status badges
  and per-topic pipeline timestamps.

### Status badge colours

| Status | Meaning |
|---|---|
| 🔵 NEW | Order placed, awaiting inventory check |
| 🟡 RESERVED | Inventory reserved, awaiting payment |
| 🟣 PAID | Payment charged |
| 🟢 SHIPPED | Order fulfilled — saga complete |
| 🔴 CANCELLED | Compensation triggered (inventory or payment failure) |

### H2 table columns

| Column | Source |
|---|---|
| `orderId`, `customerId`, `status`, `totalCents` | Core order fields |
| `itemCount`, `itemsSummary` | Line items (e.g. `SHOE-001 x2, HAT-004 x1`) |
| `orderEventAt` | Last time `order.events` updated this row |
| `inventoryCmdAt` | When `inventory.commands` was received |
| `paymentCmdAt` | When `payments.commands` was received |
| `createdAt`, `updatedAt` | Lifecycle timestamps |

## Try the Happy Path

```bash
# Place an order (total $129.99 — well under $10,000 limit)
curl -s -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"CUST-001","items":[{"sku":"SHOE-42","qty":1,"unitPriceCents":12999}]}'
# -> {"orderId":"<uuid>"}

# Poll the Kafka Streams read model
curl -s http://localhost:8080/orders/<orderId>
# -> {"status":"SHIPPED", ...}   (NEW -> RESERVED -> PAID -> SHIPPED)

# See the full pipeline in the browser
open http://localhost:8080/orders/export
```

## Trigger Failure / Compensation Paths

```bash
# Out of stock: SKU ending in -OOS -> InventoryFailed -> CANCELLED
curl -s -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"CUST-002","items":[{"sku":"HAT-OOS","qty":1,"unitPriceCents":500}]}'

# Payment declined: total >= $10,000 -> PaymentFailed -> release inventory -> CANCELLED
curl -s -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"CUST-003","items":[{"sku":"TV-1","qty":1,"unitPriceCents":1500000}]}'
```

## Monitor Kafka Traffic

```bash
# What the app has produced to Kafka
curl -s http://localhost:8080/events/produced | jq .

# What the app has consumed from Kafka
curl -s http://localhost:8080/events/consumed | jq .
```

Each entry: `{ "topic", "key" (orderId), "type" (event class), "payload" (JSON), "timestamp" }`.

## Key Design Concepts

| Concept | Where |
|---|---|
| **Ordered events per key** | `orderId` as Kafka key → all events on one partition |
| **Fan-out** | `order-saga`, `inventory-service`, `payment-service`, `order-db-sync` are independent consumer groups |
| **Saga orchestration** | `OrderSagaOrchestrator` reacts to events, issues commands, runs compensations |
| **Stream-table duality** | `OrderStateTopology` folds `order.events` stream into `KTable<orderId, OrderState>` |
| **Exactly-once** | Kafka Streams runs `processing.guarantee=exactly_once_v2` |
| **Reliability** | `@RetryableTopic` (4 attempts, exponential backoff) + `@DltHandler` |
| **CQRS** | Write path = Kafka events; read path = Streams state store OR H2 table |

## Project Structure

```
src/main/java/com/example/oms/
├── OmsApplication.java
├── config/
│   ├── KafkaConfig.java          # topic declarations + message converter
│   └── KafkaEventCapture.java    # ProducerListener + RecordInterceptor for /events endpoints
├── domain/
│   ├── OrderEvent.java           # sealed interface + @JsonTypeInfo
│   ├── OrderPlaced/Shipped/...   # event records
│   ├── OrderState.java           # Kafka Streams read model (record)
│   ├── OrderDetail.java          # H2 JPA entity (customer_orders table)
│   ├── OrderStatus.java          # NEW / RESERVED / PAID / SHIPPED / CANCELLED
│   ├── LineItem.java
│   ├── InventoryCommand.java
│   └── PaymentCommand.java
├── repository/
│   └── OrderRepository.java      # Spring Data JPA
├── saga/
│   └── OrderSagaOrchestrator.java
├── service/
│   ├── OrderService.java         # produces OrderPlaced
│   ├── InventoryService.java     # consumes inventory.commands
│   ├── PaymentService.java       # consumes payments.commands
│   ├── OrderDbSync.java          # mirrors Kafka events → H2
│   ├── EventLogStore.java        # in-memory ring buffer for /events endpoints
│   └── DataInitializer.java      # clears H2 on startup
├── streams/
│   └── OrderStateTopology.java   # Kafka Streams pipeline
└── web/
    ├── OrderController.java       # POST /orders, GET /orders/{id}, GET /orders/export
    ├── EventMonitorController.java # GET /events/produced, GET /events/consumed
    ├── PlaceOrderRequest.java
    └── EventEntry.java
```

## Production Hardening (not in this skeleton)

- Replace `kafkaTemplate.send()` in `OrderService` with a **transactional outbox** + Debezium CDC to eliminate the dual-write problem.
- Add a **Schema Registry** (Avro/Protobuf) with compatibility enforcement instead of plain JSON.
- Scale Kafka: 30 partitions, RF=3, `min.insync.replicas=2`, rack awareness.
- Replace H2 with **Cloud SQL / PostgreSQL** for persistence across restarts.
- Static membership + cooperative-sticky assignor to reduce rebalance impact.
- **Tiered storage** for long retention without large broker disks.
