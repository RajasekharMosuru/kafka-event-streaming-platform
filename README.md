# kafka-event-streaming-platform

A production-grade event streaming platform built with Java 21, Spring Boot 3, and Apache Kafka. Demonstrates advanced Kafka patterns used in high-throughput FinTech systems: exactly-once semantics, consumer groups, DLQ handling, offset management, and transactional publishing.

> **Context:** Built to showcase distributed messaging patterns from real-world experience in payment systems processing 2M+ transactions/day.

---

## Architecture — Full Event Pipeline

```
POST /api/v1/orders
         │
         ▼
   OrderService
         │
         │ executeInTransaction() ── atomic publish
         │
         ├──► [orders.created] ──────────────────────────────────────────┐
         │         │                                                      │
         │         ├── inventory-service-group ──► InventoryConsumer      │
         │         │       Reserve stock                                  │
         │         │                                                      │
         │         └── fraud-detection-group ──► FraudDetectionConsumer  │
         │                 Score risk                                     │
         │                     │                                          │
         │               HIGH/MEDIUM risk                                 │
         │                     │                                          │
         │                     ▼                                          │
         │             [fraud.alerts] ──► FraudAlertConsumer             │
         │                     Notify compliance                          │
         │                                                                │
         └──► [wallet.transactions] ──► WalletConsumer                   │
                   Debit balance                                          │
                                                                          │
                   Any consumer fails 3x                                  │
                         │                                                │
                         ▼                                                │
               [topic.DLQ] ──► DlqMonitorConsumer                        │
                   Alert on-call + store for replay ◄────────────────────┘
```

---

## Advanced Kafka Patterns Demonstrated

### 1. Consumer Groups — Independent Processing

The `orders.created` topic is consumed by **two independent groups simultaneously**:

```
orders.created topic (3 partitions)
       │
       ├── inventory-service-group  → offset pointer: independent
       └── fraud-detection-group   → offset pointer: independent
```

Each group maintains its own offset pointer. If fraud detection is slow or down, it doesn't affect inventory processing. This is the foundation of microservices event-driven architecture.

**Consumer group rebalancing:** When a new consumer instance starts or stops, Kafka automatically redistributes partition ownership within the group. With 3 partitions and 3 instances → 1 partition per instance (maximum parallelism).

### 2. Exactly-Once Semantics — Three Layers

The hardest guarantee in distributed systems, implemented in three layers:

**Layer 1 — Kafka producer idempotence:**
```yaml
enable.idempotence: true
max.in.flight.requests.per.connection: 1
acks: all
```
Kafka broker deduplicates retried messages using sequence numbers. Network blip causing a retry → broker discards the duplicate.

**Layer 2 — Transactional publishing:**
```java
kafkaTemplate.executeInTransaction(ops -> {
    ops.send("orders.created", orderEvent);     // publish order
    ops.send("wallet.transactions", debitEvent); // publish debit
    return true;
    // Both committed atomically OR neither is
});
```
Consumer reads with `isolation.level=read_committed` — only sees committed transactions. Aborted/in-flight messages are invisible.

**Layer 3 — Idempotent consumer:**
```java
if (processedTransactions.containsKey(txKey)) {
    ack.acknowledge(); // Already processed — skip safely
    return;
}
```
Application-level deduplication handles the edge case where Kafka redelivers after consumer crash mid-processing.

### 3. Dead Letter Queue — Never Lose a Message

Without DLQ: one bad message blocks the entire partition forever.

```
Message fails processing
        │
   Retry attempt 1 (after 1s)
        │ fails
   Retry attempt 2 (after 1s)
        │ fails
   Retry attempt 3 (after 1s)
        │ fails
        ▼
DeadLetterPublishingRecoverer
        │
        ▼
[orders.created.DLQ] ──► DlqMonitorConsumer
    - Logs full context (original topic, offset, error)
    - Alerts on-call engineer
    - Stores for manual replay after root cause fixed
    - Original partition continues processing next messages ✓
```

**Test DLQ routing:** Send a request with `productId: "PRODUCT_OUT_OF_STOCK"` — watch the consumer retry 3 times then route to DLQ in the logs.

### 4. Manual Offset Management

```java
// AckMode.MANUAL_IMMEDIATE — we control exactly when offset commits
@KafkaListener(...)
public void consume(@Payload Object payload, Acknowledgment ack) {
    try {
        process(payload);      // Do the work
        ack.acknowledge();     // THEN commit offset
    } catch (Exception ex) {
        throw ex;              // Don't ack → Kafka redelivers
    }
}
```

With `auto-commit=true` (default): offset commits on a timer, regardless of processing success. A crash between commit and processing = silent data loss. Manual ack eliminates this.

### 5. Partition Key Strategy

```java
// Orders keyed by customerId
kafkaTemplate.send("orders.created", customerId, orderEvent);
```

All events for the same customer land on the same partition → processed in order. This guarantees:
- Customer's order events are processed sequentially
- Wallet balance updates happen in correct order (debit before reversal)
- No race condition between order #1 and order #2 for same customer

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Java 21 |
| Framework | Spring Boot 3.2 |
| Messaging | Apache Kafka (Spring Kafka) |
| Build | **Gradle 8.6** |
| Serialisation | Jackson JSON |
| Observability | Micrometer + Prometheus |
| API Docs | SpringDoc OpenAPI |
| Testing | JUnit 5, EmbeddedKafka, Testcontainers |
| CI/CD | GitHub Actions |

---

## Running Locally

**Prerequisites:** Docker, Java 21, Gradle 8.6+

```bash
# 1. Start Kafka + Zookeeper + Kafka UI
docker-compose up -d

# 2. Run the application
./gradlew bootRun

# 3. Swagger UI
open http://localhost:8080/swagger-ui.html

# 4. Kafka UI — watch messages flow through topics in real time
open http://localhost:8090
```

---

## Testing the Pipeline

### Normal order (low risk)
```json
POST /api/v1/orders
{
  "customerId": "CUST-001",
  "productId": "PROD-001",
  "quantity": 2,
  "amount": 5000.00,
  "currency": "INR"
}
```
Watch logs: InventoryConsumer + FraudDetectionConsumer both process independently.

### High-value order (fraud alert triggered)
```json
{
  "customerId": "CUST-002",
  "productId": "PROD-002",
  "quantity": 1,
  "amount": 750000.00,
  "currency": "INR"
}
```
Watch logs: FraudDetectionConsumer scores HIGH → publishes to fraud.alerts → FraudAlertConsumer notifies compliance.

### DLQ routing (consumer failure simulation)
```json
{
  "customerId": "CUST-003",
  "productId": "PRODUCT_OUT_OF_STOCK",
  "quantity": 1,
  "amount": 1000.00,
  "currency": "INR"
}
```
Watch logs: InventoryConsumer fails 3 times → DlqMonitorConsumer receives the dead letter with full context.

---

## Kafka UI — What to Observe

Open **http://localhost:8090** and explore:

| What to look at | Where to find it |
|---|---|
| Messages flowing through topics | Topics → orders.created → Messages |
| Consumer group offsets | Consumer Groups → fraud-detection-group |
| Partition distribution | Topics → orders.created → Partitions |
| DLQ messages | Topics → orders.created.DLQ → Messages |

---

## Key Interview Talking Points

**"Why two consumer groups on the same topic?"**
> Independent scaling, independent failure domains. Fraud detection being slow doesn't block inventory. Each service owns its offset and processes at its own pace.

**"How do you prevent double-charging a customer?"**
> Three layers: Kafka producer idempotence prevents duplicate messages at broker level. Transactional publishing ensures order + debit are atomic. Application-level idempotency (transactionId check) handles consumer redelivery after crash.

**"What happens when a consumer fails repeatedly?"**
> After 3 retries with backoff, DeadLetterPublishingRecoverer routes to the DLQ topic. The partition continues processing. On-call engineer is alerted. Message is stored for manual replay after root cause fix.

**"Why manual offset commit?"**
> Auto-commit uses a timer — it can commit offset before processing completes. Crash between commit and DB write = silent data loss. Manual ack means we commit only after successful processing.

---

## Project Structure

```
src/main/java/com/rajasekhar/kafka/
├── controller/          # REST API
├── service/order/       # OrderService — saga orchestration
├── producer/            # EventPublisher — transactional Kafka producer
├── consumer/
│   ├── order/           # InventoryConsumer (group 1)
│   ├── fraud/           # FraudDetectionConsumer (group 2) + FraudAlertConsumer
│   ├── wallet/          # WalletConsumer — exactly-once balance updates
│   └── dlq/             # DlqMonitorConsumer — dead letter handler
├── domain/event/        # OrderCreatedEvent, FraudAlertEvent, WalletTransactionEvent
├── config/              # KafkaConfig (producer/consumer/error handler) + KafkaTopics
└── dto/                 # Request/Response records
```

---

## Author

**Rajasekhar Mosuru** — Senior Engineering Lead, 13+ years in distributed systems and FinTech

- LinkedIn: [rajasekhar-mosuru](https://www.linkedin.com/in/rajasekhar-mosuru-04783861/)
- GitHub: [RajasekharMosuru](https://github.com/RajasekharMosuru)
- Email: mosururajasekhar@gmail.com

*Related project: [payment-processing-engine](https://github.com/RajasekharMosuru/payment-processing-engine) — Saga pattern, idempotency, circuit breaker.*
