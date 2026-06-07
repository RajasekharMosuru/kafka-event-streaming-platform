package com.rajasekhar.kafka.domain.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Domain event published when a new order is placed.
 *
 * This single event fans out to THREE independent consumer groups:
 *  1. inventory-service-group  → reserve stock
 *  2. fraud-detection-group    → score transaction risk
 *  3. wallet-service-group     → debit customer wallet
 *
 * Kafka guarantees each group gets its own independent copy and offset pointer.
 * If fraud detection is slow, it doesn't affect inventory processing.
 *
 * Partition key = customerId → all events from same customer land on same
 * partition → ordered processing per customer guaranteed.
 */
public record OrderCreatedEvent(
    UUID orderId,
    String customerId,
    String productId,
    int quantity,
    BigDecimal amount,
    String currency,
    String idempotencyKey,
    Instant occurredAt
) {
    public static OrderCreatedEvent of(
            String customerId, String productId,
            int quantity, BigDecimal amount, String currency) {
        return new OrderCreatedEvent(
            UUID.randomUUID(), customerId, productId,
            quantity, amount, currency,
            UUID.randomUUID().toString(), Instant.now());
    }
}
