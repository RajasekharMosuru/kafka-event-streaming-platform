package com.rajasekhar.kafka.domain.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Published by FraudDetectionConsumer when a transaction scores HIGH risk.
 * Consumed by AlertConsumer which notifies compliance team.
 *
 * Risk scoring logic (simplified):
 *  - amount > 500,000 INR       → HIGH
 *  - amount > 100,000 INR       → MEDIUM
 *  - otherwise                  → LOW
 */
public record FraudAlertEvent(
    UUID alertId,
    UUID orderId,
    String customerId,
    BigDecimal amount,
    RiskLevel riskLevel,
    String reason,
    Instant occurredAt
) {
    public enum RiskLevel { LOW, MEDIUM, HIGH }

    public static FraudAlertEvent high(UUID orderId, String customerId,
                                       BigDecimal amount, String reason) {
        return new FraudAlertEvent(UUID.randomUUID(), orderId, customerId,
            amount, RiskLevel.HIGH, reason, Instant.now());
    }

    public static FraudAlertEvent medium(UUID orderId, String customerId,
                                          BigDecimal amount, String reason) {
        return new FraudAlertEvent(UUID.randomUUID(), orderId, customerId,
            amount, RiskLevel.MEDIUM, reason, Instant.now());
    }
}
