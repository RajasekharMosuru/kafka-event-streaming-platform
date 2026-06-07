package com.rajasekhar.kafka.consumer.fraud;

import com.rajasekhar.kafka.config.KafkaTopics;
import com.rajasekhar.kafka.domain.event.FraudAlertEvent;
import com.rajasekhar.kafka.domain.event.OrderCreatedEvent;
import com.rajasekhar.kafka.producer.EventPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * Fraud detection consumer — Consumer Group 2.
 *
 * Reads from the SAME orders.created topic as InventoryConsumer
 * but with a DIFFERENT group ID (fraud-detection-group).
 *
 * This means:
 * - Both groups get every message independently
 * - Inventory at offset 50, Fraud at offset 30 — totally independent
 * - Fraud service being slow/down does NOT affect inventory processing
 *
 * Risk scoring engine (simplified):
 * - Amount > 500,000 INR       → HIGH  → publish fraud.alerts → block order
 * - Amount > 100,000 INR       → MEDIUM → publish fraud.alerts → flag for review
 * - Otherwise                  → LOW   → allow
 *
 * In production: replace with ML model scoring or rules engine (Drools).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FraudDetectionConsumer {

    private static final BigDecimal HIGH_RISK_THRESHOLD   = new BigDecimal("500000");
    private static final BigDecimal MEDIUM_RISK_THRESHOLD = new BigDecimal("100000");

    private final EventPublisher eventPublisher;
    private final ObjectMapper objectMapper;

    @KafkaListener(
        topics = KafkaTopics.ORDERS_CREATED,
        groupId = KafkaTopics.GROUP_FRAUD,
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderForFraudDetection(
            @Payload Object payload,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        try {
            OrderCreatedEvent event = objectMapper.convertValue(payload, OrderCreatedEvent.class);

            log.info("[FRAUD] Scoring order: orderId={}, customerId={}, amount={}, " +
                     "partition={}, offset={}",
                event.orderId(), event.customerId(), event.amount(), partition, offset);

            FraudAlertEvent.RiskLevel risk = scoreRisk(event);

            switch (risk) {
                case HIGH -> {
                    log.warn("[FRAUD] HIGH RISK detected: orderId={}, amount={}",
                        event.orderId(), event.amount());
                    FraudAlertEvent alert = FraudAlertEvent.high(
                        event.orderId(), event.customerId(), event.amount(),
                        "Transaction amount exceeds HIGH risk threshold of " + HIGH_RISK_THRESHOLD
                    );
                    eventPublisher.publishFraudAlert(alert);
                }
                case MEDIUM -> {
                    log.warn("[FRAUD] MEDIUM RISK detected: orderId={}, amount={}",
                        event.orderId(), event.amount());
                    FraudAlertEvent alert = FraudAlertEvent.medium(
                        event.orderId(), event.customerId(), event.amount(),
                        "Transaction amount exceeds MEDIUM risk threshold of " + MEDIUM_RISK_THRESHOLD
                    );
                    eventPublisher.publishFraudAlert(alert);
                }
                case LOW ->
                    log.info("[FRAUD] LOW RISK — order cleared: orderId={}", event.orderId());
            }

            ack.acknowledge();

        } catch (Exception ex) {
            log.error("[FRAUD] Failed to score order at partition={}, offset={}: {}",
                partition, offset, ex.getMessage());
            throw new RuntimeException("Fraud detection failed", ex);
        }
    }

    private FraudAlertEvent.RiskLevel scoreRisk(OrderCreatedEvent event) {
        if (event.amount().compareTo(HIGH_RISK_THRESHOLD) > 0) {
            return FraudAlertEvent.RiskLevel.HIGH;
        } else if (event.amount().compareTo(MEDIUM_RISK_THRESHOLD) > 0) {
            return FraudAlertEvent.RiskLevel.MEDIUM;
        }
        return FraudAlertEvent.RiskLevel.LOW;
    }
}
