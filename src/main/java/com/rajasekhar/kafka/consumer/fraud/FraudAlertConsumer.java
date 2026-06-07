package com.rajasekhar.kafka.consumer.fraud;

import com.rajasekhar.kafka.config.KafkaTopics;
import com.rajasekhar.kafka.domain.event.FraudAlertEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Alert consumer — subscribes to fraud.alerts topic.
 *
 * In production this would:
 * - Send SMS/email to compliance team
 * - Create a ticket in JIRA/ServiceNow
 * - Block the customer account if HIGH risk
 * - Trigger a manual review workflow
 *
 * Separate consumer group (alert-service-group) means:
 * alerts can be reprocessed independently without affecting fraud detection offsets.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FraudAlertConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(
        topics = KafkaTopics.FRAUD_ALERTS,
        groupId = KafkaTopics.GROUP_ALERT,
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeFraudAlert(
            @Payload Object payload,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        try {
            FraudAlertEvent event = objectMapper.convertValue(payload, FraudAlertEvent.class);

            log.warn("🚨 [ALERT] FRAUD ALERT RECEIVED: alertId={}, orderId={}, " +
                     "customerId={}, risk={}, amount={}, reason={}",
                event.alertId(), event.orderId(), event.customerId(),
                event.riskLevel(), event.amount(), event.reason());

            // Simulate notification
            notifyComplianceTeam(event);
            ack.acknowledge();

        } catch (Exception ex) {
            log.error("[ALERT] Failed to process fraud alert: {}", ex.getMessage());
            throw new RuntimeException("Alert processing failed", ex);
        }
    }

    private void notifyComplianceTeam(FraudAlertEvent event) {
        // TODO: integrate with email/SMS/PagerDuty/Slack webhook
        log.info("[ALERT] Compliance team notified for orderId={}", event.orderId());
    }
}
