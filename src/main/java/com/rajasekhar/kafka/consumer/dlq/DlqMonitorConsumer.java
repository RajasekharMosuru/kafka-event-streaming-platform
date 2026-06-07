package com.rajasekhar.kafka.consumer.dlq;

import com.rajasekhar.kafka.config.KafkaTopics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Dead Letter Queue monitor.
 *
 * Any message that fails 3 retries in ANY consumer lands here.
 * Spring Kafka's DeadLetterPublishingRecoverer automatically routes
 * failed messages to {original-topic}.DLQ
 *
 * DLQ headers added by Spring (available for inspection):
 *  - kafka_dlt-original-topic      : which topic the message came from
 *  - kafka_dlt-original-partition  : which partition
 *  - kafka_dlt-original-offset     : exact offset of the failed message
 *  - kafka_dlt-exception-message   : the error that caused failure
 *  - kafka_dlt-exception-stacktrace: full stack trace
 *
 * This consumer:
 * 1. Logs the failed message with full context
 * 2. Alerts on-call engineer (PagerDuty/Slack in production)
 * 3. Stores in a dead_letter_messages DB table for manual replay
 *
 * Manual replay workflow:
 * Engineer fixes the root cause → reads from DLQ →
 * republishes to original topic → consumer reprocesses.
 */
@Slf4j
@Component
public class DlqMonitorConsumer {

    @KafkaListener(
        topics = {
            KafkaTopics.ORDERS_CREATED_DLQ,
            KafkaTopics.FRAUD_ALERTS_DLQ,
            KafkaTopics.WALLET_TRANSACTIONS_DLQ
        },
        groupId = KafkaTopics.GROUP_DLQ_MONITOR,
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void monitorDlq(
            @Payload Object payload,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(name = "kafka_dlt-original-topic",     required = false) String originalTopic,
            @Header(name = "kafka_dlt-original-offset",    required = false) String originalOffset,
            @Header(name = "kafka_dlt-exception-message",  required = false) String errorMessage,
            Acknowledgment ack) {

        log.error("💀 [DLQ] DEAD LETTER MESSAGE RECEIVED:" +
                  "\n  DLQ Topic      : {}" +
                  "\n  Original Topic : {}" +
                  "\n  Original Offset: {}" +
                  "\n  Error          : {}" +
                  "\n  Payload        : {}",
            topic, originalTopic, originalOffset, errorMessage, payload);

        // In production:
        // 1. alertOnCallEngineer(topic, errorMessage)
        // 2. deadLetterRepository.save(new DeadLetterRecord(...))
        // 3. incrementDlqMetric(topic)

        ack.acknowledge();
    }
}
