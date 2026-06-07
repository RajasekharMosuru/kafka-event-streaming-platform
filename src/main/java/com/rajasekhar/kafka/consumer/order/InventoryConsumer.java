package com.rajasekhar.kafka.consumer.order;

import com.rajasekhar.kafka.config.KafkaTopics;
import com.rajasekhar.kafka.domain.event.OrderCreatedEvent;
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
 * Inventory service consumer — Consumer Group 1.
 *
 * Consumer group key concept:
 * All instances of inventory-service-group SHARE the partitions.
 * With 3 partitions and 3 instances: each instance owns 1 partition.
 * With 3 partitions and 1 instance: one instance reads all 3 partitions.
 *
 * When a new instance joins/leaves → Kafka triggers REBALANCE:
 * All consumers in the group pause, partitions are redistributed,
 * then consumption resumes. This is automatic — no manual coordination.
 *
 * Manual acknowledgment pattern:
 * We call ack.acknowledge() ONLY after successfully processing.
 * If processing fails → don't ack → Kafka re-delivers to another consumer.
 * This guarantees at-least-once processing (with exactly-once via idempotency).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(
        topics = KafkaTopics.ORDERS_CREATED,
        groupId = KafkaTopics.GROUP_INVENTORY,
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderForInventory(
            @Payload Object payload,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            Acknowledgment ack) {

        try {
            OrderCreatedEvent event = objectMapper.convertValue(payload, OrderCreatedEvent.class);

            log.info("[INVENTORY] Processing order: orderId={}, customerId={}, " +
                     "product={}, qty={}, partition={}, offset={}",
                event.orderId(), event.customerId(), event.productId(),
                event.quantity(), partition, offset);

            // Simulate inventory reservation
            reserveInventory(event);

            // Commit offset ONLY after successful processing
            ack.acknowledge();
            log.info("[INVENTORY] Order processed and offset committed: orderId={}, offset={}",
                event.orderId(), offset);

        } catch (Exception ex) {
            // Don't ack — let error handler retry, then DLQ
            log.error("[INVENTORY] Failed to process order at partition={}, offset={}: {}",
                partition, offset, ex.getMessage());
            throw new RuntimeException("Inventory processing failed", ex);
        }
    }

    private void reserveInventory(OrderCreatedEvent event) {
        // Simulate inventory check — replace with actual DB call
        log.info("[INVENTORY] Reserving {} units of product {} for order {}",
            event.quantity(), event.productId(), event.orderId());

        // Simulate occasional failure to demonstrate DLQ routing
        if (event.productId().equals("PRODUCT_OUT_OF_STOCK")) {
            throw new IllegalStateException("Product out of stock: " + event.productId());
        }
    }
}
