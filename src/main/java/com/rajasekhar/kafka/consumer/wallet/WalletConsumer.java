package com.rajasekhar.kafka.consumer.wallet;

import com.rajasekhar.kafka.config.KafkaTopics;
import com.rajasekhar.kafka.domain.event.WalletTransactionEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Wallet/ledger consumer — exactly-once balance updates.
 *
 * The hardest problem in distributed systems: ensuring a debit happens
 * EXACTLY once even when the consumer crashes mid-processing.
 *
 * Our exactly-once strategy (layered):
 * Layer 1 — Kafka: producer idempotence + read_committed isolation
 *           ensures no duplicate messages from the broker side.
 * Layer 2 — Application: processedTransactions set tracks IDs we've seen.
 *           If we see a transactionId again → skip (idempotent consumer).
 * Layer 3 — Manual ack: offset committed only AFTER DB update succeeds.
 *           If DB update fails → no ack → Kafka redelivers → Layer 2 handles.
 *
 * In production: Layer 2 lives in Redis or DB (not in-memory) for
 * durability across restarts. TTL = 24 hours (enough for retry window).
 *
 * Offset management insight:
 * With AckMode.MANUAL_IMMEDIATE and 3 partitions:
 * - Partition 0 consumer can be at offset 100
 * - Partition 1 consumer can be at offset 45
 * - Partition 2 consumer can be at offset 200
 * Each partition's offset is tracked independently by the consumer group.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WalletConsumer {

    private final ObjectMapper objectMapper;

    // In-memory idempotency store (use Redis in production)
    private final ConcurrentHashMap<String, Boolean> processedTransactions =
        new ConcurrentHashMap<>();

    @KafkaListener(
        topics = KafkaTopics.WALLET_TRANSACTIONS,
        groupId = KafkaTopics.GROUP_WALLET,
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeWalletTransaction(
            @Payload Object payload,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        try {
            WalletTransactionEvent event = objectMapper.convertValue(
                payload, WalletTransactionEvent.class);

            log.info("[WALLET] Received transaction: id={}, type={}, customerId={}, " +
                     "amount={}, partition={}, offset={}",
                event.transactionId(), event.type(), event.customerId(),
                event.amount(), partition, offset);

            // Layer 2: Application-level idempotency check
            String txKey = event.transactionId().toString();
            if (processedTransactions.containsKey(txKey)) {
                log.warn("[WALLET] Duplicate transaction detected — skipping: id={}",
                    event.transactionId());
                ack.acknowledge();
                return;
            }

            // Process the transaction
            switch (event.type()) {
                case DEBIT    -> processDebit(event);
                case CREDIT   -> processCredit(event);
                case REVERSAL -> processReversal(event);
            }

            // Mark as processed BEFORE acknowledging
            processedTransactions.put(txKey, true);

            // Commit offset after successful processing
            ack.acknowledge();
            log.info("[WALLET] Transaction processed and offset committed: id={}, offset={}",
                event.transactionId(), offset);

        } catch (Exception ex) {
            log.error("[WALLET] Failed to process transaction at partition={}, offset={}: {}",
                partition, offset, ex.getMessage());
            throw new RuntimeException("Wallet processing failed", ex);
        }
    }

    private void processDebit(WalletTransactionEvent event) {
        log.info("[WALLET] DEBIT: customerId={}, amount={} {}",
            event.customerId(), event.amount(), event.currency());
        // TODO: UPDATE wallet SET balance = balance - ? WHERE customer_id = ?
        // Use DB transaction here for atomicity with processedTransactions store
    }

    private void processCredit(WalletTransactionEvent event) {
        log.info("[WALLET] CREDIT: customerId={}, amount={} {}",
            event.customerId(), event.amount(), event.currency());
    }

    private void processReversal(WalletTransactionEvent event) {
        log.warn("[WALLET] REVERSAL: customerId={}, amount={} — refunding order",
            event.customerId(), event.amount());
    }
}
