package com.rajasekhar.kafka.producer;

import com.rajasekhar.kafka.config.KafkaTopics;
import com.rajasekhar.kafka.domain.event.FraudAlertEvent;
import com.rajasekhar.kafka.domain.event.OrderCreatedEvent;
import com.rajasekhar.kafka.domain.event.WalletTransactionEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Transactional event publisher.
 *
 * publishOrderWithWalletDebit() demonstrates exactly-once semantics:
 * Both the order event AND wallet debit event are published in a single
 * Kafka transaction. Either both succeed or neither does — no partial state.
 *
 * This is critical for financial systems:
 * Without transactions: order published → crash → wallet debit never published
 * → inventory reserved but customer never charged → revenue loss.
 *
 * Partition key strategy:
 * - Orders keyed by customerId: all events for same customer → same partition
 *   → ordered processing guaranteed for that customer
 * - Fraud alerts keyed by orderId: fraud event always processed after order
 * - Wallet transactions keyed by customerId: balance updates always in order
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Publishes order + wallet debit atomically in one Kafka transaction.
     * If either publish fails, both are rolled back.
     */
    public void publishOrderWithWalletDebit(OrderCreatedEvent orderEvent) {
        kafkaTemplate.executeInTransaction(ops -> {
            // Step 1: Publish order event (fans out to inventory + fraud groups)
            ops.send(KafkaTopics.ORDERS_CREATED, orderEvent.customerId(), orderEvent);
            log.info("Order event published: orderId={}, customerId={}, amount={}",
                orderEvent.orderId(), orderEvent.customerId(), orderEvent.amount());

            // Step 2: Publish wallet debit in same transaction
            WalletTransactionEvent walletEvent = WalletTransactionEvent.debit(
                orderEvent.orderId(),
                orderEvent.customerId(),
                orderEvent.amount(),
                orderEvent.currency()
            );
            ops.send(KafkaTopics.WALLET_TRANSACTIONS, orderEvent.customerId(), walletEvent);
            log.info("Wallet debit event published: transactionId={}, amount={}",
                walletEvent.transactionId(), walletEvent.amount());

            return true;
        });
    }

    /**
     * Publishes a fraud alert when risk score is MEDIUM or HIGH.
     */
    public void publishFraudAlert(FraudAlertEvent alertEvent) {
        CompletableFuture<SendResult<String, Object>> future =
            kafkaTemplate.send(KafkaTopics.FRAUD_ALERTS,
                alertEvent.orderId().toString(), alertEvent);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish FraudAlertEvent: orderId={}, error={}",
                    alertEvent.orderId(), ex.getMessage());
            } else {
                log.warn("FRAUD ALERT published: orderId={}, customerId={}, risk={}, reason={}",
                    alertEvent.orderId(), alertEvent.customerId(),
                    alertEvent.riskLevel(), alertEvent.reason());
            }
        });
    }

    /**
     * Publishes a wallet reversal (compensating transaction on order failure).
     */
    public void publishWalletReversal(WalletTransactionEvent reversalEvent) {
        kafkaTemplate.send(KafkaTopics.WALLET_TRANSACTIONS,
            reversalEvent.customerId(), reversalEvent);
        log.info("Wallet reversal published: transactionId={}, customerId={}",
            reversalEvent.transactionId(), reversalEvent.customerId());
    }
}
