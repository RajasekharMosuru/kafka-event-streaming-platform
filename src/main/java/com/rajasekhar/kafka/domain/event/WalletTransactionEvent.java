package com.rajasekhar.kafka.domain.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Published when a wallet debit/credit needs to happen.
 * Consumed by WalletConsumer which updates the ledger balance.
 *
 * Exactly-once semantics critical here — double-debit = customer complaint.
 * Guaranteed by:
 *  1. Kafka producer idempotence (enable.idempotence=true)
 *  2. Transactional producer (transactional.id set)
 *  3. Consumer isolation.level=read_committed
 */
public record WalletTransactionEvent(
    UUID transactionId,
    UUID orderId,
    String customerId,
    BigDecimal amount,
    String currency,
    TransactionType type,
    Instant occurredAt
) {
    public enum TransactionType { DEBIT, CREDIT, REVERSAL }

    public static WalletTransactionEvent debit(UUID orderId, String customerId,
                                                BigDecimal amount, String currency) {
        return new WalletTransactionEvent(UUID.randomUUID(), orderId,
            customerId, amount, currency, TransactionType.DEBIT, Instant.now());
    }

    public static WalletTransactionEvent reversal(UUID orderId, String customerId,
                                                   BigDecimal amount, String currency) {
        return new WalletTransactionEvent(UUID.randomUUID(), orderId,
            customerId, amount, currency, TransactionType.REVERSAL, Instant.now());
    }
}
