package com.rajasekhar.kafka.config;

/**
 * Centralised Kafka topic registry.
 *
 * Naming convention: {domain}.{event-type}
 * DLQ convention:    {original-topic}.DLQ
 *
 * Why constants instead of @Value strings?
 * - Compile-time safety — typos in topic names are caught at build time
 * - Single source of truth — change once, applies everywhere
 * - IDE navigation — find all usages of a topic instantly
 */
public final class KafkaTopics {

    private KafkaTopics() {}

    // ── Order domain ──────────────────────────────────────────────────────
    public static final String ORDERS_CREATED         = "orders.created";
    public static final String ORDERS_CREATED_DLQ     = "orders.created.DLQ";

    // ── Fraud domain ──────────────────────────────────────────────────────
    public static final String FRAUD_ALERTS           = "fraud.alerts";
    public static final String FRAUD_ALERTS_DLQ       = "fraud.alerts.DLQ";

    // ── Wallet domain ─────────────────────────────────────────────────────
    public static final String WALLET_TRANSACTIONS    = "wallet.transactions";
    public static final String WALLET_TRANSACTIONS_DLQ = "wallet.transactions.DLQ";

    // ── Consumer group IDs ────────────────────────────────────────────────
    // Each group gets an INDEPENDENT offset pointer on the same topic.
    // inventory-service-group can be at offset 100 while
    // fraud-detection-group is still at offset 80 — they don't interfere.
    public static final String GROUP_INVENTORY        = "inventory-service-group";
    public static final String GROUP_FRAUD            = "fraud-detection-group";
    public static final String GROUP_WALLET           = "wallet-service-group";
    public static final String GROUP_ALERT            = "alert-service-group";
    public static final String GROUP_DLQ_MONITOR      = "dlq-monitor-group";
}
