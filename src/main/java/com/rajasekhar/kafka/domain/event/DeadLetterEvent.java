package com.rajasekhar.kafka.domain.event;

import java.time.Instant;

/**
 * Wrapper for any event that failed processing and was routed to the DLQ.
 *
 * DLQ (Dead Letter Queue) strategy:
 *  - Consumer fails to process a message after MAX_RETRIES (3 attempts)
 *  - Instead of blocking the partition (which would stall ALL messages),
 *    we publish to a .DLQ topic and skip the failed message
 *  - DLQ monitor alerts the on-call engineer with full context
 *  - Failed message can be replayed manually after root cause is fixed
 *
 * This is the pattern used in production payment systems at scale.
 * Without DLQ: one bad message blocks the entire partition forever.
 */
public record DeadLetterEvent(
    String originalTopic,
    String originalKey,
    Object originalPayload,
    String errorMessage,
    String errorClass,
    int retryCount,
    Instant failedAt
) {
    public static DeadLetterEvent of(String topic, String key,
                                      Object payload, Exception ex, int retryCount) {
        return new DeadLetterEvent(
            topic, key, payload,
            ex.getMessage(),
            ex.getClass().getSimpleName(),
            retryCount,
            Instant.now()
        );
    }
}
