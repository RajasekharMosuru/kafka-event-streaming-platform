package com.rajasekhar.kafka.dto.response;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderResponse(
    UUID orderId,
    String customerId,
    String productId,
    int quantity,
    BigDecimal amount,
    String currency,
    String status,
    Instant publishedAt
) {}
