package com.rajasekhar.kafka.dto.request;

import jakarta.validation.constraints.*;
import java.math.BigDecimal;

public record OrderRequest(
    @NotBlank(message = "Customer ID is required")
    String customerId,

    @NotBlank(message = "Product ID is required")
    String productId,

    @Min(value = 1, message = "Quantity must be at least 1")
    int quantity,

    @NotNull(message = "Amount is required")
    @DecimalMin(value = "0.01", message = "Amount must be greater than zero")
    BigDecimal amount,

    @NotBlank(message = "Currency is required")
    @Size(min = 3, max = 3, message = "Currency must be 3-letter ISO code")
    String currency
) {}
