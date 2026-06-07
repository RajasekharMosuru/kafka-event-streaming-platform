package com.rajasekhar.kafka.controller;

import com.rajasekhar.kafka.domain.event.OrderCreatedEvent;
import com.rajasekhar.kafka.dto.request.OrderRequest;
import com.rajasekhar.kafka.dto.response.OrderResponse;
import com.rajasekhar.kafka.service.order.OrderService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;

@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
@Tag(name = "Orders", description = "Order placement — triggers full event streaming pipeline")
public class OrderController {

    private final OrderService orderService;

    @Operation(
        summary = "Place an order",
        description = """
            Places an order and publishes events to Kafka in a single transaction.
            
            This triggers the full pipeline:
            - orders.created → InventoryConsumer (group: inventory-service-group)
            - orders.created → FraudDetectionConsumer (group: fraud-detection-group)
            - wallet.transactions → WalletConsumer (group: wallet-service-group)
            
            Test fraud detection:
            - amount > 500,000 → HIGH risk alert published to fraud.alerts
            - amount > 100,000 → MEDIUM risk alert
            
            Test DLQ routing:
            - productId = "PRODUCT_OUT_OF_STOCK" → InventoryConsumer fails 3x → routes to orders.created.DLQ
            """
    )
    @PostMapping
    public ResponseEntity<OrderResponse> placeOrder(@Valid @RequestBody OrderRequest request) {
        OrderCreatedEvent event = orderService.placeOrder(
            request.customerId(), request.productId(),
            request.quantity(), request.amount(), request.currency()
        );

        return ResponseEntity.status(HttpStatus.CREATED).body(new OrderResponse(
            event.orderId(), event.customerId(), event.productId(),
            event.quantity(), event.amount(), event.currency(),
            "PUBLISHED", Instant.now()
        ));
    }
}
