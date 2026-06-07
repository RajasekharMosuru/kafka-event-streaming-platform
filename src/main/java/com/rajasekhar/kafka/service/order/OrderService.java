package com.rajasekhar.kafka.service.order;

import com.rajasekhar.kafka.domain.event.OrderCreatedEvent;
import com.rajasekhar.kafka.producer.EventPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final EventPublisher eventPublisher;

    /**
     * Places an order and publishes events atomically.
     * The order event fans out to inventory + fraud consumer groups.
     * The wallet debit is published in the same Kafka transaction.
     */
    public OrderCreatedEvent placeOrder(String customerId, String productId,
                                         int quantity, BigDecimal amount, String currency) {
        OrderCreatedEvent event = OrderCreatedEvent.of(
            customerId, productId, quantity, amount, currency);

        log.info("Placing order: orderId={}, customerId={}, amount={}",
            event.orderId(), customerId, amount);

        // Publish order + wallet debit in one atomic Kafka transaction
        eventPublisher.publishOrderWithWalletDebit(event);

        return event;
    }
}
