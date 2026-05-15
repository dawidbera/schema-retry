package io.schemaretry.example;

import io.schemaretry.RetryContext;
import io.schemaretry.RetryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample listener that processes Order messages.
 */
public class OrderProcessor {
    private static final Logger log = LoggerFactory.getLogger(OrderProcessor.class);

    @RetryListener(topics = "orders", groupId = "order-processing-group")
    public void processOrder(Order order, RetryContext context) {
        log.info("Processing order: {} for customer: {} (Attempt: {})", 
                order.getOrderId(), order.getCustomerName(), context.getAttempt());

        // Simulate some business logic that might fail
        if (order.getAmount() > 1000) {
            log.warn("High amount order detected, simulating recoverable error for order {}", order.getOrderId());
            context.retry(new RuntimeException("Payment gateway timeout"));
        } else if (order.getAmount() < 0) {
            log.error("Invalid order amount: {}, discarding order {}", order.getAmount(), order.getOrderId());
            context.discard(new IllegalArgumentException("Negative amount"));
        } else {
            log.info("Order {} processed successfully", order.getOrderId());
        }
    }
}
