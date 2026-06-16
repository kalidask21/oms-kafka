package com.example.oms.domain;

import java.time.Instant;
import java.util.List;

/**
 * The read-model projection: an order's current state, produced by folding its
 * event stream. This is the table side of the stream-table duality.
 */
public record OrderState(String orderId, String customerId, OrderStatus status,
                         List<LineItem> items, long totalCents, Instant updatedAt) {

    public static OrderState empty() {
        return new OrderState(null, null, OrderStatus.NEW, List.of(), 0L, Instant.EPOCH);
    }

    /** Reducer: apply one event to the running state. Exhaustive over the sealed OrderEvent. */
    public OrderState apply(OrderEvent e) {
        return switch (e) {
            case OrderPlaced p -> new OrderState(
                    p.orderId(), p.customerId(), OrderStatus.NEW, p.items(),
                    p.items().stream().mapToLong(LineItem::lineTotalCents).sum(), p.occurredAt());
            case InventoryReserved r -> withStatus(OrderStatus.RESERVED, r.occurredAt());
            case PaymentCompleted c  -> withStatus(OrderStatus.PAID, c.occurredAt());
            case OrderShipped s      -> withStatus(OrderStatus.SHIPPED, s.occurredAt());
            case InventoryFailed f   -> withStatus(OrderStatus.CANCELLED, f.occurredAt());
            case PaymentFailed f     -> withStatus(OrderStatus.CANCELLED, f.occurredAt());
            case OrderCancelled c    -> withStatus(OrderStatus.CANCELLED, c.occurredAt());
        };
    }

    private OrderState withStatus(OrderStatus s, Instant at) {
        return new OrderState(orderId, customerId, s, items, totalCents, at);
    }
}
