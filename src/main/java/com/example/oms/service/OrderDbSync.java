package com.example.oms.service;

import com.example.oms.domain.*;
import com.example.oms.repository.OrderRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.stream.Collectors;

/**
 * Mirrors Kafka events into H2. Uses a dedicated consumer group so it doesn't
 * interfere with the saga orchestrator or the service listeners.
 */
@Component
public class OrderDbSync {

    private final OrderRepository repo;

    public OrderDbSync(OrderRepository repo) {
        this.repo = repo;
    }

    @Transactional
    @KafkaListener(topics = Topics.ORDER_EVENTS, groupId = "order-db-sync")
    public void onOrderEvent(OrderEvent event) {
        Instant now = Instant.now();
        switch (event) {
            case OrderPlaced p -> {
                String summary = p.items().stream()
                        .map(i -> i.sku() + " x" + i.qty())
                        .collect(Collectors.joining(", "));
                long total = p.items().stream().mapToLong(LineItem::lineTotalCents).sum();

                OrderDetail o = repo.findById(p.orderId()).orElseGet(OrderDetail::new);
                if (o.getOrderId() == null) {
                    o = new OrderDetail(p.orderId(), p.customerId(), OrderStatus.NEW,
                                        total, summary, p.items().size(), p.occurredAt());
                }
                o.setStatus(OrderStatus.NEW);
                o.setOrderEventAt(now);
                o.setUpdatedAt(now);
                repo.save(o);
            }
            case InventoryReserved r -> updateStatus(r.orderId(), OrderStatus.RESERVED, now);
            case PaymentCompleted c  -> updateStatus(c.orderId(), OrderStatus.PAID, now);
            case OrderShipped s      -> updateStatus(s.orderId(), OrderStatus.SHIPPED, now);
            case InventoryFailed f   -> updateStatus(f.orderId(), OrderStatus.CANCELLED, now);
            case PaymentFailed f     -> updateStatus(f.orderId(), OrderStatus.CANCELLED, now);
            case OrderCancelled c    -> updateStatus(c.orderId(), OrderStatus.CANCELLED, now);
        }
    }

    @Transactional
    @KafkaListener(topics = Topics.INVENTORY_COMMANDS, groupId = "order-db-sync")
    public void onInventoryCommand(InventoryCommand cmd) {
        Instant now = Instant.now();
        repo.findById(cmd.orderId()).ifPresent(o -> {
            o.setInventoryCmdAt(now);
            o.setUpdatedAt(now);
            repo.save(o);
        });
    }

    @Transactional
    @KafkaListener(topics = Topics.PAYMENT_COMMANDS, groupId = "order-db-sync")
    public void onPaymentCommand(PaymentCommand cmd) {
        Instant now = Instant.now();
        repo.findById(cmd.orderId()).ifPresent(o -> {
            o.setPaymentCmdAt(now);
            o.setUpdatedAt(now);
            repo.save(o);
        });
    }

    private void updateStatus(String orderId, OrderStatus status, Instant now) {
        repo.findById(orderId).ifPresent(o -> {
            o.setStatus(status);
            o.setOrderEventAt(now);
            o.setUpdatedAt(now);
            repo.save(o);
        });
    }
}
