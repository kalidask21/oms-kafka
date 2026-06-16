package com.example.oms.saga;

import com.example.oms.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * Stateless saga orchestrator. It reacts to domain events on order.events and
 * issues the next command (or a compensating command on failure):
 *
 *   OrderPlaced        -> RESERVE inventory
 *   InventoryReserved  -> CHARGE payment
 *   PaymentCompleted   -> ship the order
 *   PaymentFailed      -> RELEASE inventory (compensate) + cancel
 *   InventoryFailed    -> cancel
 */
@Component
public class OrderSagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaOrchestrator.class);
    private final KafkaTemplate<String, Object> kafka;

    public OrderSagaOrchestrator(KafkaTemplate<String, Object> kafka) {
        this.kafka = kafka;
    }

    @KafkaListener(topics = Topics.ORDER_EVENTS, groupId = "order-saga")
    public void on(OrderEvent e) {
        String id = e.orderId();
        switch (e) {
            case OrderPlaced p -> kafka.send(Topics.INVENTORY_COMMANDS, id,
                    new InventoryCommand(id, p.items(), InventoryCommand.Action.RESERVE));

            case InventoryReserved r -> kafka.send(Topics.PAYMENT_COMMANDS, id,
                    new PaymentCommand(id, r.totalCents(), PaymentCommand.Action.CHARGE));

            case PaymentCompleted c -> kafka.send(Topics.ORDER_EVENTS, id,
                    new OrderShipped(id, Instant.now()));

            case PaymentFailed f -> {
                kafka.send(Topics.INVENTORY_COMMANDS, id,
                        new InventoryCommand(id, List.of(), InventoryCommand.Action.RELEASE));
                kafka.send(Topics.ORDER_EVENTS, id,
                        new OrderCancelled(id, "payment failed: " + f.reason(), Instant.now()));
            }

            case InventoryFailed f -> kafka.send(Topics.ORDER_EVENTS, id,
                    new OrderCancelled(id, "inventory failed: " + f.reason(), Instant.now()));

            case OrderShipped s -> log.info("Order {} shipped — saga complete", id);
            case OrderCancelled c -> log.info("Order {} cancelled — saga complete", id);
        }
    }
}
