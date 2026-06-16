package com.example.oms.service;

import com.example.oms.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * Consumes inventory commands. Transient failures are routed to auto-created
 * retry topics with exponential backoff; exhausted records land in a DLT.
 * Demo rule: any SKU ending in "-OOS" is treated as out of stock.
 */
@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);
    private final KafkaTemplate<String, Object> kafka;

    public InventoryService(KafkaTemplate<String, Object> kafka) {
        this.kafka = kafka;
    }

    @RetryableTopic(attempts = "4",
            backoff = @Backoff(delay = 1000, multiplier = 2.0),
            dltStrategy = DltStrategy.FAIL_ON_ERROR)
    @KafkaListener(topics = Topics.INVENTORY_COMMANDS, groupId = "inventory-service")
    public void onCommand(InventoryCommand cmd) {
        if (cmd.action() == InventoryCommand.Action.RELEASE) {
            log.info("Releasing inventory for {} (compensation)", cmd.orderId());
            return;
        }
        boolean inStock = cmd.items().stream().noneMatch(i -> i.sku().endsWith("-OOS"));
        long total = cmd.items().stream().mapToLong(LineItem::lineTotalCents).sum();

        if (inStock) {
            kafka.send(Topics.ORDER_EVENTS, cmd.orderId(),
                    new InventoryReserved(cmd.orderId(), total, Instant.now()));
        } else {
            kafka.send(Topics.ORDER_EVENTS, cmd.orderId(),
                    new InventoryFailed(cmd.orderId(), "out of stock", Instant.now()));
        }
    }

    @DltHandler
    public void dlt(InventoryCommand cmd) {
        log.error("Inventory command exhausted retries, sent to DLT: {}", cmd);
    }
}
