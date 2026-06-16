package com.example.oms.service;

import com.example.oms.domain.LineItem;
import com.example.oms.domain.OrderPlaced;
import com.example.oms.domain.Topics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Entry point: turns a request into an OrderPlaced event.
 *
 * In production you would NOT send() directly inside the order DB transaction
 * (dual-write risk). You would write the event to a transactional OUTBOX table
 * in the same transaction, and let Debezium CDC stream it to Kafka.
 */
@Service
public class OrderService {

    private final KafkaTemplate<String, Object> kafka;

    public OrderService(KafkaTemplate<String, Object> kafka) {
        this.kafka = kafka;
    }

    public String placeOrder(String customerId, List<LineItem> items) {
        String orderId = UUID.randomUUID().toString();
        kafka.send(Topics.ORDER_EVENTS, orderId,                 // key = orderId
                new OrderPlaced(orderId, customerId, items, Instant.now()));
        return orderId;
    }
}
