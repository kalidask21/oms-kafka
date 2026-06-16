package com.example.oms.domain;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Sealed family of domain events that flow through the order.events log.
 *
 * The @JsonTypeInfo tag is embedded in the JSON body (eventType), so the same
 * bytes deserialize back to the correct concrete type in both Spring listeners
 * and the Kafka Streams topology.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "eventType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = OrderPlaced.class,      name = "OrderPlaced"),
        @JsonSubTypes.Type(value = InventoryReserved.class, name = "InventoryReserved"),
        @JsonSubTypes.Type(value = InventoryFailed.class,   name = "InventoryFailed"),
        @JsonSubTypes.Type(value = PaymentCompleted.class,  name = "PaymentCompleted"),
        @JsonSubTypes.Type(value = PaymentFailed.class,     name = "PaymentFailed"),
        @JsonSubTypes.Type(value = OrderShipped.class,      name = "OrderShipped"),
        @JsonSubTypes.Type(value = OrderCancelled.class,    name = "OrderCancelled")
})
public sealed interface OrderEvent
        permits OrderPlaced, InventoryReserved, InventoryFailed,
                PaymentCompleted, PaymentFailed, OrderShipped, OrderCancelled {

    /** Partition key: every event for one order lands on the same partition -> ordered lifecycle. */
    String orderId();
}
