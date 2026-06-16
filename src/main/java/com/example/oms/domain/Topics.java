package com.example.oms.domain;

/** Central registry of topic names so producers, consumers and the topology agree. */
public final class Topics {
    public static final String ORDER_EVENTS       = "order.events";       // the lifecycle event log (source of truth)
    public static final String ORDER_STATE        = "order.state";        // compacted read model (latest state per order)
    public static final String INVENTORY_COMMANDS = "inventory.commands"; // commands to the inventory service
    public static final String PAYMENT_COMMANDS   = "payments.commands";  // commands to the payment service

    private Topics() {}
}
