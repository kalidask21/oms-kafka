package com.example.oms.domain;

import java.time.Instant;

public record InventoryFailed(String orderId, String reason, Instant occurredAt)
        implements OrderEvent {}
