package com.example.oms.domain;

import java.time.Instant;

public record InventoryReserved(String orderId, long totalCents, Instant occurredAt)
        implements OrderEvent {}
