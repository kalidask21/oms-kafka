package com.example.oms.domain;

import java.time.Instant;
import java.util.List;

public record OrderPlaced(String orderId, String customerId, List<LineItem> items, Instant occurredAt)
        implements OrderEvent {}
