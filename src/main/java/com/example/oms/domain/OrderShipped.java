package com.example.oms.domain;

import java.time.Instant;

public record OrderShipped(String orderId, Instant occurredAt) implements OrderEvent {}
