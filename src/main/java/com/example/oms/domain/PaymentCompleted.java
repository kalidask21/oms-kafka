package com.example.oms.domain;

import java.time.Instant;

public record PaymentCompleted(String orderId, Instant occurredAt) implements OrderEvent {}
