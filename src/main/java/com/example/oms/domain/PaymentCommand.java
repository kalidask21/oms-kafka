package com.example.oms.domain;

public record PaymentCommand(String orderId, long amountCents, Action action) {
    public enum Action { CHARGE, REFUND }
}
