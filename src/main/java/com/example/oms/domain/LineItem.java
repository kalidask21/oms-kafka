package com.example.oms.domain;

public record LineItem(String sku, int qty, long unitPriceCents) {
    public long lineTotalCents() {
        return qty * unitPriceCents;
    }
}
