package com.example.oms.domain;

import java.util.List;

/** Command sent to the inventory service. Single record + action avoids polymorphic serde on the command topic. */
public record InventoryCommand(String orderId, List<LineItem> items, Action action) {
    public enum Action { RESERVE, RELEASE }
}
