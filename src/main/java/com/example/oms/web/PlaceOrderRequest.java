package com.example.oms.web;

import com.example.oms.domain.LineItem;
import java.util.List;

public record PlaceOrderRequest(String customerId, List<LineItem> items) {}
