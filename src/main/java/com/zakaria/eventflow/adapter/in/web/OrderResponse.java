package com.zakaria.eventflow.adapter.in.web;

import com.zakaria.eventflow.domain.model.Order;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderResponse(UUID id, String customerId, BigDecimal amount, String currency, String status, Instant createdAt) {
    static OrderResponse from(Order order) { return new OrderResponse(order.id(), order.customerId(), order.amount(), order.currency().getCurrencyCode(), order.status().name(), order.createdAt()); }
}
