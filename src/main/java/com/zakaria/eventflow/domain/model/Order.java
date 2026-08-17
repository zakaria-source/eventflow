package com.zakaria.eventflow.domain.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Currency;
import java.util.Objects;
import java.util.UUID;

public record Order(UUID id, String customerId, BigDecimal amount, Currency currency, OrderStatus status, Instant createdAt) {
    public Order {
        Objects.requireNonNull(id); Objects.requireNonNull(customerId); Objects.requireNonNull(amount);
        Objects.requireNonNull(currency); Objects.requireNonNull(status); Objects.requireNonNull(createdAt);
        if (customerId.isBlank()) throw new IllegalArgumentException("customerId must not be blank");
        if (amount.signum() <= 0) throw new IllegalArgumentException("amount must be positive");
    }
    public static Order accept(String customerId, BigDecimal amount, Currency currency) {
        return new Order(UUID.randomUUID(), customerId, amount, currency, OrderStatus.ACCEPTED, Instant.now());
    }
}
