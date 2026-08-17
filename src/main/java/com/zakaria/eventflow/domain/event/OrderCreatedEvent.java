package com.zakaria.eventflow.domain.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderCreatedEvent(UUID eventId, UUID aggregateId, String customerId, BigDecimal amount, String currency, Instant occurredAt) implements DomainEvent {
    public static final String TYPE = "orders.created.v1";
    @Override public String eventType() { return TYPE; }
}
