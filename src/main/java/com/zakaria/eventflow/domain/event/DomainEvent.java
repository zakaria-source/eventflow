package com.zakaria.eventflow.domain.event;

import java.time.Instant;
import java.util.UUID;

public interface DomainEvent {
    UUID eventId();
    UUID aggregateId();
    String eventType();
    Instant occurredAt();
}
