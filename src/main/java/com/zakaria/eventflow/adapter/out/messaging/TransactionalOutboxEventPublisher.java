package com.zakaria.eventflow.adapter.out.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zakaria.eventflow.application.port.out.DomainEventPublisher;
import com.zakaria.eventflow.domain.event.DomainEvent;
import com.zakaria.eventflow.domain.event.OrderCreatedEvent;
import org.springframework.stereotype.Component;

@Component
public class TransactionalOutboxEventPublisher implements DomainEventPublisher {

    private final OutboxJpaRepository repository;
    private final ObjectMapper objectMapper;
    private final OutboxTraceContext traceContext;

    public TransactionalOutboxEventPublisher(
            OutboxJpaRepository repository,
            ObjectMapper objectMapper,
            OutboxTraceContext traceContext
    ) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.traceContext = traceContext;
    }

    @Override
    public void publish(DomainEvent event) {
        OutboxEventJpaEntity entity = new OutboxEventJpaEntity();
        entity.eventId = event.eventId();
        entity.aggregateId = event.aggregateId();
        entity.eventType = event.eventType();
        entity.topic = topicFor(event);
        entity.payload = serialize(event);
        entity.traceContext = traceContext.capture();
        entity.occurredAt = event.occurredAt();
        repository.save(entity);
    }

    private String topicFor(DomainEvent event) {
        if (event instanceof OrderCreatedEvent) {
            return OrderCreatedEvent.TYPE;
        }
        throw new IllegalArgumentException("No topic mapping for " + event.eventType());
    }

    private String serialize(DomainEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("Could not serialize event", failure);
        }
    }
}
