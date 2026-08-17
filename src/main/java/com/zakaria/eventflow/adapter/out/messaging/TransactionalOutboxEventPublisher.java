package com.zakaria.eventflow.adapter.out.messaging;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zakaria.eventflow.application.port.out.DomainEventPublisher;
import com.zakaria.eventflow.domain.event.DomainEvent;
import com.zakaria.eventflow.domain.event.OrderCreatedEvent;
import org.springframework.stereotype.Component;
@Component
public class TransactionalOutboxEventPublisher implements DomainEventPublisher {
    private final OutboxJpaRepository repository; private final ObjectMapper objectMapper;
    public TransactionalOutboxEventPublisher(OutboxJpaRepository repository,ObjectMapper objectMapper){this.repository=repository;this.objectMapper=objectMapper;}
    public void publish(DomainEvent event){OutboxEventJpaEntity e=new OutboxEventJpaEntity();e.eventId=event.eventId();e.aggregateId=event.aggregateId();e.eventType=event.eventType();e.topic=topicFor(event);e.payload=serialize(event);e.occurredAt=event.occurredAt();repository.save(e);}
    private String topicFor(DomainEvent event){if(event instanceof OrderCreatedEvent)return OrderCreatedEvent.TYPE;throw new IllegalArgumentException("No topic mapping for "+event.eventType());}
    private String serialize(DomainEvent event){try{return objectMapper.writeValueAsString(event);}catch(JsonProcessingException ex){throw new IllegalStateException("Could not serialize event",ex);}}
}
