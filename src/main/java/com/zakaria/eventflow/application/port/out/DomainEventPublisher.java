package com.zakaria.eventflow.application.port.out;
import com.zakaria.eventflow.domain.event.DomainEvent;
public interface DomainEventPublisher { void publish(DomainEvent event); }
