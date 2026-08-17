package com.zakaria.eventflow.adapter.out.messaging;

import com.zakaria.eventflow.domain.event.OrderCreatedEvent;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Component
public class OrderProjectionHandler {

    private final ProcessedEventJpaRepository processedEvents;
    private final OrderReadModelJpaRepository readModel;
    private final ObservationRegistry observationRegistry;

    public OrderProjectionHandler(
            ProcessedEventJpaRepository processedEvents,
            OrderReadModelJpaRepository readModel,
            ObservationRegistry observationRegistry
    ) {
        this.processedEvents = processedEvents;
        this.readModel = readModel;
        this.observationRegistry = observationRegistry;
    }

    @Transactional
    public void handle(OrderCreatedEvent event) {
        Observation.createNotStarted("eventflow.order.projection", observationRegistry)
                .lowCardinalityKeyValue("event.type", event.eventType())
                .observe(() -> projectOnce(event));
    }

    private void projectOnce(OrderCreatedEvent event) {
        if (processedEvents.claim(event.eventId(), Instant.now()) == 0) {
            return;
        }

        OrderReadModelJpaEntity projection = new OrderReadModelJpaEntity();
        projection.orderId = event.aggregateId();
        projection.customerId = event.customerId();
        projection.amount = event.amount();
        projection.currency = event.currency();
        projection.sourceEventId = event.eventId();
        projection.projectedAt = Instant.now();
        readModel.save(projection);
    }
}
