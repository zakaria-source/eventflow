package com.zakaria.eventflow.adapter.out.messaging;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(name = "eventflow.outbox.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxClaimService claimer;
    private final OutboxStatusUpdater updater;
    private final OutboxTraceContext traceContext;
    private final KafkaTemplate<String, String> kafka;
    private final ObservationRegistry observationRegistry;
    private final int batchSize;
    private final int maxAttempts;
    private final Duration claimTtl;
    private final String dltTopic;

    public OutboxRelay(
            OutboxClaimService claimer,
            OutboxStatusUpdater updater,
            OutboxTraceContext traceContext,
            KafkaTemplate<String, String> kafka,
            ObservationRegistry observationRegistry,
            @Value("${eventflow.outbox.batch-size}") int batchSize,
            @Value("${eventflow.outbox.max-attempts}") int maxAttempts,
            @Value("${eventflow.outbox.claim-ttl-ms:30000}") long claimTtlMillis,
            @Value("${eventflow.kafka.topics.order-created-dlt}") String dltTopic
    ) {
        this.claimer = claimer;
        this.updater = updater;
        this.traceContext = traceContext;
        this.kafka = kafka;
        this.observationRegistry = observationRegistry;
        this.batchSize = batchSize;
        this.maxAttempts = maxAttempts;
        this.claimTtl = Duration.ofMillis(claimTtlMillis);
        this.dltTopic = dltTopic;
    }

    @Scheduled(fixedDelayString = "${eventflow.outbox.fixed-delay}")
    public void publishPending() {
        claimer.claimPending(Instant.now(), batchSize, claimTtl).forEach(this::publishObserved);
    }

    private void publishObserved(OutboxClaimService.ClaimedOutboxEvent event) {
        traceContext.runWith(event.traceContext(), () ->
                Observation.createNotStarted("eventflow.outbox.publish", observationRegistry)
                        .lowCardinalityKeyValue("event.type", event.eventType())
                        .lowCardinalityKeyValue("messaging.destination", event.topic())
                        .observe(() -> publishOne(event))
        );
    }

    private void publishOne(OutboxClaimService.ClaimedOutboxEvent event) {
        try {
            kafka.send(event.topic(), event.aggregateId().toString(), event.payload()).get(5, TimeUnit.SECONDS);
            updater.markPublished(event.eventId());
            log.info("Published event {}", event.eventId());
        } catch (Exception failure) {
            int attempts = updater.markFailed(event.eventId(), failure);
            log.warn("Publish failed event={} attempt={}", event.eventId(), attempts);
            if (attempts >= maxAttempts) {
                sendToDlt(event);
            }
        }
    }

    private void sendToDlt(OutboxClaimService.ClaimedOutboxEvent event) {
        try {
            kafka.send(dltTopic, event.aggregateId().toString(), event.payload()).get(5, TimeUnit.SECONDS);
            updater.markPublished(event.eventId());
            log.error("Moved event to DLT event={}", event.eventId());
        } catch (Exception failure) {
            log.error("DLT publish failed event={}", event.eventId(), failure);
        }
    }
}
