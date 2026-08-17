package com.zakaria.eventflow.adapter.out.messaging;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

@Component
public class OutboxStatusUpdater {

    private final OutboxJpaRepository repository;

    public OutboxStatusUpdater(OutboxJpaRepository repository) {
        this.repository = repository;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markPublished(UUID id) {
        OutboxEventJpaEntity event = repository.findById(id).orElseThrow();
        event.publishedAt = Instant.now();
        event.nextAttemptAt = null;
        event.lastError = null;
        releaseClaim(event);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int markFailed(UUID id, Exception failure) {
        OutboxEventJpaEntity event = repository.findById(id).orElseThrow();
        event.attempts++;
        long delay = Math.min(300, 1L << Math.min(event.attempts, 8));
        event.nextAttemptAt = Instant.now().plus(delay, ChronoUnit.SECONDS);
        event.lastError = failure.getClass().getSimpleName() + ": " + failure.getMessage();
        releaseClaim(event);
        return event.attempts;
    }

    private void releaseClaim(OutboxEventJpaEntity event) {
        event.claimedBy = null;
        event.claimedUntil = null;
    }
}
