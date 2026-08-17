package com.zakaria.eventflow.adapter.out.messaging;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
@Component
public class OutboxStatusUpdater {
    private final OutboxJpaRepository repository;
    public OutboxStatusUpdater(OutboxJpaRepository repository){this.repository=repository;}
    @Transactional(propagation=Propagation.REQUIRES_NEW) public void markPublished(UUID id){OutboxEventJpaEntity e=repository.findById(id).orElseThrow();e.publishedAt=Instant.now();e.lastError=null;}
    @Transactional(propagation=Propagation.REQUIRES_NEW) public int markFailed(UUID id,Exception failure){OutboxEventJpaEntity e=repository.findById(id).orElseThrow();e.attempts++;long delay=Math.min(300,1L<<Math.min(e.attempts,8));e.nextAttemptAt=Instant.now().plus(delay,ChronoUnit.SECONDS);e.lastError=failure.getClass().getSimpleName()+": "+failure.getMessage();return e.attempts;}
}
