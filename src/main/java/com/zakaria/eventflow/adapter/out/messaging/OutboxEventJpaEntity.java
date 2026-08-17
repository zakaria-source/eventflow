package com.zakaria.eventflow.adapter.out.messaging;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;
@Entity @Table(name="outbox_events")
class OutboxEventJpaEntity {
    @Id @Column(name="event_id") UUID eventId;
    @Column(name="aggregate_id",nullable=false) UUID aggregateId;
    @Column(name="event_type",nullable=false) String eventType;
    @Column(nullable=false) String topic;
    @Column(nullable=false,columnDefinition="text") String payload;
    @Column(name="occurred_at",nullable=false) Instant occurredAt;
    @Column(name="published_at") Instant publishedAt;
    @Column(nullable=false) int attempts;
    @Column(name="next_attempt_at") Instant nextAttemptAt;
    @Column(name="last_error",columnDefinition="text") String lastError;
    protected OutboxEventJpaEntity() {}
}
