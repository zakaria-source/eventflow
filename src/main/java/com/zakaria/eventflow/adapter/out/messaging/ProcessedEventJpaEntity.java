package com.zakaria.eventflow.adapter.out.messaging;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;
@Entity @Table(name="processed_events")
class ProcessedEventJpaEntity { @Id @Column(name="event_id") UUID eventId; @Column(name="processed_at",nullable=false) Instant processedAt; protected ProcessedEventJpaEntity(){} }
