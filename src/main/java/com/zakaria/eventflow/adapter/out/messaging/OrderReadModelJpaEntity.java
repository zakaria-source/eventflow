package com.zakaria.eventflow.adapter.out.messaging;
import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
@Entity @Table(name="order_read_model")
class OrderReadModelJpaEntity { @Id @Column(name="order_id") UUID orderId; @Column(name="customer_id",nullable=false) String customerId; @Column(nullable=false,precision=19,scale=2) BigDecimal amount; @Column(nullable=false,length=3) String currency; @Column(name="source_event_id",nullable=false) UUID sourceEventId; @Column(name="projected_at",nullable=false) Instant projectedAt; protected OrderReadModelJpaEntity(){} }
