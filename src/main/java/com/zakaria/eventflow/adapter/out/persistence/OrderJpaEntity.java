package com.zakaria.eventflow.adapter.out.persistence;
import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
@Entity @Table(name="orders")
class OrderJpaEntity {
    @Id UUID id;
    @Column(name="customer_id", nullable=false) String customerId;
    @Column(nullable=false, precision=19, scale=2) BigDecimal amount;
    @Column(nullable=false, length=3) String currency;
    @Column(nullable=false, length=32) String status;
    @Column(name="created_at", nullable=false) Instant createdAt;
    protected OrderJpaEntity() {}
}
