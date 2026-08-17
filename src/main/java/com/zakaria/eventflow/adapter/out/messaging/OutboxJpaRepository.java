package com.zakaria.eventflow.adapter.out.messaging;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
interface OutboxJpaRepository extends JpaRepository<OutboxEventJpaEntity, UUID> {
    @Query("select e from OutboxEventJpaEntity e where e.publishedAt is null and (e.nextAttemptAt is null or e.nextAttemptAt <= :now) order by e.occurredAt asc")
    List<OutboxEventJpaEntity> findPending(@Param("now") Instant now, Pageable pageable);
}
