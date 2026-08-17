package com.zakaria.eventflow.adapter.out.messaging;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Component
public class OutboxClaimService {

    private static final String CLAIM_SQL = """
            WITH candidates AS (
                SELECT event_id
                FROM outbox_events
                WHERE published_at IS NULL
                  AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                  AND (claimed_until IS NULL OR claimed_until <= ?)
                ORDER BY occurred_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT ?
            )
            UPDATE outbox_events AS event
            SET claimed_by = ?, claimed_until = ?
            FROM candidates
            WHERE event.event_id = candidates.event_id
            RETURNING event.event_id,
                      event.aggregate_id,
                      event.event_type,
                      event.topic,
                      event.payload,
                      event.trace_context
            """;

    private final JdbcTemplate jdbc;

    public OutboxClaimService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional
    public List<ClaimedOutboxEvent> claimPending(Instant now, int batchSize, Duration claimTtl) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        if (claimTtl.isZero() || claimTtl.isNegative()) {
            throw new IllegalArgumentException("claimTtl must be positive");
        }

        String claimId = UUID.randomUUID().toString();
        Instant claimedUntil = now.plus(claimTtl);

        return jdbc.query(connection -> {
            PreparedStatement statement = connection.prepareStatement(CLAIM_SQL);
            statement.setTimestamp(1, Timestamp.from(now));
            statement.setTimestamp(2, Timestamp.from(now));
            statement.setInt(3, batchSize);
            statement.setString(4, claimId);
            statement.setTimestamp(5, Timestamp.from(claimedUntil));
            return statement;
        }, (resultSet, rowNum) -> new ClaimedOutboxEvent(
                resultSet.getObject("event_id", UUID.class),
                resultSet.getObject("aggregate_id", UUID.class),
                resultSet.getString("event_type"),
                resultSet.getString("topic"),
                resultSet.getString("payload"),
                resultSet.getString("trace_context")
        ));
    }

    public record ClaimedOutboxEvent(
            UUID eventId,
            UUID aggregateId,
            String eventType,
            String topic,
            String payload,
            String traceContext
    ) {
    }
}
