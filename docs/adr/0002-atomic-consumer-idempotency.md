# ADR-0002: Claim consumed events atomically

**Status:** Accepted

## Context

Kafka delivery is treated as at-least-once, so the same event can be delivered more than once. A naive `exists(eventId)` followed by `insert(eventId)` is not safe under concurrency: two consumers can observe the event as absent before either transaction inserts it.

## Decision

Claim an event with a single PostgreSQL statement:

```sql
INSERT INTO processed_events (event_id, processed_at)
VALUES (?, ?)
ON CONFLICT (event_id) DO NOTHING;
```

The returned update count decides whether the consumer owns the event. The claim and the read-model update run in the same database transaction.

## Consequences

Positive:

- duplicate delivery becomes safe even when duplicates are processed concurrently;
- there is no check-then-act race between `exists` and `insert`;
- if projection fails, the event claim rolls back with it and the event can be retried.

Trade-offs:

- the implementation uses PostgreSQL-specific `ON CONFLICT` semantics;
- `processed_events` grows over time and needs a retention strategy at scale;
- the idempotency boundary is the event ID, so producers must keep event IDs stable across retries.
