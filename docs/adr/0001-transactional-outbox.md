# ADR-0001: Use a transactional outbox for domain-event publication

**Status:** Accepted

## Context
Creating an order modifies PostgreSQL and must also emit an event to Kafka. Two independent writes can leave the systems inconsistent.

## Decision
Persist the order and a serialized domain event in `outbox_events` in the same PostgreSQL transaction. A separate relay publishes committed rows to Kafka.

## Consequences
Positive: no lost event after a committed order; API availability is decoupled from transient broker outages; publication attempts are observable and retryable.

Trade-offs: publication is eventually consistent; duplicate Kafka messages remain possible; the outbox requires retention/cleanup at scale.
