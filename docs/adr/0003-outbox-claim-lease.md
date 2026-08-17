# ADR-0003: PostgreSQL claim leases for multi-replica outbox publishing

- Status: Accepted
- Date: 2026-08-17

## Context

EventFlow can run more than one application replica in Kubernetes. A plain polling query such as `SELECT ... WHERE published_at IS NULL` allows two relay instances to read the same pending outbox row and publish it concurrently.

Consumer idempotency protects downstream state from duplicate delivery, but it does not make duplicate broker traffic desirable. The relay should divide pending work between healthy replicas without holding a database transaction open while waiting for Kafka.

## Decision

Each relay iteration first executes a short PostgreSQL transaction that atomically claims a batch:

1. eligible rows are selected in creation order;
2. `FOR UPDATE SKIP LOCKED` prevents concurrent workers from selecting rows currently being claimed by another transaction;
3. the same statement writes a unique `claimed_by` token and a `claimed_until` lease deadline;
4. the database transaction commits before any Kafka network call starts;
5. successful or failed publication clears the lease when the outbox status is updated.

The claim is implemented as a PostgreSQL CTE followed by `UPDATE ... RETURNING`, so selection and lease persistence are one atomic database operation.

## Why a lease instead of holding row locks during Kafka publication?

Keeping `SELECT ... FOR UPDATE` locks open while calling Kafka would make database transaction duration depend on network latency and broker health. It would also increase lock contention and complicate status updates.

A persisted lease keeps the critical database transaction short while ensuring another replica will not immediately claim the same event.

## Crash recovery

If a relay process dies after claiming rows but before updating their status, the rows become eligible again when `claimed_until` expires. This gives the system recovery without manual intervention.

A crash after Kafka acknowledges the event but before `published_at` is stored can still cause a later duplicate publication. EventFlow deliberately provides at-least-once delivery and relies on the consumer's atomic event-ID claim to make that duplicate safe.

## Consequences

### Positive

- multiple relay replicas can share work;
- healthy workers skip rows locked by another claim transaction;
- Kafka calls happen outside the database transaction;
- abandoned work is recoverable through lease expiry;
- duplicate publication caused by ordinary concurrent polling is reduced.

### Trade-offs

- the lease duration must exceed normal publication time but stay short enough for useful crash recovery;
- clock behavior matters because lease expiry is time-based;
- at-least-once delivery still requires idempotent consumers;
- published and processed outbox records still need a retention/cleanup policy.

## Verification

`OutboxClaimIT` creates six outbox events and launches two workers concurrently. It verifies that both workers receive disjoint batches, all six rows are leased exactly once across the two claims, an immediate third claim receives no work, and expired leases become claimable again.
