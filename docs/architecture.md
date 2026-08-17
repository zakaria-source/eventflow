# Architecture notes

## Consistency model
The synchronous API guarantees that an accepted order is durable in PostgreSQL together with a durable intent to publish `orders.created.v1`. Kafka publication is intentionally asynchronous.

## Delivery semantics
Kafka and the relay are treated as **at-least-once**. Exactly-once business behavior is achieved through idempotent consumers rather than pretending the transport never duplicates messages.

## Scaling the outbox
The demo polls a bounded batch. At higher throughput I would add `FOR UPDATE SKIP LOCKED` semantics or partition the outbox so multiple relay instances can publish concurrently.

## Ordering
Messages are keyed by `orderId`, preserving per-order partition ordering. Global ordering is intentionally not promised.

## Schema evolution
The first contract is `orders.created.v1`. Breaking changes should produce a new version rather than silently changing an existing event.
