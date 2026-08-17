# ADR-0004: Persist W3C trace context across the transactional outbox

- Status: Accepted
- Date: 2026-08-17

## Context

A transactional outbox deliberately separates the business transaction from the later Kafka publication. That separation also breaks ordinary thread-local trace propagation: the HTTP request finishes, the event waits in PostgreSQL, and a scheduler publishes it later on another thread or replica.

Without explicit propagation, the request, outbox relay and Kafka consumer are observable but appear as unrelated traces.

## Decision

EventFlow persists the W3C trace carrier with each outbox row.

1. When the domain event is written, `OutboxTraceContext` injects the current OpenTelemetry context using `W3CTraceContextPropagator` and serializes the carrier into `outbox_events.trace_context`.
2. The outbox claim query returns that carrier together with the event payload.
3. Before creating the `eventflow.outbox.publish` observation, the relay extracts the carrier into a fresh OpenTelemetry `Context.root()` and makes it current for the publish operation.
4. Spring Kafka observation is enabled for both `KafkaTemplate` and listener containers. The producer observation therefore injects the restored context into Kafka headers and the listener observation extracts it on the consumer side.

The domain model remains independent of tracing APIs; propagation is confined to the messaging adapter.

## Why restore from `Context.root()`?

The relay runs on a scheduled worker that may itself have an observation in scope. Restoring from the root prevents the scheduler's execution span from accidentally becoming the business parent. The persisted request context is the authoritative parent for the event publication.

## Why persist a W3C carrier instead of only trace ID and span ID?

The carrier preserves standard propagation semantics, including trace flags and `tracestate`, and avoids inventing a project-specific trace identifier format. It also keeps the stored representation compatible with standard W3C tracing tooling.

## Consequences

### Positive

- the business trace can continue across a database-backed asynchronous delay;
- Kafka producer and consumer spans attach to the same distributed trace;
- no trace context is held in memory while the event waits in PostgreSQL;
- relay failover between Kubernetes replicas does not lose the trace parent;
- tracing remains outside the domain layer.

### Trade-offs

- tracing metadata is stored with the outbox event;
- malformed stored trace context must be treated as infrastructure corruption;
- the current implementation persists trace context, not application baggage;
- retention of old outbox rows also retains their trace carrier until cleanup.

## Verification

`OutboxTraceContextIT` creates a real Micrometer span, writes an order and outbox event inside that span, reads the serialized carrier from PostgreSQL, restores it outside the original scope, and verifies that the original trace ID becomes current again.

`OrderPipelineIT` continues to verify the complete PostgreSQL -> outbox relay -> Kafka -> consumer -> projection path with Kafka observations enabled.
