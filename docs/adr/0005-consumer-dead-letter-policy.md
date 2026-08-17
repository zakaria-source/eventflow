# ADR-0005: Bounded consumer retries with a dedicated dead-letter topic

- Status: Accepted
- Date: 2026-08-17

## Context

An event can be published successfully to Kafka and still be impossible for the consumer to process because of malformed payloads, incompatible data or deterministic application failures. Retrying such a record forever can block useful work on a partition and hides the poison message inside repeated logs.

EventFlow already had a dead-letter path for failures in the outbox publisher. That failure domain is different: the relay could not successfully publish an outbox event. Reusing the same topic for consumer processing failures would make diagnosis ambiguous.

## Decision

EventFlow uses Spring Kafka's `DefaultErrorHandler` with a bounded `FixedBackOff` and a `DeadLetterPublishingRecoverer`.

- initial listener delivery plus two retries;
- fixed backoff between attempts;
- after the retry budget is exhausted, the original Kafka record is published to `orders.created.v1.consumer.DLT`;
- the DLT record retains Spring Kafka diagnostic headers describing the original topic and failure;
- outbox relay publication failures use the separate `orders.created.v1.publish.DLT` topic.

Spring Boot's auto-configured Kafka listener factory consumes the unique `CommonErrorHandler` bean, so EventFlow keeps the standard Boot listener configuration instead of replacing the whole factory.

## Why blocking retries?

The retry budget is deliberately small and local to the listener. This preserves the ordering behavior of the original partition during the brief retry window and avoids introducing retry-topic infrastructure for a demo that currently has only one event flow.

For long retry windows or high-volume workloads, non-blocking retry topics would be a separate architectural choice and would need an explicit discussion of ordering trade-offs.

## Consequences

### Positive

- poison records stop retrying forever;
- consumer failures and publisher failures are operationally distinguishable;
- the failed payload remains inspectable in Kafka;
- the normal projection is not written for malformed records;
- retry policy is configurable rather than hidden in listener code.

### Trade-offs

- blocking backoff pauses consumption of that partition during retries;
- DLT records require an operational replay/triage procedure;
- deterministic validation/deserialization failures might be classified for immediate recovery in a larger system rather than consuming the retry budget;
- sending to a DLT on the same Kafka cluster does not protect against a total broker outage.

## Verification

`ConsumerDltIT` starts PostgreSQL and Kafka with Testcontainers, publishes malformed JSON to `orders.created.v1`, waits for the listener retry policy to exhaust, consumes the resulting record from `orders.created.v1.consumer.DLT`, verifies that the key and payload are preserved and that Spring Kafka DLT diagnostic headers exist, and confirms that no business projection was created.
