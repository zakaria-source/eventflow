package com.zakaria.eventflow;

import com.zakaria.eventflow.adapter.out.messaging.OutboxClaimService;
import com.zakaria.eventflow.application.port.in.CreateOrderUseCase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Currency;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=false",
        "eventflow.outbox.enabled=false",
        "eventflow.kafka.admin-enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
class OutboxClaimIT {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("eventflow")
            .withUsername("eventflow")
            .withPassword("eventflow");

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    CreateOrderUseCase createOrder;

    @Autowired
    OutboxClaimService claimer;

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void concurrent_workers_claim_disjoint_batches_and_expired_leases_are_recoverable() throws Exception {
        for (int i = 0; i < 6; i++) {
            createOrder.create(
                    "customer-" + i,
                    new BigDecimal("10.00").add(BigDecimal.valueOf(i)),
                    Currency.getInstance("EUR")
            );
        }

        Instant now = Instant.now();
        Duration lease = Duration.ofSeconds(30);
        int workers = 2;
        int batchSize = 3;

        ExecutorService executor = Executors.newFixedThreadPool(workers);
        CountDownLatch ready = new CountDownLatch(workers);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(workers);
        List<List<OutboxClaimService.ClaimedOutboxEvent>> claims = Collections.synchronizedList(new ArrayList<>());
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());

        try {
            for (int i = 0; i < workers; i++) {
                executor.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        claims.add(claimer.claimPending(now, batchSize, lease));
                    } catch (Throwable failure) {
                        failures.add(failure);
                    } finally {
                        done.countDown();
                    }
                });
            }

            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            assertThat(done.await(20, TimeUnit.SECONDS)).isTrue();
        } finally {
            executor.shutdownNow();
        }

        assertThat(failures).isEmpty();
        assertThat(claims).hasSize(workers);

        List<UUID> claimedIds = claims.stream()
                .flatMap(List::stream)
                .map(OutboxClaimService.ClaimedOutboxEvent::eventId)
                .toList();
        Set<UUID> uniqueIds = new HashSet<>(claimedIds);

        assertThat(claimedIds).hasSize(6);
        assertThat(uniqueIds).hasSize(6);
        assertThat(jdbc.queryForObject(
                "select count(*) from outbox_events where claimed_by is not null and claimed_until is not null",
                Integer.class
        )).isEqualTo(6);
        assertThat(jdbc.queryForObject(
                "select count(distinct claimed_by) from outbox_events",
                Integer.class
        )).isEqualTo(2);

        assertThat(claimer.claimPending(Instant.now(), batchSize, lease)).isEmpty();

        jdbc.update("update outbox_events set claimed_until = now() - interval '1 second'");

        List<OutboxClaimService.ClaimedOutboxEvent> recovered =
                claimer.claimPending(Instant.now(), batchSize, lease);

        assertThat(recovered).hasSize(batchSize);
        assertThat(uniqueIds).containsAll(recovered.stream()
                .map(OutboxClaimService.ClaimedOutboxEvent::eventId)
                .toList());
    }
}
