package com.zakaria.eventflow;

import com.zakaria.eventflow.adapter.out.messaging.OrderProjectionHandler;
import com.zakaria.eventflow.domain.event.OrderCreatedEvent;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
class OrderProjectionIdempotencyIT {

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
    OrderProjectionHandler handler;

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void concurrent_duplicate_deliveries_create_one_projection() throws Exception {
        OrderCreatedEvent event = new OrderCreatedEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "customer-idempotent",
                new BigDecimal("42.00"),
                "EUR",
                Instant.now()
        );

        int workers = 8;
        ExecutorService executor = Executors.newFixedThreadPool(workers);
        CountDownLatch ready = new CountDownLatch(workers);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(workers);
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());

        try {
            for (int i = 0; i < workers; i++) {
                executor.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        handler.handle(event);
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
        assertThat(jdbc.queryForObject("select count(*) from processed_events", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from order_read_model", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "select source_event_id from order_read_model where order_id = ?",
                UUID.class,
                event.aggregateId()
        )).isEqualTo(event.eventId());
    }
}
