package com.zakaria.eventflow;

import com.zakaria.eventflow.adapter.out.messaging.OutboxTraceContext;
import com.zakaria.eventflow.application.port.in.CreateOrderUseCase;
import com.zakaria.eventflow.domain.model.Order;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
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
import java.util.Currency;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=false",
        "eventflow.outbox.enabled=false",
        "eventflow.kafka.admin-enabled=false",
        "management.otlp.tracing.export.enabled=false"
})
class OutboxTraceContextIT {

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
    OutboxTraceContext outboxTraceContext;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    Tracer tracer;

    @Test
    void trace_context_survives_the_database_backed_async_boundary() {
        Span parent = tracer.nextSpan().name("test-order-request").start();
        String expectedTraceId = parent.context().traceId();
        Order order;

        try (Tracer.SpanInScope ignored = tracer.withSpan(parent)) {
            order = createOrder.create(
                    "customer-traced",
                    new BigDecimal("55.00"),
                    Currency.getInstance("EUR")
            );
        } finally {
            parent.end();
        }

        String serializedCarrier = jdbc.queryForObject(
                "select trace_context from outbox_events where aggregate_id = ?",
                String.class,
                order.id()
        );

        assertThat(serializedCarrier).isNotBlank();
        assertThat(serializedCarrier).contains(expectedTraceId);

        AtomicReference<String> restoredTraceId = new AtomicReference<>();
        outboxTraceContext.runWith(serializedCarrier, () -> restoredTraceId.set(
                io.opentelemetry.api.trace.Span.current().getSpanContext().getTraceId()
        ));

        assertThat(restoredTraceId.get()).isEqualTo(expectedTraceId);
    }
}
