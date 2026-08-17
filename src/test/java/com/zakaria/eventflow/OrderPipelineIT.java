package com.zakaria.eventflow;

import com.zakaria.eventflow.application.port.in.CreateOrderUseCase;
import com.zakaria.eventflow.domain.model.Order;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Currency;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(properties = {
        "eventflow.outbox.fixed-delay=100",
        "spring.kafka.admin.fail-fast=true",
        "management.otlp.tracing.export.enabled=false"
})
class OrderPipelineIT {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("eventflow")
            .withUsername("eventflow")
            .withPassword("eventflow");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0")
    ).withKraft();

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    CreateOrderUseCase createOrder;

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void order_flows_from_transactional_outbox_through_kafka_to_projection() {
        Order order = createOrder.create(
                "customer-pipeline",
                new BigDecimal("129.90"),
                Currency.getInstance("EUR")
        );

        Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    Integer published = jdbc.queryForObject(
                            "select count(*) from outbox_events where aggregate_id = ? and published_at is not null",
                            Integer.class,
                            order.id()
                    );
                    Integer activeClaims = jdbc.queryForObject(
                            "select count(*) from outbox_events where aggregate_id = ? and (claimed_by is not null or claimed_until is not null)",
                            Integer.class,
                            order.id()
                    );
                    Integer projected = jdbc.queryForObject(
                            "select count(*) from order_read_model where order_id = ?",
                            Integer.class,
                            order.id()
                    );
                    Integer processed = jdbc.queryForObject(
                            "select count(*) from processed_events",
                            Integer.class
                    );
                    Integer projectedWithExpectedCustomer = jdbc.queryForObject(
                            "select count(*) from order_read_model where order_id = ? and customer_id = ?",
                            Integer.class,
                            order.id(),
                            "customer-pipeline"
                    );

                    assertThat(published).isEqualTo(1);
                    assertThat(activeClaims).isZero();
                    assertThat(projected).isEqualTo(1);
                    assertThat(processed).isEqualTo(1);
                    assertThat(projectedWithExpectedCustomer).isEqualTo(1);
                });
    }
}
