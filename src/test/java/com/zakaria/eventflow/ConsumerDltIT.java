package com.zakaria.eventflow;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(properties = {
        "eventflow.outbox.enabled=false",
        "eventflow.kafka.consumer-retry.backoff-ms=50",
        "eventflow.kafka.consumer-retry.retries=2",
        "spring.kafka.admin.fail-fast=true",
        "management.otlp.tracing.export.enabled=false"
})
class ConsumerDltIT {

    private static final String ORDER_TOPIC = "orders.created.v1";
    private static final String CONSUMER_DLT = "orders.created.v1.consumer.DLT";

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
    KafkaTemplate<String, String> kafka;

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void malformed_record_is_retried_then_published_to_consumer_dlt() throws Exception {
        String poisonPayload = "{not-valid-json";

        try (KafkaConsumer<String, String> dltConsumer = new KafkaConsumer<>(consumerProperties())) {
            dltConsumer.subscribe(List.of(CONSUMER_DLT));
            dltConsumer.poll(Duration.ofMillis(500));

            kafka.send(ORDER_TOPIC, "poison-order", poisonPayload).get(5, TimeUnit.SECONDS);

            ConsumerRecord<String, String> deadLetter = awaitDeadLetter(dltConsumer, Duration.ofSeconds(20));

            assertThat(deadLetter).isNotNull();
            assertThat(deadLetter.key()).isEqualTo("poison-order");
            assertThat(deadLetter.value()).isEqualTo(poisonPayload);
            assertThat(deadLetter.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotNull();
            assertThat(deadLetter.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)).isNotNull();
            assertThat(jdbc.queryForObject("select count(*) from order_read_model", Integer.class)).isZero();
        }
    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "eventflow-dlt-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return properties;
    }

    private ConsumerRecord<String, String> awaitDeadLetter(
            KafkaConsumer<String, String> consumer,
            Duration timeout
    ) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(250));
            for (ConsumerRecord<String, String> record : records) {
                return record;
            }
        }
        return null;
    }
}
