package com.zakaria.eventflow;
import com.zakaria.eventflow.application.port.in.CreateOrderUseCase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.*;
import java.math.BigDecimal;
import java.util.Currency;
import static org.assertj.core.api.Assertions.assertThat;
@Testcontainers
@SpringBootTest(properties={"spring.kafka.listener.auto-startup=false","eventflow.outbox.enabled=false","eventflow.kafka.admin-enabled=false"})
class OrderOutboxIT {
 @Container static final PostgreSQLContainer<?> POSTGRES=new PostgreSQLContainer<>("postgres:17-alpine").withDatabaseName("eventflow").withUsername("eventflow").withPassword("eventflow");
 @DynamicPropertySource static void db(DynamicPropertyRegistry r){r.add("spring.datasource.url",POSTGRES::getJdbcUrl);r.add("spring.datasource.username",POSTGRES::getUsername);r.add("spring.datasource.password",POSTGRES::getPassword);}
 @Autowired CreateOrderUseCase createOrder; @Autowired JdbcTemplate jdbc;
 @Test void order_and_outbox_event_are_committed_together(){createOrder.create("customer-it",new BigDecimal("75.00"),Currency.getInstance("EUR"));assertThat(jdbc.queryForObject("select count(*) from orders",Integer.class)).isEqualTo(1);assertThat(jdbc.queryForObject("select count(*) from outbox_events",Integer.class)).isEqualTo(1);}
}
