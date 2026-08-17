package com.zakaria.eventflow.application.service;
import com.zakaria.eventflow.application.port.out.*;
import com.zakaria.eventflow.domain.event.OrderCreatedEvent;
import com.zakaria.eventflow.domain.model.Order;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.Currency;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
class CreateOrderServiceTest {
 @Test void persists_order_and_emits_domain_event(){OrderRepository orders=mock(OrderRepository.class);DomainEventPublisher events=mock(DomainEventPublisher.class);when(orders.save(any(Order.class))).thenAnswer(i->i.getArgument(0));CreateOrderService service=new CreateOrderService(orders,events);Order created=service.create("customer-42",new BigDecimal("129.90"),Currency.getInstance("EUR"));assertThat(created.customerId()).isEqualTo("customer-42");verify(orders).save(created);verify(events).publish(argThat(e->e instanceof OrderCreatedEvent ce&&ce.aggregateId().equals(created.id())));}
}
