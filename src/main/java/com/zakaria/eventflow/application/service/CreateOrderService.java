package com.zakaria.eventflow.application.service;

import com.zakaria.eventflow.application.port.in.CreateOrderUseCase;
import com.zakaria.eventflow.application.port.out.DomainEventPublisher;
import com.zakaria.eventflow.application.port.out.OrderRepository;
import com.zakaria.eventflow.domain.event.OrderCreatedEvent;
import com.zakaria.eventflow.domain.model.Order;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.util.Currency;
import java.util.UUID;

@Service
public class CreateOrderService implements CreateOrderUseCase {
    private final OrderRepository orderRepository; private final DomainEventPublisher eventPublisher;
    public CreateOrderService(OrderRepository orderRepository, DomainEventPublisher eventPublisher) { this.orderRepository = orderRepository; this.eventPublisher = eventPublisher; }
    @Override @Transactional
    public Order create(String customerId, BigDecimal amount, Currency currency) {
        Order saved = orderRepository.save(Order.accept(customerId, amount, currency));
        eventPublisher.publish(new OrderCreatedEvent(UUID.randomUUID(), saved.id(), saved.customerId(), saved.amount(), saved.currency().getCurrencyCode(), saved.createdAt()));
        return saved;
    }
}
