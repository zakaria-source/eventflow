package com.zakaria.eventflow.adapter.out.persistence;

import com.zakaria.eventflow.application.port.out.OrderRepository;
import com.zakaria.eventflow.domain.model.Order;
import com.zakaria.eventflow.domain.model.OrderStatus;
import org.springframework.stereotype.Repository;
import java.util.Currency;
import java.util.Optional;
import java.util.UUID;

@Repository
public class JpaOrderRepositoryAdapter implements OrderRepository {
    private final SpringDataOrderRepository repository;
    public JpaOrderRepositoryAdapter(SpringDataOrderRepository repository) { this.repository = repository; }
    public Order save(Order order) {
        OrderJpaEntity e = new OrderJpaEntity(); e.id=order.id(); e.customerId=order.customerId(); e.amount=order.amount(); e.currency=order.currency().getCurrencyCode(); e.status=order.status().name(); e.createdAt=order.createdAt(); return toDomain(repository.save(e));
    }
    public Optional<Order> findById(UUID id) { return repository.findById(id).map(this::toDomain); }
    private Order toDomain(OrderJpaEntity e) { return new Order(e.id,e.customerId,e.amount,Currency.getInstance(e.currency),OrderStatus.valueOf(e.status),e.createdAt); }
}
