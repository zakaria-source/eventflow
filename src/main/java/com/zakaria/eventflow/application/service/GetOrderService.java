package com.zakaria.eventflow.application.service;

import com.zakaria.eventflow.application.port.in.GetOrderQuery;
import com.zakaria.eventflow.application.port.out.OrderRepository;
import com.zakaria.eventflow.domain.model.Order;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.Optional;
import java.util.UUID;

@Service
public class GetOrderService implements GetOrderQuery {
    private final OrderRepository orderRepository;
    public GetOrderService(OrderRepository orderRepository) { this.orderRepository = orderRepository; }
    @Override @Transactional(readOnly = true) public Optional<Order> findById(UUID id) { return orderRepository.findById(id); }
}
