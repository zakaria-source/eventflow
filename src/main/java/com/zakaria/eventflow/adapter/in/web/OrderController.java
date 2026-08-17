package com.zakaria.eventflow.adapter.in.web;

import com.zakaria.eventflow.application.port.in.CreateOrderUseCase;
import com.zakaria.eventflow.application.port.in.GetOrderQuery;
import com.zakaria.eventflow.domain.model.Order;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import java.net.URI;
import java.util.Currency;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {
    private final CreateOrderUseCase createOrder; private final GetOrderQuery getOrder;
    public OrderController(CreateOrderUseCase createOrder, GetOrderQuery getOrder) { this.createOrder = createOrder; this.getOrder = getOrder; }
    @PostMapping public ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {
        Order order = createOrder.create(request.customerId(), request.amount(), Currency.getInstance(request.currency()));
        URI location = ServletUriComponentsBuilder.fromCurrentRequest().path("/{id}").buildAndExpand(order.id()).toUri();
        return ResponseEntity.created(location).body(OrderResponse.from(order));
    }
    @GetMapping("/{id}") public ResponseEntity<OrderResponse> get(@PathVariable UUID id) {
        return getOrder.findById(id).map(OrderResponse::from).map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }
}
