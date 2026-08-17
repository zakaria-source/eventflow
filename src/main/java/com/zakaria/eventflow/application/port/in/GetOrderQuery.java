package com.zakaria.eventflow.application.port.in;

import com.zakaria.eventflow.domain.model.Order;
import java.util.Optional;
import java.util.UUID;

public interface GetOrderQuery { Optional<Order> findById(UUID id); }
