package com.zakaria.eventflow.application.port.in;

import com.zakaria.eventflow.domain.model.Order;
import java.math.BigDecimal;
import java.util.Currency;

public interface CreateOrderUseCase { Order create(String customerId, BigDecimal amount, Currency currency); }
