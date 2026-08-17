package com.zakaria.eventflow.adapter.in.web;
import jakarta.validation.constraints.*;
import java.math.BigDecimal;
public record CreateOrderRequest(@NotBlank String customerId, @NotNull @DecimalMin("0.01") BigDecimal amount, @NotBlank @Pattern(regexp="[A-Z]{3}") String currency) {}
