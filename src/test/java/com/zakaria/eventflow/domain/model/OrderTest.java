package com.zakaria.eventflow.domain.model;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.Currency;
import static org.assertj.core.api.Assertions.*;
class OrderTest {
 @Test void rejects_non_positive_amounts(){assertThatThrownBy(()->Order.accept("customer-1",BigDecimal.ZERO,Currency.getInstance("EUR"))).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("positive");}
 @Test void accepts_a_valid_order(){Order o=Order.accept("customer-1",new BigDecimal("42.50"),Currency.getInstance("EUR"));assertThat(o.status()).isEqualTo(OrderStatus.ACCEPTED);assertThat(o.id()).isNotNull();}
}
