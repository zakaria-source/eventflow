package com.zakaria.eventflow.adapter.out.messaging;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;
interface OrderReadModelJpaRepository extends JpaRepository<OrderReadModelJpaEntity,UUID>{}
