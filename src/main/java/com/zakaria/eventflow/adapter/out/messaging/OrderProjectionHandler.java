package com.zakaria.eventflow.adapter.out.messaging;
import com.zakaria.eventflow.domain.event.OrderCreatedEvent;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
@Component
public class OrderProjectionHandler {
    private final ProcessedEventJpaRepository processed; private final OrderReadModelJpaRepository readModel;
    public OrderProjectionHandler(ProcessedEventJpaRepository processed,OrderReadModelJpaRepository readModel){this.processed=processed;this.readModel=readModel;}
    @Transactional public void handle(OrderCreatedEvent event){if(processed.existsById(event.eventId()))return;OrderReadModelJpaEntity p=new OrderReadModelJpaEntity();p.orderId=event.aggregateId();p.customerId=event.customerId();p.amount=event.amount();p.currency=event.currency();p.sourceEventId=event.eventId();p.projectedAt=Instant.now();readModel.save(p);ProcessedEventJpaEntity done=new ProcessedEventJpaEntity();done.eventId=event.eventId();done.processedAt=Instant.now();processed.save(done);}
}
