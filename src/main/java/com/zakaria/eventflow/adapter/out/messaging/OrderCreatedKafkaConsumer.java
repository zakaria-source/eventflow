package com.zakaria.eventflow.adapter.out.messaging;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zakaria.eventflow.domain.event.OrderCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
@Component
public class OrderCreatedKafkaConsumer {
    private final ObjectMapper mapper; private final OrderProjectionHandler handler;
    public OrderCreatedKafkaConsumer(ObjectMapper mapper,OrderProjectionHandler handler){this.mapper=mapper;this.handler=handler;}
    @KafkaListener(topics="${eventflow.kafka.topics.order-created}") public void consume(ConsumerRecord<String,String> record)throws JsonProcessingException{handler.handle(mapper.readValue(record.value(),OrderCreatedEvent.class));}
}
