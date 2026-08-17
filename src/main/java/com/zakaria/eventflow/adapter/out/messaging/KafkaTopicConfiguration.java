package com.zakaria.eventflow.adapter.out.messaging;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.TopicBuilder;
@Configuration
@ConditionalOnProperty(name="eventflow.kafka.admin-enabled",havingValue="true",matchIfMissing=true)
public class KafkaTopicConfiguration {
    @Bean NewTopic orderCreatedTopic(@Value("${eventflow.kafka.topics.order-created}")String name){return TopicBuilder.name(name).partitions(3).replicas(1).build();}
    @Bean NewTopic orderCreatedDltTopic(@Value("${eventflow.kafka.topics.order-created-dlt}")String name){return TopicBuilder.name(name).partitions(3).replicas(1).build();}
}
