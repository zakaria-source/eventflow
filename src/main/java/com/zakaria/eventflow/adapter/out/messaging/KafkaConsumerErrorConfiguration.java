package com.zakaria.eventflow.adapter.out.messaging;

import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerErrorConfiguration {

    @Bean
    CommonErrorHandler kafkaConsumerErrorHandler(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${eventflow.kafka.topics.order-created-consumer-dlt}") String consumerDltTopic,
            @Value("${eventflow.kafka.consumer-retry.backoff-ms:250}") long backoffMillis,
            @Value("${eventflow.kafka.consumer-retry.retries:2}") long retries
    ) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, failure) -> new TopicPartition(consumerDltTopic, record.partition())
        );
        recoverer.setFailIfSendResultIsError(true);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(backoffMillis, retries)
        );
        errorHandler.setResetStateOnExceptionChange(true);
        return errorHandler;
    }
}
