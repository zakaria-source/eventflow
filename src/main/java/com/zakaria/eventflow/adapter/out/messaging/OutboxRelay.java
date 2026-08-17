package com.zakaria.eventflow.adapter.out.messaging;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
@Component
@ConditionalOnProperty(name="eventflow.outbox.enabled",havingValue="true",matchIfMissing=true)
public class OutboxRelay {
    private static final Logger log=LoggerFactory.getLogger(OutboxRelay.class);
    private final OutboxJpaRepository repository; private final OutboxStatusUpdater updater; private final KafkaTemplate<String,String> kafka; private final int batchSize,maxAttempts; private final String dltTopic;
    public OutboxRelay(OutboxJpaRepository repository,OutboxStatusUpdater updater,KafkaTemplate<String,String> kafka,@Value("${eventflow.outbox.batch-size}") int batchSize,@Value("${eventflow.outbox.max-attempts}") int maxAttempts,@Value("${eventflow.kafka.topics.order-created-dlt}") String dltTopic){this.repository=repository;this.updater=updater;this.kafka=kafka;this.batchSize=batchSize;this.maxAttempts=maxAttempts;this.dltTopic=dltTopic;}
    @Scheduled(fixedDelayString="${eventflow.outbox.fixed-delay}") public void publishPending(){repository.findPending(Instant.now(),PageRequest.of(0,batchSize)).forEach(this::publishOne);}
    private void publishOne(OutboxEventJpaEntity e){try{kafka.send(e.topic,e.aggregateId.toString(),e.payload).get(5,TimeUnit.SECONDS);updater.markPublished(e.eventId);log.info("Published event {}",e.eventId);}catch(Exception failure){int attempts=updater.markFailed(e.eventId,failure);log.warn("Publish failed event={} attempt={}",e.eventId,attempts);if(attempts>=maxAttempts)sendToDlt(e);}}
    private void sendToDlt(OutboxEventJpaEntity e){try{kafka.send(dltTopic,e.aggregateId.toString(),e.payload).get(5,TimeUnit.SECONDS);updater.markPublished(e.eventId);log.error("Moved event to DLT event={}",e.eventId);}catch(Exception failure){log.error("DLT publish failed event={}",e.eventId,failure);}}
}
