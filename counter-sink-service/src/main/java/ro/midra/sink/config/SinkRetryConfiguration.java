package ro.midra.sink.config;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class SinkRetryConfiguration {
  @Bean
  DefaultErrorHandler errorHandler(
      KafkaTemplate<Object, Object> template,
      @Value("${counter.retry.initial-ms:500}") long initial,
      @Value("${counter.retry.maximum-ms:30000}") long maximum) {
    var recoverer =
        new DeadLetterPublishingRecoverer(
            template,
            (record, error) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
    recoverer.setFailIfSendResultIsError(true);
    var backoff = new ExponentialBackOff(initial, 2);
    backoff.setMaxInterval(maximum);
    backoff.setMaxElapsedTime(Long.MAX_VALUE);
    var handler = new DefaultErrorHandler(recoverer, backoff);
    handler.addNotRetryableExceptions(SerializationException.class, IllegalArgumentException.class);
    return handler;
  }
}
