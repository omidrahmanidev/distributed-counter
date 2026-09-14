package ro.midra.view.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import ro.midra.contracts.JsonSerde;
import ro.midra.contracts.ViewEvent;
import ro.midra.view.adapter.out.KafkaViewEventPublisher;
import ro.midra.view.application.ViewEventPublisher;
import ro.midra.view.domain.CounterShardSelector;
import ro.midra.view.domain.EventIdentity;

import java.time.Clock;
import java.util.Map;

@Configuration
@EnableConfigurationProperties(ProducerSettings.class)
public class IngestionConfiguration {
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    EventIdentity identity(@Value("${counter.hmac-secret}") String secret) {
        return new EventIdentity(secret);
    }

    @Bean
    CounterShardSelector shards(@Value("${counter.shard-count:128}") int count) {
        return new CounterShardSelector(count);
    }

    @Bean(destroyMethod = "close")
    KafkaSender<String, ViewEvent> sender(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String brokers,
            ProducerSettings settings) {
        Map<String, Object> properties = new java.util.HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, settings.lingerMs());
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, settings.batchBytes());
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, settings.bufferBytes());
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, settings.maxBlockMs());
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, settings.deliveryTimeoutMs());
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, settings.requestTimeoutMs());
        return KafkaSender.create(
                SenderOptions.<String, ViewEvent>create(properties)
                        .withKeySerializer(new StringSerializer())
                        .withValueSerializer(new JsonSerde<>(ViewEvent.class).serializer())
                        .maxInFlight(settings.maxInFlight()));
    }

    @Bean
    ViewEventPublisher publisher(KafkaSender<String, ViewEvent> sender, ProducerSettings settings) {
        return new KafkaViewEventPublisher(sender, settings.capacity());
    }

    @Bean(destroyMethod = "close")
    KafkaClientMetrics producerMetrics(
            KafkaSender<String, ViewEvent> sender, MeterRegistry registry) {
        // Only bean startup waits here; no HTTP/event-loop thread executes this initialization.
        var metrics =
                java.util.Objects.requireNonNull(
                        sender.doOnProducer(KafkaClientMetrics::new).block(java.time.Duration.ofSeconds(10)));
        metrics.bindTo(registry);
        return metrics;
    }
}
