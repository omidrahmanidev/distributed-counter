package ro.midra.stream.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(StreamsSettings.class)
public class StreamsRuntime {
  @Bean(initMethod = "start", destroyMethod = "close")
  KafkaStreams streams(
      MeterRegistry metrics,
      StreamsSettings settings,
      @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String brokers) {
    var streams =
        new KafkaStreams(
            CounterTopology.build(
                settings.dedupRetention(),
                settings.snapshotInterval(),
                settings.shardCount(),
                settings.snapshotBatchSize(),
                metrics),
            properties(settings, brokers));
    streams.setUncaughtExceptionHandler(
        error -> StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
    return streams;
  }

  private Properties properties(StreamsSettings settings, String brokers) {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, settings.applicationId());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(StreamsConfig.STATE_DIR_CONFIG, settings.stateDirectory());
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, settings.standbyReplicas());
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, settings.replicationFactor());
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, settings.streamThreads());
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    return props;
  }

  @Bean(destroyMethod = "close")
  KafkaStreamsMetrics streamMetrics(KafkaStreams streams, MeterRegistry registry) {
    var binder = new KafkaStreamsMetrics(streams);
    binder.bindTo(registry);
    return binder;
  }

  @Bean
  HealthIndicator streamsLivenessHealthIndicator(KafkaStreams streams) {
    return () ->
        streams.state() == KafkaStreams.State.ERROR
                || streams.state() == KafkaStreams.State.NOT_RUNNING
            ? Health.down().build()
            : Health.up().build();
  }

  @Bean
  HealthIndicator streamsHealthIndicator(KafkaStreams streams) {
    return () ->
        streams.state() == KafkaStreams.State.RUNNING
            ? Health.up().build()
            : Health.down().withDetail("state", streams.state()).build();
  }
}
