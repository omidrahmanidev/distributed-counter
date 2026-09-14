package ro.midra.stream.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("counter")
public record StreamsSettings(
    @DefaultValue("video-counter-v1") String applicationId,
    @DefaultValue("/tmp/video-counter-state") String stateDirectory,
    @DefaultValue("P7D") Duration dedupRetention,
    @DefaultValue("PT1S") Duration snapshotInterval,
    @DefaultValue("128") int shardCount,
    @DefaultValue("0") int standbyReplicas,
    @DefaultValue("1") int replicationFactor,
    @DefaultValue("2") int streamThreads,
    @DefaultValue("10000") int snapshotBatchSize) {
  public StreamsSettings {
    if (applicationId.isBlank()
        || stateDirectory.isBlank()
        || dedupRetention.isNegative()
        || dedupRetention.isZero()
        || snapshotInterval.isNegative()
        || snapshotInterval.isZero()
        || shardCount <= 0
        || standbyReplicas < 0
        || replicationFactor <= 0
        || streamThreads <= 0
        || snapshotBatchSize <= 0)
      throw new IllegalArgumentException("Invalid Streams configuration");
  }
}
