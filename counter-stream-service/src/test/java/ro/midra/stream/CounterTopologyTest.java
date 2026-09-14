package ro.midra.stream;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ro.midra.contracts.*;
import ro.midra.stream.config.CounterTopology;

class CounterTopologyTest {
  private TopologyTestDriver driver;
  private TestInputTopic<String, ViewEvent> views;
  private TestInputTopic<String, CounterShardSnapshot> shards;
  private TestOutputTopic<String, VideoTotalSnapshot> totals;
  private SimpleMeterRegistry metrics;

  @BeforeEach
  void setUp() {
    metrics = new SimpleMeterRegistry();
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    driver =
        new TopologyTestDriver(
            CounterTopology.build(Duration.ofDays(7), Duration.ofSeconds(1), 128, metrics),
            props,
            Instant.parse("2026-01-01T00:00:00Z"));
    views =
        driver.createInputTopic(
            Topics.VIEWS, new StringSerializer(), new JsonSerde<>(ViewEvent.class).serializer());
    shards =
        driver.createInputTopic(
            Topics.SHARDS,
            new StringSerializer(),
            new JsonSerde<>(CounterShardSnapshot.class).serializer());
    totals =
        driver.createOutputTopic(
            Topics.TOTALS,
            new StringDeserializer(),
            new JsonSerde<>(VideoTotalSnapshot.class).deserializer());
  }

  @AfterEach
  void close() {
    driver.close();
    metrics.close();
  }

  @Test
  void duplicatesCountOnceAndLegitimateRepeatedViewsCountTwice() {
    var first = event("a", 0);
    views.pipeInput("123:0", first);
    views.pipeInput("123:0", first);
    views.pipeInput("123:0", event("b", 0));
    tick();
    assertThat(lastTotal()).isEqualTo(2);
    assertThat(metrics.counter("duplicate_view_events_total").count()).isEqualTo(1);
  }

  @Test
  void incrementallyAggregatesAndRejectsEqualAndOlderSnapshots() {
    shard(0, 100);
    shard(1, 120);
    shard(2, 80);
    tick();
    assertThat(lastTotal()).isEqualTo(300);
    shard(0, 110);
    tick();
    assertThat(lastTotal()).isEqualTo(310);
    shard(0, 110);
    shard(0, 100);
    tick();
    assertThat(totals.isEmpty()).isTrue();
    assertThat(metrics.counter("stale_shard_snapshots_total").count()).isEqualTo(2);
  }

  @Test
  void oldOccurrenceTimeStillCounts() {
    views.pipeInput(
        "123:0",
        new ViewEvent(
            "a".repeat(64),
            123,
            "b".repeat(64),
            0,
            Instant.parse("2020-01-01T00:00:00Z"),
            Instant.now()));
    tick();
    assertThat(lastTotal()).isEqualTo(1);
  }

  @Test
  void retryAfterRetentionMayCountAgain() {
    var event = event("a", 0);
    views.pipeInput("123:0", event);
    tick();
    assertThat(lastTotal()).isEqualTo(1);
    driver.advanceWallClockTime(Duration.ofDays(8));
    views.pipeInput("123:0", event);
    tick();
    assertThat(lastTotal()).isEqualTo(2);
  }

  @Test
  void invalidContractsGoToDeadLetterTopic() {
    var raw =
        driver.createInputTopic(Topics.VIEWS, new StringSerializer(), new ByteArraySerializer());
    raw.pipeInput("123:0", "broken".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    var dead =
        driver.createOutputTopic(
            Topics.VIEWS + ".DLT", new StringDeserializer(), new ByteArrayDeserializer());
    assertThat(new String(dead.readValue(), java.nio.charset.StandardCharsets.UTF_8))
        .isEqualTo("broken");
  }

  private void shard(int id, long count) {
    shards.pipeInput("123", new CounterShardSnapshot(123, id, count, count, Instant.now()));
  }

  private ViewEvent event(String id, int shard) {
    return new ViewEvent(id.repeat(64), 123, "c".repeat(64), shard, Instant.now(), Instant.now());
  }

  private void tick() {
    driver.advanceWallClockTime(Duration.ofSeconds(1));
    driver.advanceWallClockTime(Duration.ofSeconds(1));
  }

  private long lastTotal() {
    var values = totals.readValuesToList();
    return values.getLast().viewCount();
  }
}
