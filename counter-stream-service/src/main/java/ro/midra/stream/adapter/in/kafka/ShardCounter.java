package ro.midra.stream.adapter.in.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import ro.midra.contracts.CounterShardSnapshot;
import ro.midra.contracts.ViewEvent;

@RequiredArgsConstructor
public final class ShardCounter
    extends ContextualProcessor<String, ViewEvent, String, CounterShardSnapshot> {
  public static final String DEDUP = "view-deduplication",
      COUNTS = "shard-counts",
      DIRTY = "dirty-shards";
  private final Duration retention;
  private final Duration interval;
  private final int batchSize;
  private final MeterRegistry metrics;
  private WindowStore<String, Long> seen;
  private KeyValueStore<String, Long> counts;
  private KeyValueStore<String, Long> dirty;

  @Override
  public void init(ProcessorContext<String, CounterShardSnapshot> context) {
    super.init(context);
    seen = context.getStateStore(DEDUP);
    counts = context.getStateStore(COUNTS);
    dirty = context.getStateStore(DIRTY);
    context.schedule(interval, PunctuationType.WALL_CLOCK_TIME, this::emitDirty);
  }

  /**
   * Counts unique view events into their assigned shard.
   *
   * <p>Deduplication is local to a Kafka Streams window store keyed by the deterministic event ID.
   * The upstream Kafka key is the video/shard identity, so retries of the same event land on the
   * same task and find the same deduplication state. Counts are absolute shard totals; dirty state
   * only records which shard needs to be emitted during the next punctuation.
   */
  @Override
  public void process(Record<String, ViewEvent> record) {
    ViewEvent event = record.value();
    long nowMs = context().currentSystemTimeMs();
    if (isDuplicate(event, nowMs)) {
      metrics.counter("duplicate_view_events_total").increment();
      return;
    }

    markSeen(event, nowMs);
    long updatedCount = incrementShard(record.key());
    markShardDirty(record.key(), updatedCount);
    metrics.counter("unique_view_events_total").increment();
  }

  private boolean isDuplicate(ViewEvent event, long nowMs) {
    try (var matches =
        seen.fetch(
            event.eventId(),
            Instant.ofEpochMilli(Math.max(0, nowMs - retention.toMillis())),
            Instant.ofEpochMilli(nowMs))) {
      return matches.hasNext();
    }
  }

  private void markSeen(ViewEvent event, long nowMs) {
    seen.put(event.eventId(), nowMs, nowMs);
  }

  private long incrementShard(String shardKey) {
    Long previousCount = counts.get(shardKey);
    long updatedCount = Math.addExact(previousCount == null ? 0 : previousCount, 1);
    counts.put(shardKey, updatedCount);
    return updatedCount;
  }

  private void markShardDirty(String shardKey, long updatedCount) {
    dirty.put(shardKey, updatedCount);
  }

  /**
   * Emits a bounded batch of absolute shard snapshots.
   *
   * <p>The dirty store is deliberately separate from the count store. It lets the punctuator scan
   * only shards that changed since their last emission, while the count store remains the source of
   * truth used to build the snapshot. Processed dirty entries are cleared after forwarding so a
   * failed task can replay from Kafka/state stores without losing the latest absolute count.
   */
  private void emitDirty(long timestamp) {
    List<String> emittedShardKeys = new ArrayList<>();
    try (var entries = dirty.all()) {
      while (entries.hasNext() && emittedShardKeys.size() < batchSize) {
        var entry = entries.next();
        forwardSnapshot(entry.key, timestamp);
        emittedShardKeys.add(entry.key);
      }
    }
    emittedShardKeys.forEach(dirty::delete);
    context().commit();
  }

  private void forwardSnapshot(String shardKey, long timestamp) {
    ShardIdentity shardIdentity = ShardIdentity.parse(shardKey);
    long count = counts.get(shardKey);
    context()
        .forward(
            new Record<>(
                Long.toString(shardIdentity.videoId()),
                new CounterShardSnapshot(
                    shardIdentity.videoId(),
                    shardIdentity.shardId(),
                    count,
                    count,
                    Instant.ofEpochMilli(timestamp)),
                timestamp));
    metrics.counter("counter_shard_snapshots_total").increment();
  }

  private record ShardIdentity(long videoId, int shardId) {
    private static ShardIdentity parse(String key) {
      String[] parts = key.split(":");
      return new ShardIdentity(Long.parseLong(parts[0]), Integer.parseInt(parts[1]));
    }
  }
}
