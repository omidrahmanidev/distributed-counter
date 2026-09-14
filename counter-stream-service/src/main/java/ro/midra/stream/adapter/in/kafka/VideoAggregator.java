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
import ro.midra.contracts.CounterShardSnapshot;
import ro.midra.contracts.VideoTotalSnapshot;

@RequiredArgsConstructor
public final class VideoAggregator
    extends ContextualProcessor<String, CounterShardSnapshot, String, VideoTotalSnapshot> {
  public static final String LATEST = "latest-shards",
      TOTALS = "video-totals",
      DIRTY = "dirty-videos";
  private final Duration interval;
  private final int batchSize;
  private final MeterRegistry metrics;
  private KeyValueStore<String, Long> latest;
  private KeyValueStore<String, Long> totals;
  private KeyValueStore<String, Long> dirty;

  @Override
  public void init(ProcessorContext<String, VideoTotalSnapshot> context) {
    super.init(context);
    latest = context.getStateStore(LATEST);
    totals = context.getStateStore(TOTALS);
    dirty = context.getStateStore(DIRTY);
    context.schedule(interval, PunctuationType.WALL_CLOCK_TIME, this::emitDirty);
  }

  /**
   * Aggregates absolute shard snapshots into a total view count for a video.
   *
   * <p>Shard snapshots carry absolute counts, not deltas. The snapshot contract currently requires
   * {@code version == count}, so this processor stores the latest accepted shard count/version and
   * applies only the difference between the incoming and previous values to the video total.
   *
   * <p>The Kafka record key is the video ID, ensuring all shard snapshots for the same video are
   * processed by the same Kafka Streams task.
   */
  @Override
  public void process(Record<String, CounterShardSnapshot> record) {
    CounterShardSnapshot snapshot = record.value();
    String shardKey = shardKey(snapshot);
    Long previousShardCount = latest.get(shardKey);
    if (isStale(snapshot, previousShardCount)) {
      metrics.counter("stale_shard_snapshots_total").increment();
      return;
    }

    long delta = snapshot.count() - previousCount(previousShardCount);
    long updatedTotal = updateVideoTotal(record.key(), delta);
    rememberLatestShardState(shardKey, snapshot);
    markVideoDirty(record.key(), updatedTotal);
  }

  private String shardKey(CounterShardSnapshot snapshot) {
    return snapshot.videoId() + ":" + snapshot.counterShardId();
  }

  private boolean isStale(CounterShardSnapshot snapshot, Long previousShardCount) {
    return previousShardCount != null && snapshot.version() <= previousShardCount;
  }

  private long previousCount(Long previousShardCount) {
    return previousShardCount == null ? 0 : previousShardCount;
  }

  private long updateVideoTotal(String videoKey, long delta) {
    Long previousTotal = totals.get(videoKey);
    long updatedTotal = Math.addExact(previousTotal == null ? 0 : previousTotal, delta);
    totals.put(videoKey, updatedTotal);
    return updatedTotal;
  }

  private void rememberLatestShardState(String shardKey, CounterShardSnapshot snapshot) {
    latest.put(shardKey, snapshot.version());
  }

  private void markVideoDirty(String videoKey, long updatedTotal) {
    dirty.put(videoKey, updatedTotal);
  }

  /**
   * Emits a bounded batch of absolute video total snapshots.
   *
   * <p>Totals are emitted from the state store instead of from the dirty value itself so the
   * forwarded snapshot always reflects the authoritative aggregate held by this task. The Kafka key
   * remains the video ID to preserve downstream partitioning and idempotency behavior.
   */
  private void emitDirty(long timestamp) {
    List<String> emittedVideoKeys = new ArrayList<>();
    try (var entries = dirty.all()) {
      while (entries.hasNext() && emittedVideoKeys.size() < batchSize) {
        var entry = entries.next();
        forwardSnapshot(entry.key, timestamp);
        emittedVideoKeys.add(entry.key);
      }
    }
    emittedVideoKeys.forEach(dirty::delete);
    context().commit();
  }

  private void forwardSnapshot(String videoKey, long timestamp) {
    long viewCount = totals.get(videoKey);
    context()
        .forward(
            new Record<>(
                videoKey,
                new VideoTotalSnapshot(
                    Long.parseLong(videoKey),
                    viewCount,
                    viewCount,
                    Instant.ofEpochMilli(timestamp)),
                timestamp));
    metrics.counter("video_total_snapshots_total").increment();
  }
}
