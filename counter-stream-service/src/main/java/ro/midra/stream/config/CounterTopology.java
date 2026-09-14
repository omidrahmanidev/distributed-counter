package ro.midra.stream.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import ro.midra.contracts.*;
import ro.midra.stream.adapter.in.kafka.ShardCounter;
import ro.midra.stream.adapter.in.kafka.VideoAggregator;

import java.time.Duration;
import java.util.function.BiPredicate;

/**
 * Builds the Kafka Streams topology for the counter pipeline.
 *
 * <p>The topology has two stateful stages. The first stage consumes view events keyed by {@code
 * videoId:shardId}, deduplicates deterministic event IDs, and emits absolute shard snapshots. The
 * second stage consumes those shard snapshots keyed by video ID and aggregates them into absolute
 * total snapshots. Invalid or corrupted records are forwarded to the source topic's DLT without
 * changing the original topic names or valid-record keys.
 */
public final class CounterTopology {
    private CounterTopology() {
    }

    public static Topology build(
            Duration retention, Duration interval, int shardCount, MeterRegistry metrics) {
        return build(retention, interval, shardCount, 10000, metrics);
    }

    public static Topology build(
            Duration retention, Duration interval, int shardCount, int batchSize, MeterRegistry metrics) {
        validateSettings(retention, interval, shardCount, batchSize);

        var builder = new StreamsBuilder();
        addStateStores(builder, retention);
        buildShardCounter(builder, retention, interval, shardCount, batchSize, metrics);
        buildVideoAggregator(builder, interval, shardCount, batchSize, metrics);
        return builder.build();
    }

    private static void validateSettings(
            Duration retention, Duration interval, int shardCount, int batchSize) {
        if (batchSize <= 0
                || retention.isNegative()
                || retention.isZero()
                || interval.isNegative()
                || interval.isZero()
                || shardCount <= 0)
            throw new IllegalArgumentException("Positive stream configuration required");
    }

    private static void addStateStores(StreamsBuilder builder, Duration retention) {
        builder.addStateStore(
                Stores.windowStoreBuilder(
                        Stores.persistentWindowStore(ShardCounter.DEDUP, retention, retention, false),
                        Serdes.String(),
                        Serdes.Long()));
        for (String name :
                new String[]{
                        ShardCounter.COUNTS,
                        ShardCounter.DIRTY,
                        VideoAggregator.LATEST,
                        VideoAggregator.TOTALS,
                        VideoAggregator.DIRTY
                })
            builder.addStateStore(
                    Stores.keyValueStoreBuilder(
                            Stores.persistentKeyValueStore(name), Serdes.String(), Serdes.Long()));
    }

    private static void buildShardCounter(
            StreamsBuilder builder,
            Duration retention,
            Duration interval,
            int shardCount,
            int batchSize,
            MeterRegistry metrics) {
        decoded(
                builder,
                Topics.VIEWS,
                ViewEvent.class,
                (key, event) ->
                        event.counterShardId() < shardCount
                                && (event.videoId() + ":" + event.counterShardId()).equals(key))
                .process(
                        () -> new ShardCounter(retention, interval, batchSize, metrics),
                        Named.as("count-shards"),
                        ShardCounter.DEDUP,
                        ShardCounter.COUNTS,
                        ShardCounter.DIRTY)
                .to(
                        Topics.SHARDS,
                        Produced.with(Serdes.String(), new JsonSerde<>(CounterShardSnapshot.class)));
    }

    private static void buildVideoAggregator(
            StreamsBuilder builder,
            Duration interval,
            int shardCount,
            int batchSize,
            MeterRegistry metrics) {
        decoded(
                builder,
                Topics.SHARDS,
                CounterShardSnapshot.class,
                (key, event) ->
                        event.counterShardId() < shardCount && Long.toString(event.videoId()).equals(key))
                .process(
                        () -> new VideoAggregator(interval, batchSize, metrics),
                        Named.as("aggregate-videos"),
                        VideoAggregator.LATEST,
                        VideoAggregator.TOTALS,
                        VideoAggregator.DIRTY)
                .to(
                        Topics.TOTALS,
                        Produced.with(Serdes.String(), new JsonSerde<>(VideoTotalSnapshot.class)));
    }

    /**
     * Deserializes byte-valued input explicitly so bad payloads can be sent to a dead-letter topic.
     *
     * <p>Kafka Streams' normal value serde path would fail the task before the topology could route
     * the record. Keeping the original bytes in the wrapper lets the DLT receive exactly what failed.
     */
    private static <T> KStream<String, T> decoded(
            StreamsBuilder builder, String topic, Class<T> type, BiPredicate<String, T> valid) {
        var deserializer = new JsonSerde<>(type).deserializer();
        KStream<String, Decoded<T>> decoded =
                builder.stream(topic, Consumed.with(Serdes.String(), Serdes.ByteArray()))
                        .mapValues(
                                (key, bytes) -> {
                                    try {
                                        T value = deserializer.deserialize(topic, bytes);
                                        return new Decoded<>(bytes, value, value != null && valid.test(key, value));
                                    } catch (SerializationException | IllegalArgumentException error) {
                                        return new Decoded<T>(bytes, null, false);
                                    }
                                });
        decoded
                .filter((key, value) -> !value.valid())
                .mapValues(Decoded::bytes)
                .to(topic + ".DLT", Produced.with(Serdes.String(), Serdes.ByteArray()));
        return decoded.filter((key, value) -> value.valid()).mapValues(Decoded::value);
    }

    private record Decoded<T>(byte[] bytes, T value, boolean valid) {
    }
}
