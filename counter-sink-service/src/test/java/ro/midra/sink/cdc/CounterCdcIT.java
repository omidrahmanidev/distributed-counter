package ro.midra.sink.cdc;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import ro.midra.contracts.VideoTotalSnapshot;
import ro.midra.sink.application.PersistVideoTotal;
import ro.midra.sink.projector.adapter.out.RedisProjectionStore;
import ro.midra.sink.projector.application.CounterProjection;
import ro.midra.sink.projector.application.RebuildProjection;
import ro.midra.sink.projector.config.ProjectionProperties;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CounterCdcIT extends CdcIntegrationSupport {
    @Autowired
    PersistVideoTotal sink;
    @Autowired
    JdbcTemplate jdbc;
    @Autowired
    StringRedisTemplate redis;
    @Autowired
    KafkaListenerEndpointRegistry listeners;
    @Autowired
    KafkaTemplate<String, byte[]> kafka;
    @Autowired
    RedisProjectionStore store;
    @Autowired
    ProjectionProperties properties;
    @Autowired
    RebuildProjection rebuild;
    @Autowired
    io.micrometer.core.instrument.MeterRegistry metrics;

    @Test
    @Order(1)
    void postgresSnapshotReachesRedisOnlyThroughCdcAndLogicalReplicationIsActive() {
        eventually(90001, 42);
        assertThat(jdbc.queryForObject("SHOW wal_level", String.class)).isEqualTo("logical");
        await()
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(
                        () ->
                                assertThat(
                                        jdbc.queryForObject(
                                                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'counter_snapshot_cdc' AND plugin = 'pgoutput'",
                                                Integer.class))
                                        .isEqualTo(1));
        var listener = listeners.getListenerContainer("counter-cdc");
        listener.stop();
        try {
            persist(10001, 100);
            assertThat(
                    jdbc.queryForObject(
                            "SELECT view_count FROM video_counter WHERE video_id=10001", Long.class))
                    .isEqualTo(100);
            assertThat(redis.hasKey("video:10001:views")).isFalse();
        } finally {
            listener.start();
        }
        eventually(10001, 100);
    }

    @Test
    @Order(2)
    void normalUpdatePropagates() {
        persist(10002, 100);
        eventually(10002, 100);
        persist(10002, 150);
        eventually(10002, 150);
    }

    @Test
    @Order(3)
    void duplicateAndOlderCdcEventsAreHarmless() throws Exception {
        send(10003, 10);
        send(10003, 10);
        send(10003, 9);
        // Same partition barrier proves all prior records were processed before checking stale state.
        send(10004, 1);
        eventually(10004, 1);
        assertCounter(10003, 10);
    }

    @Test
    @Order(4)
    void atomicRedisOperationRejectsConcurrentOlderWritesAndPreservesLongPrecision()
            throws Exception {
        long base = 9007199254740993L;
        try (var executor = Executors.newFixedThreadPool(8)) {
            List<Callable<Boolean>> writes = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                long version = base + i;
                writes.add(() -> store.apply(new CounterProjection(10005, version, version)));
            }
            Collections.shuffle(writes);
            for (var future : executor.invokeAll(writes)) future.get();
        }
        assertCounter(10005, base + 199);
        assertThat(store.apply(new CounterProjection(10005, 1, 1))).isFalse();
        assertCounter(10005, base + 199);
    }

    @Test
    @Order(5)
    void completeRedisLossRebuildsUnchangedPostgresRowsThroughIncrementalSnapshot() {
        for (long id = 11000; id < 11020; id++) persist(id, id);
        for (long id = 11000; id < 11020; id++) eventually(id, id);
        long signalsBefore = signals();
        try (var connection = redis.getConnectionFactory().getConnection()) {
            connection.serverCommands().flushAll();
        }
        await()
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(signals()).isEqualTo(signalsBefore + 1));
        for (long id = 11000; id < 11020; id++) eventually(id, id);
        for (int i = 0; i < 5; i++) rebuild.check();
        assertThat(signals()).isEqualTo(signalsBefore + 1);
    }

    @Test
    @Order(6)
    void newerUpdateWinsWhileIncrementalRecoveryIsRunning() {
        // Many small chunks make the snapshot window observable without sleeping.
        for (long id = 12000; id < 14000; id++) persist(id, 1);
        eventually(13999, 1);
        long windows = windows();
        try (var connection = redis.getConnectionFactory().getConnection()) {
            connection.serverCommands().flushAll();
        }
        await()
                .pollInterval(Duration.ofMillis(10))
                .atMost(Duration.ofSeconds(60))
                .until(() -> windows() > windows);
        assertThat(redis.hasKey("video:13999:views"))
                .as("Recovery must still be in progress")
                .isFalse();
        persist(13999, 999);
        eventually(13999, 999);
        await()
                .atMost(Duration.ofSeconds(120))
                .untilAsserted(
                        () -> {
                            for (long id = 12000; id < 13999; id++) assertCounter(id, 1);
                        });
        assertCounter(13999, 999);
    }

    @Test
    @Order(7)
    void connectorRestartResumesWithExistingSlotAndOffsets() throws Exception {
        persist(14001, 10);
        eventually(14001, 10);
        String slotLsn =
                jdbc.queryForObject(
                        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='counter_snapshot_cdc'",
                        String.class);
        long requests = signals();
        ENV.connector.restart();
        persist(14001, 20);
        eventually(14001, 20);
        assertThat(signals()).isEqualTo(requests);
        await()
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(
                        () ->
                                assertThat(
                                        jdbc.queryForObject(
                                                "SELECT confirmed_flush_lsn >= ?::pg_lsn FROM pg_replication_slots WHERE slot_name='counter_snapshot_cdc'",
                                                Boolean.class,
                                                slotLsn))
                                        .isTrue());
    }

    @Test
    @Order(8)
    void redisUnavailableDuringCdcDeliveryRetriesWithoutLosingUpdates() {
        persist(14002, 1);
        eventually(14002, 1);
        double failures = metrics.counter("counter_cdc_failures").count();
        ENV.redis.getDockerClient().pauseContainerCmd(ENV.redis.getContainerId()).exec();
        try {
            persist(14002, 2);
            await()
                    .atMost(Duration.ofSeconds(30))
                    .untilAsserted(
                            () ->
                                    assertThat(metrics.counter("counter_cdc_failures").count())
                                            .isGreaterThan(failures));
        } finally {
            ENV.redis.getDockerClient().unpauseContainerCmd(ENV.redis.getContainerId()).exec();
        }
        eventually(14002, 2);
    }

    private void persist(long id, long count) {
        sink.persist(new VideoTotalSnapshot(id, count, count, Instant.now()));
    }

    private void send(long id, long version) throws Exception {
        String json =
                "{\"op\":\"u\",\"after\":{\"video_id\":"
                        + id
                        + ",\"view_count\":"
                        + version
                        + ",\"version\":"
                        + version
                        + "}}";
        kafka
                .send(
                        properties.getTopic(),
                        0,
                        Long.toString(id),
                        json.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .get(10, TimeUnit.SECONDS);
    }

    private void eventually(long id, long count) {
        await().atMost(Duration.ofSeconds(90)).untilAsserted(() -> assertCounter(id, count));
    }

    private void assertCounter(long id, long count) {
        assertThat(redis.opsForHash().entries("video:" + id + ":views"))
                .containsEntry("count", Long.toString(count))
                .containsEntry("version", Long.toString(count));
    }

    private long signals() {
        return jdbc.queryForObject(
                "SELECT count(*) FROM debezium_signal WHERE type='execute-snapshot'", Long.class);
    }

    private long windows() {
        return jdbc.queryForObject(
                "SELECT count(*) FROM debezium_signal WHERE type='snapshot-window-open'", Long.class);
    }
}
