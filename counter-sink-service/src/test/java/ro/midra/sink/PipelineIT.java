package ro.midra.sink;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.r2dbc.spi.ConnectionFactories;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import ro.midra.contracts.JsonSerde;
import ro.midra.contracts.Topics;
import ro.midra.contracts.VideoTotalSnapshot;
import ro.midra.contracts.ViewEvent;
import ro.midra.sink.adapter.out.PostgresCounterStore;
import ro.midra.sink.application.PersistVideoTotal;
import ro.midra.sink.cdc.CdcIntegrationSupport;
import ro.midra.sink.projector.adapter.out.RedisProjectionStore;
import ro.midra.sink.projector.application.CounterProjection;
import ro.midra.sink.projector.config.ProjectionProperties;
import ro.midra.stream.config.CounterTopology;
import ro.midra.view.adapter.in.ViewController;
import ro.midra.view.adapter.out.KafkaViewEventPublisher;
import ro.midra.view.adapter.out.PostgresCounterQuery;
import ro.midra.view.adapter.out.RedisCounterCache;
import ro.midra.view.application.AcceptView;
import ro.midra.view.application.GetViewCount;
import ro.midra.view.application.ViewRequestValidator;
import ro.midra.view.domain.CounterShardSelector;
import ro.midra.view.domain.EventIdentity;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class PipelineIT extends CdcIntegrationSupport {
    static final PostgreSQLContainer<?> POSTGRES = ENV.postgres;
    static final GenericContainer<?> REDIS = ENV.redis;
    static final KafkaContainer KAFKA = ENV.kafka;

    static JdbcTemplate jdbc;
    static LettuceConnectionFactory connection;
    static StringRedisTemplate redis;
    static PostgresCounterStore postgresStore;
    static RedisProjectionStore redisStore;

    @BeforeAll
    static void prepare() {
        var source =
                new DriverManagerDataSource(
                        POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        Flyway.configure().dataSource(source).load().migrate();
        jdbc = new JdbcTemplate(source);
        connection = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        connection.afterPropertiesSet();
        connection.start();
        redis = new StringRedisTemplate(connection);
        postgresStore = new PostgresCounterStore(jdbc);
        redisStore = new RedisProjectionStore(redis, new ProjectionProperties());
    }

    @AfterAll
    static void close() {
        if (connection != null) connection.destroy();
    }

    @Test
    void postgresRejectsDuplicateAndOlderVersions() {
        postgresStore.persist(snapshot(1, 1000));
        postgresStore.persist(snapshot(1, 1000));
        postgresStore.persist(snapshot(1, 900));
        assertThat(
                jdbc.queryForObject(
                        "SELECT view_count FROM video_counter WHERE video_id=1", Long.class))
                .isEqualTo(1000);
    }

    @Test
    void redisRejectsEqualAndOlderVersionsIncludingLargeIntegers() {
        long large = 9007199254740993L;
        redisStore.apply(new CounterProjection(2, large, large));
        redisStore.apply(new CounterProjection(2, large - 1, large - 1));
        redisStore.apply(new CounterProjection(2, large, large));
        assertThat(redis.opsForHash().get("video:2:views", "count")).isEqualTo(Long.toString(large));
        redisStore.apply(new CounterProjection(2, large + 1, large + 1));
        assertThat(redis.opsForHash().get("video:2:views", "count"))
                .isEqualTo(Long.toString(large + 1));
    }

    @Test
    void staleDeliveryLeavesAuthoritativeDatabaseUnchanged() {
        postgresStore.persist(snapshot(3, 1000));
        new PersistVideoTotal(postgresStore, new SimpleMeterRegistry()).persist(snapshot(3, 900));
        await()
                .untilAsserted(
                        () -> assertThat(redis.opsForHash().get("video:3:views", "count")).isEqualTo("1000"));
    }

    @Test
    void cacheMissReadsPostgres() {
        postgresStore.persist(snapshot(4, 42));
        var client = WebTestClient.bindToController(new ViewController(null, getViewCount())).build();
        client
                .get()
                .uri("/api/videos/4/views")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .jsonPath("$.views")
                .isEqualTo(42);
    }

    @Test
    void httpKafkaStreamsSinkAndReadModelConverge() throws Exception {
        var metrics = new SimpleMeterRegistry();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "integration-counter");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(
                StreamsConfig.STATE_DIR_CONFIG,
                java.nio.file.Files.createTempDirectory("counter-it").toString());
        var options =
                SenderOptions.<String, ViewEvent>create(
                                Map.of(
                                        "bootstrap.servers",
                                        KAFKA.getBootstrapServers(),
                                        "acks",
                                        "all",
                                        "enable.idempotence",
                                        true))
                        .withKeySerializer(new StringSerializer())
                        .withValueSerializer(new JsonSerde<>(ViewEvent.class).serializer());
        var sender = KafkaSender.create(options);
        try (var streams =
                     new KafkaStreams(
                             CounterTopology.build(Duration.ofDays(7), Duration.ofMillis(200), 128, metrics),
                             props);
             var consumer =
                     new KafkaConsumer<String, byte[]>(
                             Map.of(
                                     "bootstrap.servers",
                                     KAFKA.getBootstrapServers(),
                                     "group.id",
                                     "it-sink",
                                     "auto.offset.reset",
                                     "earliest",
                                     "isolation.level",
                                     "read_committed",
                                     "enable.auto.commit",
                                     false),
                             new StringDeserializer(),
                             new ByteArrayDeserializer())) {
            streams.start();
            consumer.subscribe(List.of(Topics.TOTALS));
            var accept =
                    new AcceptView(
                            validator(),
                            new EventIdentity("integration-secret-with-at-least-32-characters"),
                            new CounterShardSelector(128),
                            new KafkaViewEventPublisher(sender, 100),
                            metrics);
            var client =
                    WebTestClient.bindToController(new ViewController(accept, getViewCount()))
                            .configureClient()
                            .responseTimeout(Duration.ofSeconds(30))
                            .build();
            for (String key : List.of("A", "A", "B"))
                client
                        .post()
                        .uri("/api/videos/123/views")
                        .header("X-Authenticated-User-Email", "user@example.com")
                        .header("Idempotency-Key", key)
                        .exchange()
                        .expectStatus()
                        .isAccepted();
            var sink = new PersistVideoTotal(postgresStore, metrics);
            await()
                    .atMost(Duration.ofSeconds(90))
                    .untilAsserted(
                            () -> {
                                for (var record : consumer.poll(Duration.ofMillis(200)))
                                    sink.persist(
                                            new JsonSerde<>(VideoTotalSnapshot.class)
                                                    .deserializer()
                                                    .deserialize(record.topic(), record.value()));
                                consumer.commitSync();
                                assertThat(redis.opsForHash().get("video:123:views", "count")).isEqualTo("2");
                            });
            client
                    .get()
                    .uri("/api/videos/123/views")
                    .exchange()
                    .expectBody()
                    .jsonPath("$.views")
                    .isEqualTo(2);
            assertThat(
                    jdbc.queryForObject(
                            "SELECT view_count FROM video_counter WHERE video_id=123", Long.class))
                    .isEqualTo(2);
            streams.close();
            props.put(
                    StreamsConfig.STATE_DIR_CONFIG,
                    java.nio.file.Files.createTempDirectory("counter-restored-it").toString());
            try (var restored =
                         new KafkaStreams(
                                 CounterTopology.build(Duration.ofDays(7), Duration.ofMillis(200), 128, metrics),
                                 props)) {
                restored.start();
                for (String key : List.of("A", "C"))
                    client
                            .post()
                            .uri("/api/videos/123/views")
                            .header("X-Authenticated-User-Email", "user@example.com")
                            .header("Idempotency-Key", key)
                            .exchange()
                            .expectStatus()
                            .isAccepted();
                await()
                        .atMost(Duration.ofSeconds(90))
                        .untilAsserted(
                                () -> {
                                    for (var record : consumer.poll(Duration.ofMillis(200)))
                                        sink.persist(
                                                new JsonSerde<>(VideoTotalSnapshot.class)
                                                        .deserializer()
                                                        .deserialize(record.topic(), record.value()));
                                    consumer.commitSync();
                                    assertThat(redis.opsForHash().get("video:123:views", "count")).isEqualTo("3");
                                });
            }
        } finally {
            sender.close();
        }
    }

    private GetViewCount getViewCount() {
        String url =
                "r2dbc:postgresql://"
                        + POSTGRES.getUsername()
                        + ":"
                        + POSTGRES.getPassword()
                        + "@"
                        + POSTGRES.getHost()
                        + ":"
                        + POSTGRES.getMappedPort(5432)
                        + "/"
                        + POSTGRES.getDatabaseName();
        return new GetViewCount(
                new RedisCounterCache(
                        new ReactiveStringRedisTemplate(connection),
                        new ro.midra.view.config.CounterCacheProperties("video:")),
                new PostgresCounterQuery(DatabaseClient.create(ConnectionFactories.get(url))),
                validator());
    }

    private ViewRequestValidator validator() {
        return new ViewRequestValidator(
                Duration.ofMinutes(5), Duration.ofDays(3650), Clock.systemUTC());
    }

    private static VideoTotalSnapshot snapshot(long video, long count) {
        return new VideoTotalSnapshot(video, count, count, Instant.now());
    }
}
