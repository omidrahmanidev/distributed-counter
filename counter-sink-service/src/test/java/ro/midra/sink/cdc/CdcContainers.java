package ro.midra.sink.cdc;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.flywaydb.core.Flyway;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import ro.midra.contracts.Topics;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class CdcContainers implements AutoCloseable {
    public final Network network = Network.newNetwork();
    public final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:17.6-alpine")
                    .withNetwork(network)
                    .withNetworkAliases("postgres")
                    .withCommand(
                            "postgres",
                            "-c",
                            "wal_level=logical",
                            "-c",
                            "max_replication_slots=10",
                            "-c",
                            "max_wal_senders=10");
    public final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"))
                    .withNetwork(network)
                    .withListener("kafka:19092");
    public final GenericContainer<?> redis =
            new GenericContainer<>("redis:7.4.5-alpine")
                    .withNetwork(network)
                    .withExposedPorts(6379)
                    .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1));
    public final GenericContainer<?> connect =
            new GenericContainer<>("quay.io/debezium/connect:3.0.8.Final")
                    .withNetwork(network)
                    .withExposedPorts(8083)
                    .withEnv("BOOTSTRAP_SERVERS", "kafka:19092")
                    .withEnv("GROUP_ID", "counter-connect")
                    .withEnv("CONFIG_STORAGE_TOPIC", "counter-connect-configs")
                    .withEnv("OFFSET_STORAGE_TOPIC", "counter-connect-offsets")
                    .withEnv("STATUS_STORAGE_TOPIC", "counter-connect-status")
                    .withEnv("CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                    .withEnv("OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                    .withEnv("STATUS_STORAGE_REPLICATION_FACTOR", "1")
                    .withEnv("OFFSET_FLUSH_INTERVAL_MS", "1000")
                    .waitingFor(Wait.forHttp("/connectors").forStatusCode(200))
                    .withStartupTimeout(Duration.ofMinutes(3));
    public DebeziumConnectorTestClient connector;

    public CdcContainers start() {
        return start(jdbc -> {
        });
    }

    public CdcContainers start(
            java.util.function.Consumer<org.springframework.jdbc.core.JdbcTemplate> beforeConnector) {
        try {
            postgres.start();
            kafka.start();
            redis.start();
            Flyway.configure()
                    .dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                    .load()
                    .migrate();
            beforeConnector.accept(
                    new org.springframework.jdbc.core.JdbcTemplate(
                            new org.springframework.jdbc.datasource.DriverManagerDataSource(
                                    postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())));
            try (var admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
                admin
                        .createTopics(
                                List.of(
                                        new NewTopic(Topics.VIEWS, 4, (short) 1),
                                        new NewTopic(Topics.SHARDS, 4, (short) 1),
                                        new NewTopic(Topics.TOTALS, 4, (short) 1),
                                        new NewTopic(Topics.VIEWS + ".DLT", 4, (short) 1),
                                        new NewTopic(Topics.SHARDS + ".DLT", 4, (short) 1)))
                        .all()
                        .get();
            }
            connect.start();
            connector =
                    new DebeziumConnectorTestClient(
                            "http://" + connect.getHost() + ":" + connect.getMappedPort(8083));
            connector.register(
                    postgres.getUsername(), postgres.getPassword(), postgres.getDatabaseName());
            return this;
        } catch (Exception error) {
            close();
            throw new IllegalStateException("Cannot start CDC Testcontainers environment", error);
        }
    }

    public void properties(java.util.function.BiConsumer<String, Object> add) {
        // The existing pipeline test dependency on view-service brings R2DBC onto the test classpath.
        add.accept(
                "spring.autoconfigure.exclude",
                "org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration");
        add.accept("spring.datasource.url", postgres.getJdbcUrl());
        add.accept("spring.datasource.username", postgres.getUsername());
        add.accept("spring.datasource.password", postgres.getPassword());
        add.accept("spring.kafka.bootstrap-servers", kafka.getBootstrapServers());
        add.accept("spring.data.redis.host", redis.getHost());
        add.accept("spring.data.redis.port", redis.getMappedPort(6379));
        add.accept("spring.data.redis.timeout", "500ms");
        add.accept("counter.projection.recovery-interval-ms", 500);
    }

    @Override
    public void close() {
        connect.stop();
        redis.stop();
        kafka.stop();
        postgres.stop();
        network.close();
    }
}
