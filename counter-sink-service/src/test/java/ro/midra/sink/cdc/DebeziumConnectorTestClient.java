package ro.midra.sink.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.awaitility.Awaitility.await;

@Slf4j
public class DebeziumConnectorTestClient {
    public static final String NAME = "counter-snapshot-connector";
    private final String base;
    private final HttpClient http =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ObjectMapper mapper = new ObjectMapper();

    public DebeziumConnectorTestClient(String base) {
        this.base = base;
    }

    public void register(String user, String password, String database) throws Exception {
        ObjectNode config;
        try (var stream = getClass().getResourceAsStream("/debezium/counter-connector.json")) {
            config = (ObjectNode) mapper.readTree(stream);
        }
        config.put("database.user", user);
        config.put("database.password", password);
        config.put("database.dbname", database);
        config.put("incremental.snapshot.chunk.size", "16");
        config.put("offset.flush.interval.ms", "1000");
        var response =
                http.send(
                        HttpRequest.newBuilder(URI.create(base + "/connectors/" + NAME + "/config"))
                                .timeout(Duration.ofSeconds(30))
                                .header("Content-Type", "application/json")
                                .PUT(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(config)))
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2)
            throw new IllegalStateException(
                    "Connector registration HTTP " + response.statusCode() + ": " + response.body());
        log.info("Registered Debezium connector {}", NAME);
        awaitRunning();
    }

    public JsonNode status() throws Exception {
        var response =
                http.send(
                        HttpRequest.newBuilder(URI.create(base + "/connectors/" + NAME + "/status"))
                                .timeout(Duration.ofSeconds(5))
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200)
            throw new IllegalStateException("Connector status: " + response.body());
        return mapper.readTree(response.body());
    }

    public void restart() throws Exception {
        var response =
                http.send(
                        HttpRequest.newBuilder(
                                        URI.create(
                                                base
                                                        + "/connectors/"
                                                        + NAME
                                                        + "/restart?includeTasks=true&onlyFailed=false"))
                                .timeout(Duration.ofSeconds(30))
                                .POST(HttpRequest.BodyPublishers.noBody())
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2)
            throw new IllegalStateException(
                    "Connector restart HTTP " + response.statusCode() + ": " + response.body());
        awaitRunning();
    }

    public void awaitRunning() {
        await()
                .atMost(Duration.ofSeconds(90))
                .pollInterval(Duration.ofSeconds(1))
                .ignoreExceptions()
                .untilAsserted(
                        () -> {
                            var state = status();
                            org.assertj.core.api.Assertions.assertThat(
                                            state.path("connector").path("state").asText())
                                    .as("Connector status: %s", state)
                                    .isEqualTo("RUNNING");
                            org.assertj.core.api.Assertions.assertThat(state.path("tasks").size()).isEqualTo(1);
                            org.assertj.core.api.Assertions.assertThat(
                                            state.path("tasks").get(0).path("state").asText())
                                    .as("Task status: %s", state)
                                    .isEqualTo("RUNNING");
                        });
    }
}
