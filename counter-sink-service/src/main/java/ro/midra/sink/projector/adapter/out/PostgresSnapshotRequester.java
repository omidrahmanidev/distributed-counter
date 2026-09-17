package ro.midra.sink.projector.adapter.out;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import ro.midra.sink.projector.application.SnapshotRequester;
import ro.midra.sink.projector.config.ProjectionProperties;

import java.util.List;
import java.util.Map;

@Slf4j
@Repository
@RequiredArgsConstructor
public class PostgresSnapshotRequester implements SnapshotRequester {
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final ProjectionProperties properties;

    /**
     * A durable, idempotent request; Debezium reads the rows, never this application.
     */
    @Override
    public boolean request(String generation) {
        try {
            String data =
                    mapper.writeValueAsString(
                            Map.of(
                                    "type",
                                    "incremental",
                                    "data-collections",
                                    List.of(properties.getCounterTable())));
            int inserted =
                    jdbc.update(
                            "INSERT INTO "
                                    + properties.getSignalingTable()
                                    + " (id,type,data) VALUES (?, 'execute-snapshot', ?) ON CONFLICT (id) DO NOTHING",
                            generation,
                            data);
            if (inserted > 0) log.info("Debezium incremental snapshot requested id={}", generation);
            return inserted > 0;
        } catch (JsonProcessingException error) {
            throw new IllegalStateException("Cannot encode snapshot signal", error);
        }
    }
}
