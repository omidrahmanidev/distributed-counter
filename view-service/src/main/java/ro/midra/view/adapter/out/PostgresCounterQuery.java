package ro.midra.view.adapter.out;

import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import ro.midra.view.application.CounterQueryPort;
import ro.midra.view.application.StoredCounter;

import java.util.Optional;

@Component
public class PostgresCounterQuery implements CounterQueryPort {
    private final DatabaseClient database;

    public PostgresCounterQuery(DatabaseClient database) {
        this.database = database;
    }

    @Override
    public Mono<Optional<StoredCounter>> find(long videoId) {
        return database
                .sql("SELECT view_count, version FROM video_counter WHERE video_id = :id")
                .bind("id", videoId)
                .map(
                        (row, metadata) ->
                                new StoredCounter(
                                        row.get("view_count", Long.class), row.get("version", Long.class)))
                .one()
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty());
    }
}
