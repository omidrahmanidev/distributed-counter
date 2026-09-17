package ro.midra.sink.adapter.out;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import ro.midra.contracts.VideoTotalSnapshot;
import ro.midra.sink.application.CounterPersistencePort;

@Repository
@RequiredArgsConstructor
public class PostgresCounterStore implements CounterPersistencePort {
    private static final String UPSERT_TOTAL_SNAPSHOT =
            """
                    INSERT INTO video_counter(video_id,view_count,version,updated_at) VALUES (?,?,?,NOW())
                    ON CONFLICT(video_id) DO UPDATE SET view_count=EXCLUDED.view_count, version=EXCLUDED.version, updated_at=NOW()
                    WHERE video_counter.version < EXCLUDED.version
                    """;

    private final JdbcTemplate jdbc;

    /**
     * Atomically applies a snapshot when its version is newer than the stored version.
     */
    @Override
    public void persist(VideoTotalSnapshot snapshot) {
        jdbc.update(
                UPSERT_TOTAL_SNAPSHOT, snapshot.videoId(), snapshot.viewCount(), snapshot.version());
    }
}
