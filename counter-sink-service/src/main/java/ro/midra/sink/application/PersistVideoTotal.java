package ro.midra.sink.application;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ro.midra.contracts.VideoTotalSnapshot;

@Service
@RequiredArgsConstructor
public class PersistVideoTotal {
  private final CounterPersistencePort postgres;
  private final CounterCachePort redis;
  private final MeterRegistry metrics;

  /**
   * Persists a total snapshot through PostgreSQL first, then repairs Redis from the authoritative
   * row returned by PostgreSQL.
   *
   * <p>Both stores reject stale versions independently. Reading back from PostgreSQL after the
   * UPSERT lets an old Kafka redelivery refill an empty cache without rolling the counter back.
   */
  public void persist(VideoTotalSnapshot snapshot) {
    try {
      redis.persist(postgres.persist(snapshot));
      metrics.counter("counter_sink_success_total").increment();
    } catch (org.springframework.dao.DataAccessException error) {
      metrics.counter("counter_sink_failure_total").increment();
      throw error;
    }
  }
}
