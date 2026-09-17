package ro.midra.sink.application;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import ro.midra.contracts.VideoTotalSnapshot;

@Service
@RequiredArgsConstructor
public class PersistVideoTotal {
    private final CounterPersistencePort postgres;
    private final MeterRegistry metrics;

    public void persist(VideoTotalSnapshot snapshot) {
        try {
            postgres.persist(snapshot);
            metrics.counter("counter_sink_success_total").increment();
        } catch (DataAccessException error) {
            metrics.counter("counter_sink_failure_total").increment();
            throw error;
        }
    }
}
