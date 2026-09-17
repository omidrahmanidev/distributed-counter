package ro.midra.sink.projector.application;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProjectCounter {
    private final ProjectionStore store;
    private final MeterRegistry metrics;

    public void apply(CounterProjection projection) {
        try {
            boolean applied = store.apply(projection);
            metrics.counter(applied ? "counter_cdc_processed" : "counter_cdc_stale").increment();
            log.debug(
                    "CDC videoId={} incomingVersion={} decision={}",
                    projection.videoId(),
                    projection.version(),
                    applied ? "applied" : "equal/stale ignored");
        } catch (RuntimeException error) {
            metrics.counter("counter_cdc_failures").increment();
            throw error;
        }
    }
}
