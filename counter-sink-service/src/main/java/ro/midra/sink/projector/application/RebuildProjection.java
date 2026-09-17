package ro.midra.sink.projector.application;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class RebuildProjection {
    private final ProjectionGeneration generations;
    private final SnapshotRequester snapshots;
    private final MeterRegistry metrics;

    public void check() {
        generations
                .pending()
                .ifPresent(
                        generation -> {
                            if (snapshots.request(generation)) {
                                metrics.counter("counter_cdc_rebuilds").increment();
                                log.info("Redis projection rebuild requested generation={}", generation);
                            }
                            generations.requested(generation);
                        });
    }
}
