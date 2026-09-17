package ro.midra.sink.projector.adapter.in;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ro.midra.sink.projector.application.RebuildProjection;
import ro.midra.sink.projector.config.ProjectionProperties;

@Slf4j
@Component
@RequiredArgsConstructor
public class ProjectionRecoveryScheduler {
    private final RebuildProjection rebuild;
    private final ProjectionProperties properties;
    private boolean unavailable;

    @Scheduled(fixedDelayString = "#{@projectionProperties.recoveryIntervalMs}")
    public void check() {
        if (!properties.isRebuildEnabled()) return;
        try {
            rebuild.check();
            if (unavailable) log.info("Projection recovery dependencies available again");
            unavailable = false;
        } catch (RuntimeException error) {
            if (!unavailable)
                log.warn("Redis or signaling database unavailable; recovery will retry", error);
            unavailable = true;
            log.debug("Recovery check failed", error);
        }
    }
}
