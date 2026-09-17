package ro.midra.sink.cdc;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import ro.midra.sink.Application;

@SpringBootTest(classes = Application.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
public abstract class CdcIntegrationSupport {
    protected static final CdcContainers ENV =
            new CdcContainers()
                    .start(
                            jdbc ->
                                    new ro.midra.sink.application.PersistVideoTotal(
                                            new ro.midra.sink.adapter.out.PostgresCounterStore(jdbc),
                                            new io.micrometer.core.instrument.simple.SimpleMeterRegistry())
                                            .persist(
                                                    new ro.midra.contracts.VideoTotalSnapshot(
                                                            90001, 42, 42, java.time.Instant.now())));

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        ENV.properties((key, value) -> registry.add(key, () -> value));
    }
}
