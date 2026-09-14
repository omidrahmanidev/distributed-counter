package ro.midra.view.application;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import ro.midra.contracts.ViewEvent;
import ro.midra.view.domain.CounterShardSelector;
import ro.midra.view.domain.EventIdentity;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class AcceptView {
    private final ViewRequestValidator validator;
    private final EventIdentity identity;
    private final CounterShardSelector shards;
    private final ViewEventPublisher publisher;
    private final MeterRegistry metrics;

    /**
     * Builds a deterministic event identity and shard assignment before publishing to Kafka.
     *
     * <p>The idempotency key is part of the HMAC input, so retries of the same HTTP request produce
     * the same event ID. Shard selection is derived from that event ID, which keeps retries on the
     * same counter shard and lets the stream processor deduplicate without a database call.
     */
    public Mono<String> accept(AcceptViewCommand command) {
        validator.validate(command);
        metrics.counter("view_requests_total").increment();
        String id = identity.eventId(command.email(), command.videoId(), command.idempotencyKey());
        Instant now = Instant.now();
        var event =
                new ViewEvent(
                        id,
                        command.videoId(),
                        identity.userHash(command.email()),
                        shards.select(id),
                        command.occurredAt() == null ? now : command.occurredAt(),
                        now);
        log.atDebug()
                .addKeyValue("videoId", command.videoId())
                .addKeyValue("eventId", id)
                .addKeyValue("counterShardId", event.counterShardId())
                .log("Publishing view");
        return publisher
                .publish(event)
                .onErrorMap(
                        error ->
                                new ViewAcceptanceException(
                                        "Kafka acceptance failed; retry with the same key", error))
                .doOnSuccess(ignored -> metrics.counter("view_events_published_total").increment())
                .doOnError(error -> metrics.counter("view_event_publish_failures_total").increment())
                .thenReturn(id);
    }
}
