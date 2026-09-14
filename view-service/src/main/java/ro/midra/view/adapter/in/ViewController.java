package ro.midra.view.adapter.in;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import ro.midra.view.application.AcceptView;
import ro.midra.view.application.AcceptViewCommand;
import ro.midra.view.application.GetViewCount;

import java.time.Instant;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/videos/{videoId}/views")
public class ViewController {
    private final AcceptView accept;
    private final GetViewCount getViewCount;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<Accepted> post(
            @PathVariable long videoId,
            @RequestHeader("X-Authenticated-User-Email") String email,
            @RequestHeader("Idempotency-Key") String key,
            @RequestHeader(value = "X-Request-Id", defaultValue = "internal") String requestId,
            @RequestHeader(value = "X-Occurred-At", required = false) Instant occurredAt) {
        return accept
                .accept(new AcceptViewCommand(videoId, email, key, occurredAt))
                .doOnNext(
                        id ->
                                log.atDebug()
                                        .addKeyValue("requestId", requestId)
                                        .addKeyValue("videoId", videoId)
                                        .addKeyValue("eventId", id)
                                        .log("View accepted by Kafka"))
                .map(id -> new Accepted(id, "ACCEPTED"));
    }

    @GetMapping
    public Mono<Count> get(@PathVariable long videoId) {
        return getViewCount.get(videoId).map(count -> new Count(videoId, count));
    }

    public record Accepted(String eventId, String status) {
    }

    public record Count(long videoId, long views) {
    }
}
