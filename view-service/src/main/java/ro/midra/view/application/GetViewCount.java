package ro.midra.view.application;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class GetViewCount {
    private final CounterCachePort cache;
    private final CounterQueryPort query;
    private final ViewRequestValidator validator;

    /**
     * Reads the view total using Redis as a cache and PostgreSQL as the durable read model.
     *
     * <p>Redis misses and failures fall back to PostgreSQL without writing the cache. Only the CDC
     * projector populates Redis.
     */
    public Mono<Long> get(long videoId) {
        validator.validateVideo(videoId);
        return readCache(videoId)
                .flatMap(
                        cached ->
                                cached
                                        .map(counter -> Mono.just(counter.count()))
                                        .orElseGet(() -> readDurable(videoId)));
    }

    private Mono<Optional<StoredCounter>> readCache(long videoId) {
        return cache
                .read(videoId)
                .onErrorResume(
                        error -> {
                            log.atDebug()
                                    .addKeyValue("videoId", videoId)
                                    .log("Cache read unavailable; using PostgreSQL");
                            return Mono.just(Optional.empty());
                        });
    }

    private Mono<Long> readDurable(long videoId) {
        return query.find(videoId).map(counter -> counter.map(StoredCounter::count).orElse(0L));
    }
}
