package ro.midra.view.application;

import reactor.core.publisher.Mono;

import java.util.Optional;

public interface CounterCachePort {
    Mono<Optional<StoredCounter>> read(long videoId);

    Mono<Void> repair(long videoId, StoredCounter counter);
}
