package ro.midra.view.application;

import reactor.core.publisher.Mono;

import java.util.Optional;

public interface CounterQueryPort {
    Mono<Optional<StoredCounter>> find(long videoId);
}
