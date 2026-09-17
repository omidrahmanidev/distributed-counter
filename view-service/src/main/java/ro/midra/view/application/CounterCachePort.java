package ro.midra.view.application;

import java.util.Optional;
import reactor.core.publisher.Mono;

public interface CounterCachePort {
  Mono<Optional<StoredCounter>> read(long videoId);
}
