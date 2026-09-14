package ro.midra.view.application;

import reactor.core.publisher.Mono;
import ro.midra.contracts.ViewEvent;

public interface ViewEventPublisher {
    Mono<Void> publish(ViewEvent event);
}
