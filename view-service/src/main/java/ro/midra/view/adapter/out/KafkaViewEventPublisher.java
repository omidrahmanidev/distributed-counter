package ro.midra.view.adapter.out;

import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import ro.midra.contracts.Topics;
import ro.midra.contracts.ViewEvent;
import ro.midra.view.application.ViewEventPublisher;

import java.util.concurrent.atomic.AtomicInteger;

public final class KafkaViewEventPublisher implements ViewEventPublisher {
    private final KafkaSender<String, ViewEvent> sender;
    private final AtomicInteger pending = new AtomicInteger();
    private final int capacity;

    public KafkaViewEventPublisher(KafkaSender<String, ViewEvent> sender, int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("Positive publisher capacity required");
        this.sender = sender;
        this.capacity = capacity;
    }

    public Mono<Void> publish(ViewEvent event) {
        return Mono.defer(
                () -> {
                    // Admit before scheduling so overload cannot accumulate in the scheduler queue.
                    if (pending.incrementAndGet() > capacity) {
                        pending.decrementAndGet();
                        return Mono.error(new IllegalStateException("Publisher capacity exhausted"));
                    }
                    return send(event)
                            .subscribeOn(Schedulers.boundedElastic())
                            .doFinally(signal -> pending.decrementAndGet());
                });
    }

    private Mono<Void> send(ViewEvent event) {
        return Mono.defer(
                () -> {
                    var record =
                            new ProducerRecord<>(
                                    Topics.VIEWS, event.videoId() + ":" + event.counterShardId(), event);
                    return sender
                            .send(Mono.just(SenderRecord.create(record, event.eventId())))
                            .single()
                            .flatMap(
                                    result ->
                                            result.exception() == null
                                                    ? Mono.<Void>empty()
                                                    : Mono.error(result.exception()));
                });
    }
}
