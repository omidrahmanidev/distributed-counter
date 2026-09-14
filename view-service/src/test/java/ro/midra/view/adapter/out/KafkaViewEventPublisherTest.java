package ro.midra.view.adapter.out;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;
import ro.midra.contracts.ViewEvent;

class KafkaViewEventPublisherTest {
  @Test
  void completionWaitsForAcknowledgmentAndOverloadFailsBeforeScheduling() {
    KafkaSender<String, ViewEvent> sender = mock(KafkaSender.class);
    Sinks.One<SenderResult<String>> acknowledgment = Sinks.one();
    when(sender.<String>send(any())).thenReturn(acknowledgment.asMono().flux());
    var publisher = new KafkaViewEventPublisher(sender, 1);
    var completed = new AtomicBoolean();
    SenderResult<String> result = mock(SenderResult.class);

    StepVerifier.create(publisher.publish(event()).doOnSuccess(ignored -> completed.set(true)))
        .then(
            () -> {
              assertThat(completed).isFalse();
              StepVerifier.create(publisher.publish(event()))
                  .expectErrorMatches(error -> error.getMessage().contains("capacity exhausted"))
                  .verify(java.time.Duration.ofSeconds(5));
            })
        .then(() -> acknowledgment.tryEmitValue(result))
        .expectComplete()
        .verify(java.time.Duration.ofSeconds(5));
    assertThat(completed).isTrue();
  }

  @Test
  void brokerFailureIsPropagated() {
    KafkaSender<String, ViewEvent> sender = mock(KafkaSender.class);
    SenderResult<String> failure = mock(SenderResult.class);
    when(failure.exception()).thenReturn(new IllegalStateException("broker failure"));
    when(sender.<String>send(any())).thenReturn(reactor.core.publisher.Flux.just(failure));
    StepVerifier.create(new KafkaViewEventPublisher(sender, 1).publish(event()))
        .expectErrorMessage("broker failure")
        .verify(java.time.Duration.ofSeconds(5));
  }

  private ViewEvent event() {
    return new ViewEvent("a".repeat(64), 123, "b".repeat(64), 0, Instant.now(), Instant.now());
  }
}
