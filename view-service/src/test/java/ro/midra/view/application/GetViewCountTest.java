package ro.midra.view.application;

import static org.assertj.core.api.Assertions.*;

import java.time.*;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class GetViewCountTest {
  private final ViewRequestValidator validator =
      new ViewRequestValidator(
          Duration.ofMinutes(5),
          Duration.ofDays(3650),
          Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC));

  @Test
  void redisHitDoesNotReadPostgres() {
    var calls = new AtomicInteger();
    var useCase =
        new GetViewCount(
            id -> Mono.just(Optional.of(new StoredCounter(42, 42))),
            id -> {
              calls.incrementAndGet();
              return Mono.just(Optional.empty());
            },
            validator);
    StepVerifier.create(useCase.get(123)).expectNext(42L).verifyComplete();
    assertThat(calls).hasValue(0);
  }

  @Test
  void redisMissReadsPostgresWithoutCacheWrites() {
    var useCase =
        new GetViewCount(
            id -> Mono.just(Optional.empty()),
            id -> Mono.just(Optional.of(new StoredCounter(43, 43))),
            validator);
    StepVerifier.create(useCase.get(123)).expectNext(43L).verifyComplete();
  }

  @Test
  void redisFailureFallsBackToPostgres() {
    var useCase =
        new GetViewCount(
            id -> Mono.error(new IllegalStateException("redis down")),
            id -> Mono.just(Optional.of(new StoredCounter(44, 44))),
            validator);
    StepVerifier.create(useCase.get(123)).expectNext(44L).verifyComplete();
  }

  @Test
  void missingPostgresRowReturnsZero() {
    var useCase =
        new GetViewCount(
            id -> Mono.just(Optional.empty()), id -> Mono.just(Optional.empty()), validator);
    StepVerifier.create(useCase.get(123)).expectNext(0L).verifyComplete();
  }

  @Test
  void invalidVideoIdFailsBeforeReading() {
    var calls = new AtomicInteger();
    var useCase =
        new GetViewCount(
            id -> {
              calls.incrementAndGet();
              return Mono.just(Optional.empty());
            },
            id -> {
              calls.incrementAndGet();
              return Mono.just(Optional.empty());
            },
            validator);
    assertThatThrownBy(() -> useCase.get(0)).isInstanceOf(InvalidViewRequestException.class);
    assertThat(calls).hasValue(0);
  }
}
