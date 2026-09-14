package ro.midra.view.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
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
    var cache = new RecordingCache(Mono.just(Optional.of(new StoredCounter(42, 42))), Mono.empty());
    var query = new RecordingQuery(Mono.just(Optional.of(new StoredCounter(999, 999))));

    StepVerifier.create(new GetViewCount(cache, query, validator).get(123))
        .expectNext(42L)
        .verifyComplete();

    assertThat(query.calls).isZero();
    assertThat(cache.repairs).isZero();
  }

  @Test
  void redisMissReadsPostgresAndRepairsCache() {
    var cache = new RecordingCache(Mono.just(Optional.empty()), Mono.empty());
    var query = new RecordingQuery(Mono.just(Optional.of(new StoredCounter(43, 43))));

    StepVerifier.create(new GetViewCount(cache, query, validator).get(123))
        .expectNext(43L)
        .verifyComplete();

    assertThat(query.calls).isEqualTo(1);
    assertThat(cache.repairs).isEqualTo(1);
    assertThat(cache.repairedCounter).isEqualTo(new StoredCounter(43, 43));
  }

  @Test
  void redisFailureFallsBackToPostgres() {
    var cache =
        new RecordingCache(Mono.error(new IllegalStateException("redis down")), Mono.empty());
    var query = new RecordingQuery(Mono.just(Optional.of(new StoredCounter(44, 44))));

    StepVerifier.create(new GetViewCount(cache, query, validator).get(123))
        .expectNext(44L)
        .verifyComplete();

    assertThat(query.calls).isEqualTo(1);
  }

  @Test
  void postgresSuccessStillReturnsWhenRedisRepairFails() {
    var cache =
        new RecordingCache(
            Mono.just(Optional.empty()), Mono.error(new IllegalStateException("redis still down")));
    var query = new RecordingQuery(Mono.just(Optional.of(new StoredCounter(45, 45))));

    StepVerifier.create(new GetViewCount(cache, query, validator).get(123))
        .expectNext(45L)
        .verifyComplete();
  }

  @Test
  void missingPostgresRowReturnsZero() {
    var cache = new RecordingCache(Mono.just(Optional.empty()), Mono.empty());
    var query = new RecordingQuery(Mono.just(Optional.empty()));

    StepVerifier.create(new GetViewCount(cache, query, validator).get(123))
        .expectNext(0L)
        .verifyComplete();

    assertThat(cache.repairs).isZero();
  }

  @Test
  void invalidVideoIdFailsBeforeReading() {
    var cache = new RecordingCache(Mono.just(Optional.of(new StoredCounter(1, 1))), Mono.empty());
    var query = new RecordingQuery(Mono.just(Optional.of(new StoredCounter(1, 1))));
    var useCase = new GetViewCount(cache, query, validator);

    assertThatThrownBy(() -> useCase.get(0)).isInstanceOf(InvalidViewRequestException.class);
    assertThat(cache.reads).isZero();
    assertThat(query.calls).isZero();
  }

  private static final class RecordingCache implements CounterCachePort {
    private final Mono<Optional<StoredCounter>> readResult;
    private final Mono<Void> repairResult;
    private int reads;
    private int repairs;
    private StoredCounter repairedCounter;

    private RecordingCache(Mono<Optional<StoredCounter>> readResult, Mono<Void> repairResult) {
      this.readResult = readResult;
      this.repairResult = repairResult;
    }

    @Override
    public Mono<Optional<StoredCounter>> read(long videoId) {
      reads++;
      return readResult;
    }

    @Override
    public Mono<Void> repair(long videoId, StoredCounter counter) {
      repairs++;
      repairedCounter = counter;
      return repairResult;
    }
  }

  private static final class RecordingQuery implements CounterQueryPort {
    private final Mono<Optional<StoredCounter>> result;
    private int calls;

    private RecordingQuery(Mono<Optional<StoredCounter>> result) {
      this.result = result;
    }

    @Override
    public Mono<Optional<StoredCounter>> find(long videoId) {
      calls++;
      return result;
    }
  }
}
