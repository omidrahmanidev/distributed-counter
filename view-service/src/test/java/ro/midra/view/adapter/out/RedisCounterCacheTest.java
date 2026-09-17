package ro.midra.view.adapter.out;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import ro.midra.view.application.StoredCounter;
import ro.midra.view.config.CounterCacheProperties;

class RedisCounterCacheTest {
  @Test
  void readsCounterUsingConfiguredKeyPrefix() {
    ReactiveStringRedisTemplate redis = mock(ReactiveStringRedisTemplate.class);
    ReactiveHashOperations<String, String, String> hash = mock();
    when(redis.<String, String>opsForHash()).thenReturn(hash);
    when(hash.get("custom:123:views", "count")).thenReturn(Mono.just("42"));

    var cache = new RedisCounterCache(redis, new CounterCacheProperties("custom:"));

    StepVerifier.create(cache.read(123))
        .assertNext(counter -> assertThat(counter).isEqualTo(Optional.of(new StoredCounter(42, 42))))
        .verifyComplete();
    verify(hash).get("custom:123:views", "count");
  }
}
