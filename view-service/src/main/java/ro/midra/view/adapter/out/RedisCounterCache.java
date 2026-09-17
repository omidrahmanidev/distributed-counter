package ro.midra.view.adapter.out;

import java.util.Optional;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import ro.midra.view.application.CounterCachePort;
import ro.midra.view.application.StoredCounter;
import ro.midra.view.config.CounterCacheProperties;

@Component
@EnableConfigurationProperties(CounterCacheProperties.class)
public class RedisCounterCache implements CounterCachePort {
    private static final String COUNTER_KEY_SUFFIX = ":views";
    private static final String COUNT_FIELD = "count";

    private final ReactiveStringRedisTemplate redis;
    private final CounterCacheProperties properties;

    public RedisCounterCache(ReactiveStringRedisTemplate redis, CounterCacheProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    @Override
    public Mono<Optional<StoredCounter>> read(long videoId) {
        return redis
                .<String, String>opsForHash()
                .get(counterKey(videoId), COUNT_FIELD)
                .map(count -> Optional.of(new StoredCounter(Long.parseLong(count), Long.parseLong(count))))
                .defaultIfEmpty(Optional.empty());
    }

    private String counterKey(long videoId) {
        return properties.keyPrefix() + videoId + COUNTER_KEY_SUFFIX;
    }
}
