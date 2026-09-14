package ro.midra.view.adapter.out;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import ro.midra.view.application.CounterCachePort;
import ro.midra.view.application.StoredCounter;

import java.util.List;
import java.util.Optional;

@Component
public class RedisCounterCache implements CounterCachePort {
    private static final String COUNTER_KEY_PREFIX = "video:";
    private static final String COUNTER_KEY_SUFFIX = ":views";
    private static final String COUNT_FIELD = "count";

    private final ReactiveStringRedisTemplate redis;
    private final DefaultRedisScript<Long> update = new DefaultRedisScript<>();

    public RedisCounterCache(ReactiveStringRedisTemplate redis) {
        this.redis = redis;
        update.setLocation(new ClassPathResource("redis/update-counter.lua"));
        update.setResultType(Long.class);
    }

    @Override
    public Mono<Optional<StoredCounter>> read(long videoId) {
        return redis
                .<String, String>opsForHash()
                .get(counterKey(videoId), COUNT_FIELD)
                .map(count -> Optional.of(new StoredCounter(Long.parseLong(count), Long.parseLong(count))))
                .defaultIfEmpty(Optional.empty());
    }

    @Override
    public Mono<Void> repair(long videoId, StoredCounter counter) {
        return redis
                .execute(
                        update,
                        List.of(counterKey(videoId)),
                        List.of(Long.toString(counter.count()), Long.toString(counter.version())))
                .then();
    }

    private String counterKey(long videoId) {
        return COUNTER_KEY_PREFIX + videoId + COUNTER_KEY_SUFFIX;
    }
}
