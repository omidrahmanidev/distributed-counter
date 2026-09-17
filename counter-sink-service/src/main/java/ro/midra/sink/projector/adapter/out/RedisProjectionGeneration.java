package ro.midra.sink.projector.adapter.out;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Repository;
import ro.midra.sink.projector.application.ProjectionGeneration;
import ro.midra.sink.projector.config.ProjectionProperties;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class RedisProjectionGeneration implements ProjectionGeneration {
    private final StringRedisTemplate redis;
    private final ProjectionProperties properties;
    private static final DefaultRedisScript<Long> MARK_REQUESTED =
            new DefaultRedisScript<>(
                    """
                            if redis.call('GET', KEYS[1]) == ARGV[1] then
                              redis.call('SET', KEYS[1], 'requested:' .. ARGV[1])
                              return 1
                            end
                            return 0
                            """,
                    Long.class);

    /**
     * SET NX elects a generation; PostgreSQL's signal primary key coordinates its request across
     * instances. Pending generations survive process crashes. A flush creates a new generation;
     * compare-and-set prevents a request for the previous generation from hiding that loss.
     */
    @Override
    public Optional<String> pending() {
        String key = properties.getMarkerKey();
        String value = redis.opsForValue().get(key);
        if (value == null) {
            redis.opsForValue().setIfAbsent(key, UUID.randomUUID().toString());
            value = redis.opsForValue().get(key);
        }
        return value == null || value.startsWith("requested:") ? Optional.empty() : Optional.of(value);
    }

    @Override
    public void requested(String generation) {
        redis.execute(MARK_REQUESTED, List.of(properties.getMarkerKey()), generation);
    }
}
