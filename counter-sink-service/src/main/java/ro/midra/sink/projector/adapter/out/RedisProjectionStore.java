package ro.midra.sink.projector.adapter.out;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Repository;
import ro.midra.sink.projector.application.CounterProjection;
import ro.midra.sink.projector.application.ProjectionStore;
import ro.midra.sink.projector.config.ProjectionProperties;

import java.util.List;

@Repository
public class RedisProjectionStore implements ProjectionStore {
    private final StringRedisTemplate redis;
    private final ProjectionProperties properties;
    private final DefaultRedisScript<Long> update = new DefaultRedisScript<>();

    public RedisProjectionStore(StringRedisTemplate redis, ProjectionProperties properties) {
        this.redis = redis;
        this.properties = properties;
        update.setLocation(new ClassPathResource("redis/update-counter.lua"));
        update.setResultType(Long.class);
    }

    /**
     * Compares decimal versions and writes both fields in one atomic Redis operation.
     */
    @Override
    public boolean apply(CounterProjection projection) {
        Long result =
                redis.execute(
                        update,
                        List.of(properties.getKeyPrefix() + projection.videoId() + ":views"),
                        Long.toString(projection.views()),
                        Long.toString(projection.version()));
        if (result == null) throw new IllegalStateException("Redis projection returned no result");
        return result == 1;
    }
}
