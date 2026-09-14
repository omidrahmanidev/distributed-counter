package ro.midra.sink.adapter.out;

import java.util.List;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Repository;
import ro.midra.contracts.VideoTotalSnapshot;
import ro.midra.sink.application.CounterCachePort;

@Repository
public class RedisCounterStore implements CounterCachePort {
  private static final String COUNTER_KEY_PREFIX = "video:";
  private static final String COUNTER_KEY_SUFFIX = ":views";

  private final StringRedisTemplate redis;
  private final DefaultRedisScript<Long> update = new DefaultRedisScript<>();

  public RedisCounterStore(StringRedisTemplate redis) {
    this.redis = redis;
    update.setLocation(new ClassPathResource("redis/update-counter.lua"));
    update.setResultType(Long.class);
  }

  /**
   * Updates the Redis read model through a Lua script so count/version comparison is atomic.
   *
   * <p>KEYS[1] is the per-video hash key. ARGV[1] is the absolute count and ARGV[2] is its version.
   * The script writes only when the incoming version is newer than the version already in Redis,
   * protecting the cache from out-of-order Kafka delivery.
   */
  @Override
  public void persist(VideoTotalSnapshot snapshot) {
    redis.execute(
        update,
        List.of(counterKey(snapshot.videoId())),
        Long.toString(snapshot.viewCount()),
        Long.toString(snapshot.version()));
  }

  private String counterKey(long videoId) {
    return COUNTER_KEY_PREFIX + videoId + COUNTER_KEY_SUFFIX;
  }
}
