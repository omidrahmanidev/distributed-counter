package ro.midra.contracts;

import java.time.Instant;

public record CounterShardSnapshot(
        long videoId, int counterShardId, long count, long version, Instant emittedAt) {
    public CounterShardSnapshot {
        if (videoId <= 0 || counterShardId < 0 || count < 0 || version != count || emittedAt == null)
            throw new IllegalArgumentException("Invalid snapshot");
    }
}
