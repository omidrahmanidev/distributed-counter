package ro.midra.view.domain;

public record CounterShardSelector(int shardCount) {
    public CounterShardSelector {
        if (shardCount <= 0) throw new IllegalArgumentException("Positive shard count required");
    }

    public int select(String eventId) {
        return (int)
                Long.remainderUnsigned(Long.parseUnsignedLong(eventId.substring(0, 16), 16), shardCount);
    }
}
