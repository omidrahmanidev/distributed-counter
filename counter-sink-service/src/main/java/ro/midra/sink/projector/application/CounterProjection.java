package ro.midra.sink.projector.application;

public record CounterProjection(long videoId, long views, long version) {
    public CounterProjection {
        if (videoId <= 0 || views < 0 || version < 0)
            throw new IllegalArgumentException("Invalid counter projection");
    }
}
