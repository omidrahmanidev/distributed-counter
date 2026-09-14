package ro.midra.sink.application;

import ro.midra.contracts.VideoTotalSnapshot;

public interface CounterCachePort {
    void persist(VideoTotalSnapshot snapshot);
}
