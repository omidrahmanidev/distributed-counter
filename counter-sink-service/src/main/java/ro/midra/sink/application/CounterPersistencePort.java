package ro.midra.sink.application;

import ro.midra.contracts.VideoTotalSnapshot;

public interface CounterPersistencePort {
    VideoTotalSnapshot persist(VideoTotalSnapshot snapshot);
}
