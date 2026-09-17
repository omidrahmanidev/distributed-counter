package ro.midra.sink.projector.application;

public interface ProjectionStore {
    boolean apply(CounterProjection projection);
}
