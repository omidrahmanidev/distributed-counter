package ro.midra.sink.projector.application;

public interface SnapshotRequester {
  boolean request(String generation);
}
