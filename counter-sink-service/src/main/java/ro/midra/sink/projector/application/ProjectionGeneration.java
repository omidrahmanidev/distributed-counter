package ro.midra.sink.projector.application;

import java.util.Optional;

public interface ProjectionGeneration {
    Optional<String> pending();

    void requested(String generation);
}
