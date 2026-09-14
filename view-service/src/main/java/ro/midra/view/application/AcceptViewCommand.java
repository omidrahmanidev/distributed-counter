package ro.midra.view.application;

import java.time.Instant;

public record AcceptViewCommand(
        long videoId, String email, String idempotencyKey, Instant occurredAt) {
}
