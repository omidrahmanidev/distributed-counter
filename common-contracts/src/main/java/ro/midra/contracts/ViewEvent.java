package ro.midra.contracts;

import java.time.Instant;

public record ViewEvent(
    String eventId,
    long videoId,
    String userHash,
    int counterShardId,
    Instant occurredAt,
    Instant acceptedAt) {
  public ViewEvent {
    if (eventId == null
        || !eventId.matches("[0-9a-f]{64}")
        || videoId <= 0
        || counterShardId < 0
        || userHash == null
        || !userHash.matches("[0-9a-f]{64}")
        || occurredAt == null
        || acceptedAt == null) throw new IllegalArgumentException("Invalid view event");
  }
}
