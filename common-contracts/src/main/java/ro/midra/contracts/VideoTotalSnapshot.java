package ro.midra.contracts;

import java.time.Instant;

public record VideoTotalSnapshot(long videoId, long viewCount, long version, Instant emittedAt) {
  public VideoTotalSnapshot {
    if (videoId <= 0 || viewCount < 0 || version != viewCount || emittedAt == null)
      throw new IllegalArgumentException("Invalid snapshot");
  }
}
