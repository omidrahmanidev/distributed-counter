package ro.midra.view.domain;

import static org.assertj.core.api.Assertions.*;

import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class EventIdentityTest {
  private final EventIdentity identity =
      new EventIdentity("test-only-secret-with-at-least-32-characters");

  @Test
  void retryHasIdenticalIdentity() {
    assertThat(identity.eventId("user@example.com", 123, "A"))
        .isEqualTo(identity.eventId("user@example.com", 123, "A"));
  }

  @Test
  void legitimateRepeatedViewsHaveDifferentIdentities() {
    assertThat(identity.eventId("user@example.com", 123, "A"))
        .isNotEqualTo(identity.eventId("user@example.com", 123, "B"));
  }

  @Test
  void identitySeparatesUsersAndVideos() {
    assertThat(identity.eventId("a@example.com", 123, "A"))
        .isNotEqualTo(identity.eventId("b@example.com", 123, "A"))
        .isNotEqualTo(identity.eventId("a@example.com", 124, "A"));
  }

  @Test
  void shardSelectionIsStableAndDistributed() {
    var selector = new CounterShardSelector(128);
    String event = identity.eventId("user@example.com", 123, "A");
    assertThat(selector.select(event)).isEqualTo(selector.select(event)).isBetween(0, 127);
    assertThat(
            IntStream.range(0, 10000)
                .map(
                    i ->
                        selector.select(
                            identity.eventId("user@example.com", 123, Integer.toString(i))))
                .distinct()
                .count())
        .isEqualTo(128);
  }

  @Test
  void weakSecretIsRejected() {
    assertThatIllegalArgumentException().isThrownBy(() -> new EventIdentity("weak"));
  }
}
