package ro.midra.view.application;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class ViewRequestValidatorTest {
  private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
  private final ViewRequestValidator validator =
      new ViewRequestValidator(
          Duration.ofMinutes(5), Duration.ofDays(3650), Clock.fixed(NOW, ZoneOffset.UTC));

  @Test
  void acceptsValidRequest() {
    assertThatCode(
            () ->
                validator.validate(
                    new AcceptViewCommand(123, "user@example.com", "retry_1", NOW.minusSeconds(1))))
        .doesNotThrowAnyException();
  }

  @Test
  void rejectsInvalidVideoId() {
    assertInvalid(new AcceptViewCommand(0, "user@example.com", "A", NOW));
  }

  @Test
  void rejectsMissingBlankAndMalformedEmail() {
    assertInvalid(new AcceptViewCommand(123, null, "A", NOW));
    assertInvalid(new AcceptViewCommand(123, " ", "A", NOW));
    assertInvalid(new AcceptViewCommand(123, "invalid", "A", NOW));
  }

  @Test
  void rejectsNonCanonicalEmail() {
    assertInvalid(new AcceptViewCommand(123, "User@Example.COM", "A", NOW));
    assertInvalid(new AcceptViewCommand(123, " user@example.com ", "A", NOW));
  }

  @Test
  void rejectsMissingBlankAndMalformedIdempotencyKey() {
    assertInvalid(new AcceptViewCommand(123, "user@example.com", null, NOW));
    assertInvalid(new AcceptViewCommand(123, "user@example.com", "", NOW));
    assertInvalid(new AcceptViewCommand(123, "user@example.com", "bad key", NOW));
  }

  @Test
  void rejectsTooOldOccurrenceTime() {
    assertInvalid(
        new AcceptViewCommand(123, "user@example.com", "A", NOW.minus(Duration.ofDays(3651))));
  }

  @Test
  void rejectsTooFarInFutureOccurrenceTime() {
    assertInvalid(
        new AcceptViewCommand(123, "user@example.com", "A", NOW.plus(Duration.ofMinutes(6))));
  }

  private void assertInvalid(AcceptViewCommand command) {
    assertThatThrownBy(() -> validator.validate(command))
        .isInstanceOf(InvalidViewRequestException.class);
  }
}
