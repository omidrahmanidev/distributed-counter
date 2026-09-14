package ro.midra.view.application;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;

@Component
public class ViewRequestValidator {
    private static final String EMAIL_PATTERN = "[^\\s@]+@[^\\s@]+\\.[^\\s@]+";
    private static final String IDEMPOTENCY_KEY_PATTERN = "[A-Za-z0-9_-]{1,128}";

    private final Duration futureSkew;
    private final Duration maximumAge;
    private final Clock clock;

    public ViewRequestValidator(
            @Value("${counter.timestamps.future-skew:PT5M}") Duration futureSkew,
            @Value("${counter.timestamps.maximum-age:P3650D}") Duration maximumAge,
            Clock clock) {
        this.futureSkew = futureSkew;
        this.maximumAge = maximumAge;
        this.clock = clock;
    }

    /**
     * Applies the request policies that protect deterministic event identity and timestamp sanity.
     *
     * <p>The controller supplies raw HTTP values, but the application layer owns the decision about
     * whether a view can enter the ingestion pipeline. Time is read from an injected clock so
     * boundary cases can be tested deterministically.
     */
    public void validate(AcceptViewCommand command) {
        validateVideo(command.videoId());
        validateIdentity(command.email(), command.idempotencyKey());
        validateOccurrenceTime(command.occurredAt());
    }

    public void validateVideo(long videoId) {
        if (videoId <= 0) throw new InvalidViewRequestException("Positive video ID required");
    }

    private void validateIdentity(String email, String idempotencyKey) {
        if (email == null
                || email.length() > 254
                || !email.matches(EMAIL_PATTERN)
                || !email.equals(email.trim().toLowerCase(Locale.ROOT))
                || idempotencyKey == null
                || !idempotencyKey.matches(IDEMPOTENCY_KEY_PATTERN))
            throw new InvalidViewRequestException("Invalid identity or idempotency key");
    }

    private void validateOccurrenceTime(Instant occurredAt) {
        if (occurredAt == null) return;
        Instant now = Instant.now(clock);
        if (occurredAt.isAfter(now.plus(futureSkew)) || occurredAt.isBefore(now.minus(maximumAge)))
            throw new InvalidViewRequestException("Unreasonable occurrence time");
    }
}
