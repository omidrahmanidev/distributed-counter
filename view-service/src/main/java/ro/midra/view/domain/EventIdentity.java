package ro.midra.view.domain;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

public final class EventIdentity {
    private final byte[] secret;

    public EventIdentity(String secret) {
        if (secret == null || secret.length() < 32)
            throw new IllegalArgumentException("HMAC secret must have at least 32 characters");
        this.secret = secret.getBytes(StandardCharsets.UTF_8);
    }

    public String userHash(String email) {
        return hash("user:" + email);
    }

    public String eventId(String email, long videoId, String key) {
        return hash("event:" + email.length() + ":" + email + ":" + videoId + ":" + key);
    }

    private String hash(String value) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(value.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException("HMAC unavailable", e);
        }
    }
}
