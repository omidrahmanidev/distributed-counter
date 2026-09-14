package ro.midra.gateway.adapter.in;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;

class IdentityFilterTest {
    @Test
    void replacesSpoofedIdentityAndNormalizesEmail() {
        var exchange =
                MockServerWebExchange.from(
                        MockServerHttpRequest.post("/api/videos/123/views")
                                .header("X-User-Email", " User@Example.COM ")
                                .header("X-Authenticated-User-Email", "attacker@example.com")
                                .header("Idempotency-Key", "A"));
        new IdentityFilter()
                .filter(
                        exchange,
                        forwarded -> {
                            assertThat(forwarded.getRequest().getHeaders().getFirst("X-Authenticated-User-Email"))
                                    .isEqualTo("user@example.com");
                            assertThat(forwarded.getRequest().getHeaders().containsKey("X-User-Email")).isFalse();
                            assertThat(forwarded.getRequest().getHeaders().getFirst("X-Request-Id")).isNotBlank();
                            return Mono.empty();
                        })
                .block();
    }

    @Test
    void malformedEmailIsRejected() {
        var exchange =
                MockServerWebExchange.from(
                        MockServerHttpRequest.post("/api/videos/123/views")
                                .header("X-User-Email", "invalid")
                                .header("Idempotency-Key", "A"));
        new IdentityFilter()
                .filter(
                        exchange,
                        forwarded -> {
                            throw new AssertionError("Must not route");
                        })
                .block();
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void unrelatedPostRouteDoesNotRequireViewIdentityHeaders() {
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.post("/api/admin/reindex"));

        new IdentityFilter()
                .filter(
                        exchange,
                        forwarded -> {
                            assertThat(
                                    forwarded.getRequest().getHeaders().containsKey("X-Authenticated-User-Email"))
                                    .isFalse();
                            assertThat(forwarded.getRequest().getHeaders().getFirst("X-Request-Id")).isNotBlank();
                            return Mono.empty();
                        })
                .block();

        assertThat(exchange.getResponse().getStatusCode()).isNull();
    }

    @Test
    void publicReadsDoNotRequireEmail() {
        var exchange =
                MockServerWebExchange.from(
                        MockServerHttpRequest.get("/api/videos/123/views")
                                .header("X-Authenticated-User-Email", "spoof"));
        new IdentityFilter()
                .filter(
                        exchange,
                        forwarded -> {
                            assertThat(
                                    forwarded.getRequest().getHeaders().containsKey("X-Authenticated-User-Email"))
                                    .isFalse();
                            return Mono.empty();
                        })
                .block();
    }
}
