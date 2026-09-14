package ro.midra.gateway.adapter.in;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.Locale;
import java.util.UUID;

@Component
public class IdentityFilter implements GlobalFilter, Ordered {
    private static final String USER_EMAIL_HEADER = "X-User-Email";
    private static final String AUTHENTICATED_EMAIL_HEADER = "X-Authenticated-User-Email";
    private static final String REQUEST_ID_HEADER = "X-Request-Id";
    private static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";
    private static final String EMAIL_PATTERN = "[^\\s@]+@[^\\s@]+\\.[^\\s@]+";
    private static final String REQUEST_TOKEN_PATTERN = "[A-Za-z0-9_-]{1,128}";

    public int getOrder() {
        return -100;
    }

    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        var request = exchange.getRequest();
        boolean viewMutation =
                isViewMutation(request.getMethod(), request.getPath().pathWithinApplication().value());
        String email = request.getHeaders().getFirst(USER_EMAIL_HEADER);
        String idempotencyKey = request.getHeaders().getFirst(IDEMPOTENCY_KEY_HEADER);
        if (viewMutation && (!hasValidEmail(email) || !hasValidRequestToken(idempotencyKey))) {
            return reject(exchange);
        }

        String requestId = requestId(request.getHeaders().getFirst(REQUEST_ID_HEADER));
        exchange.getResponse().getHeaders().set(REQUEST_ID_HEADER, requestId);
        return chain.filter(
                exchange
                        .mutate()
                        .request(
                                request
                                        .mutate()
                                        .headers(
                                                headers -> {
                                                    headers.remove(AUTHENTICATED_EMAIL_HEADER);
                                                    headers.remove(USER_EMAIL_HEADER);
                                                    if (viewMutation)
                                                        headers.set(AUTHENTICATED_EMAIL_HEADER, normalize(email));
                                                    headers.set(REQUEST_ID_HEADER, requestId);
                                                })
                                        .build())
                        .build());
    }

    private boolean isViewMutation(HttpMethod method, String path) {
        return method == HttpMethod.POST && path.matches("/api/videos/[^/]+/views/?");
    }

    private boolean hasValidEmail(String email) {
        return email != null && email.length() <= 254 && email.trim().matches(EMAIL_PATTERN);
    }

    private boolean hasValidRequestToken(String token) {
        return token != null && token.matches(REQUEST_TOKEN_PATTERN);
    }

    private String requestId(String supplied) {
        return hasValidRequestToken(supplied) ? supplied : UUID.randomUUID().toString();
    }

    private String normalize(String email) {
        return email == null ? "" : email.trim().toLowerCase(Locale.ROOT);
    }

    private Mono<Void> reject(ServerWebExchange exchange) {
        exchange.getResponse().setStatusCode(HttpStatus.BAD_REQUEST);
        return exchange.getResponse().setComplete();
    }
}
