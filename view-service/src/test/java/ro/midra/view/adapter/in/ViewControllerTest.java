package ro.midra.view.adapter.in;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;
import ro.midra.view.application.AcceptView;
import ro.midra.view.application.AcceptViewCommand;
import ro.midra.view.application.GetViewCount;
import ro.midra.view.application.InvalidViewRequestException;
import ro.midra.view.application.ViewAcceptanceException;

class ViewControllerTest {
  @Test
  void postDelegatesToAcceptViewUseCase() {
    var accept = mock(AcceptView.class);
    var getViewCount = mock(GetViewCount.class);
    var occurredAt = Instant.parse("2026-01-01T00:00:00Z");
    when(accept.accept(any())).thenReturn(Mono.just("event"));

    client(accept, getViewCount)
        .post()
        .uri("/api/videos/123/views")
        .header("X-Authenticated-User-Email", "user@example.com")
        .header("Idempotency-Key", "A")
        .header("X-Occurred-At", occurredAt.toString())
        .exchange()
        .expectStatus()
        .isAccepted()
        .expectBody()
        .jsonPath("$.eventId")
        .isEqualTo("event");

    ArgumentCaptor<AcceptViewCommand> command = ArgumentCaptor.forClass(AcceptViewCommand.class);
    verify(accept).accept(command.capture());
    assertThat(command.getValue())
        .isEqualTo(new AcceptViewCommand(123, "user@example.com", "A", occurredAt));
  }

  @Test
  void getDelegatesToReadUseCase() {
    var accept = mock(AcceptView.class);
    var getViewCount = mock(GetViewCount.class);
    when(getViewCount.get(123)).thenReturn(Mono.just(42L));

    client(accept, getViewCount)
        .get()
        .uri("/api/videos/123/views")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.views")
        .isEqualTo(42);

    verify(getViewCount).get(123);
  }

  @Test
  void validationFailureReturnsBadRequest() {
    var accept = mock(AcceptView.class);
    when(accept.accept(any())).thenReturn(Mono.error(new InvalidViewRequestException("invalid")));

    client(accept, mock(GetViewCount.class))
        .post()
        .uri("/api/videos/123/views")
        .header("X-Authenticated-User-Email", "user@example.com")
        .header("Idempotency-Key", "A")
        .exchange()
        .expectStatus()
        .isBadRequest();
  }

  @Test
  void kafkaFailureReturnsUnavailable() {
    var accept = mock(AcceptView.class);
    when(accept.accept(any()))
        .thenReturn(
            Mono.error(
                new ViewAcceptanceException("Kafka acceptance failed", new RuntimeException())));

    client(accept, mock(GetViewCount.class))
        .post()
        .uri("/api/videos/123/views")
        .header("X-Authenticated-User-Email", "user@example.com")
        .header("Idempotency-Key", "A")
        .exchange()
        .expectStatus()
        .isEqualTo(503);
  }

  private WebTestClient client(AcceptView accept, GetViewCount getViewCount) {
    return WebTestClient.bindToController(new ViewController(accept, getViewCount))
        .controllerAdvice(new ViewExceptionHandler())
        .build();
  }
}
