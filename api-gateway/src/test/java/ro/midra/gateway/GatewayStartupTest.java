package ro.midra.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.gateway.route.RouteLocator;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class GatewayStartupTest {
  @Autowired RouteLocator routes;

  @Test
  void applicationStartsWithVideoRoute() {
    assertThat(routes.getRoutes().map(route -> route.getId()).collectList().block())
        .contains("views");
  }
}
