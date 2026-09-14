package ro.midra.gateway.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GatewayRoutesConfiguration {
  @Bean
  RouteLocator routes(
      RouteLocatorBuilder builder, @Value("${view-service.url:http://localhost:8081}") String uri) {
    return builder.routes().route("views", r -> r.path("/api/videos/**").uri(uri)).build();
  }
}
