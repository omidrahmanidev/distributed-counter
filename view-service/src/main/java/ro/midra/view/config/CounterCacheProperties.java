package ro.midra.view.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("counter.projection")
public record CounterCacheProperties(@DefaultValue("video:") String keyPrefix) {
  public CounterCacheProperties {
    if (keyPrefix == null || keyPrefix.isBlank())
      throw new IllegalArgumentException("Counter cache key prefix is required");
  }
}
