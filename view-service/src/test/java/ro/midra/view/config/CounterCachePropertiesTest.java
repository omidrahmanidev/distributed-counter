package ro.midra.view.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

class CounterCachePropertiesTest {
  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withInitializer(new ConfigDataApplicationContextInitializer())
          .withUserConfiguration(PropertiesConfiguration.class)
          .withPropertyValues("HMAC_SECRET=secret", "COUNTER_REDIS_KEY_PREFIX=custom:");

  @Test
  void bindsKeyPrefixFromSharedRedisPrefixEnvironmentVariable() {
    contextRunner.run(
        context ->
            assertThat(context.getBean(CounterCacheProperties.class).keyPrefix())
                .isEqualTo("custom:"));
  }

  @Configuration(proxyBeanMethods = false)
  @EnableConfigurationProperties(CounterCacheProperties.class)
  static class PropertiesConfiguration {}
}
