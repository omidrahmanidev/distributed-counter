package ro.midra.sink.projector.config;

import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

@Configuration
@EnableScheduling
public class ProjectionConfiguration {
    @Bean
    @ConfigurationProperties("counter.projection")
    ProjectionProperties projectionProperties() {
        return new ProjectionProperties();
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> cdcListenerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory,
            ProjectionProperties properties) {
        var factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        var retry =
                new DefaultErrorHandler(
                        new FixedBackOff(properties.getRetryIntervalMs(), FixedBackOff.UNLIMITED_ATTEMPTS));
        // Fail closed even for malformed events: never skip CDC silently or acknowledge lost state.
        retry.setClassifications(Map.of(), true);
        factory.setCommonErrorHandler(retry);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }
}
