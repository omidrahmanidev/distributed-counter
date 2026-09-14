package ro.midra.view.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("counter.producer")
public record ProducerSettings(
        @DefaultValue("33554432") long bufferBytes,
        @DefaultValue("5") int lingerMs,
        @DefaultValue("65536") int batchBytes,
        @DefaultValue("1000") int maxBlockMs,
        @DefaultValue("30000") int deliveryTimeoutMs,
        @DefaultValue("10000") int requestTimeoutMs,
        @DefaultValue("4096") int capacity,
        @DefaultValue("1024") int maxInFlight) {
    public ProducerSettings {
        if (bufferBytes <= 0
                || lingerMs < 0
                || batchBytes <= 0
                || maxBlockMs <= 0
                || capacity <= 0
                || maxInFlight <= 0
                || requestTimeoutMs <= 0
                || deliveryTimeoutMs < requestTimeoutMs + lingerMs)
            throw new IllegalArgumentException("Invalid producer capacity or timeout configuration");
    }
}
