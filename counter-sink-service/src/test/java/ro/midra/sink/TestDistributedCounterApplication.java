package ro.midra.sink;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;
import ro.midra.sink.cdc.CdcContainers;

public class TestDistributedCounterApplication {
    public static void main(String[] args) {
        SpringApplication.from(Application::main).with(DevelopmentInfrastructure.class).run(args);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class DevelopmentInfrastructure {
        @Bean(destroyMethod = "close")
        CdcContainers cdcContainers() {
            return new CdcContainers().start();
        }

        @Bean
        DynamicPropertyRegistrar infrastructureProperties(CdcContainers containers) {
            return registry -> containers.properties((key, value) -> registry.add(key, () -> value));
        }
    }
}
