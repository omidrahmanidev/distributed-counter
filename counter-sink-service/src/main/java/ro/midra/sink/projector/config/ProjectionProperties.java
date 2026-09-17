package ro.midra.sink.projector.config;

import jakarta.validation.constraints.*;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties("counter.projection")
public class ProjectionProperties {
  @NotBlank private String topic = "counter-cdc.public.video_counter";
  @NotBlank private String consumerGroup = "counter-cache-projector-v1";
  @NotBlank private String keyPrefix = "video:";
  @NotBlank private String markerKey = "counter-projection:generation";

  @Pattern(regexp = "[a-z_][a-z0-9_]*\\.[a-z_][a-z0-9_]*")
  private String signalingTable = "public.debezium_signal";

  @Pattern(regexp = "[a-z_][a-z0-9_]*\\.[a-z_][a-z0-9_]*")
  private String counterTable = "public.video_counter";

  private boolean rebuildEnabled = true;

  @Min(100)
  private long recoveryIntervalMs = 5000;

  @Min(100)
  @Max(30000)
  private long retryIntervalMs = 2000;
}
