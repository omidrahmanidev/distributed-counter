package ro.midra.sink.projector.adapter.in;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ro.midra.sink.projector.application.CounterProjection;

import java.io.IOException;
import java.util.Optional;

@Component
@RequiredArgsConstructor
public class CounterCdcMapper {
    private final ObjectMapper mapper;

    public Optional<CounterProjection> map(byte[] value) {
        if (value == null) return Optional.empty();
        try {
            JsonNode envelope = mapper.readTree(value);
            if (envelope.has("payload")) envelope = envelope.get("payload");
            String operation = envelope.path("op").asText();
            if (!operation.equals("c") && !operation.equals("u") && !operation.equals("r"))
                throw new IllegalArgumentException("Unsupported counter CDC operation: " + operation);
            JsonNode row = envelope.path("after");
            return Optional.of(
                    new CounterProjection(
                            number(row, "video_id"), number(row, "view_count"), number(row, "version")));
        } catch (IOException error) {
            throw new IllegalArgumentException("Invalid counter CDC JSON", error);
        }
    }

    private long number(JsonNode row, String field) {
        JsonNode value = row.path(field);
        if (!value.isIntegralNumber() || !value.canConvertToLong())
            throw new IllegalArgumentException("Invalid CDC field: " + field);
        return value.longValue();
    }
}
