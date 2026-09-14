package ro.midra.contracts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public final class JsonSerde<T> implements Serde<T> {
    private final ObjectMapper mapper = JsonMapper.builder().addModule(new JavaTimeModule()).build();
    private final Class<T> type;

    public JsonSerde(Class<T> type) {
        this.type = type;
    }

    public Serializer<T> serializer() {
        return (topic, value) -> {
            try {
                return value == null ? null : mapper.writeValueAsBytes(value);
            } catch (java.io.IOException e) {
                throw new SerializationException("Cannot encode contract", e);
            }
        };
    }

    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            try {
                return bytes == null ? null : mapper.readValue(bytes, type);
            } catch (java.io.IOException e) {
                throw new SerializationException("Invalid contract", e);
            }
        };
    }
}
