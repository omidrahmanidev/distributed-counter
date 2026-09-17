package ro.midra.sink.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import ro.midra.sink.projector.adapter.in.CounterCdcMapper;
import ro.midra.sink.projector.application.CounterProjection;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CounterCdcMapperTest {
    private final CounterCdcMapper mapper = new CounterCdcMapper(new ObjectMapper());

    @ParameterizedTest
    @ValueSource(strings = {"c", "u", "r"})
    void mapsStreamingAndSnapshotEnvelopesIdentically(String operation) {
        String json =
                "{\"op\":\""
                        + operation
                        + "\",\"after\":{\"video_id\":1,\"view_count\":100,\"version\":9007199254740993}}";
        var expected = new CounterProjection(1, 100, 9007199254740993L);
        assertThat(mapper.map(bytes(json))).contains(expected);
        assertThat(mapper.map(bytes("{\"schema\":{},\"payload\":" + json + "}"))).contains(expected);
    }

    @Test
    void ignoresKafkaTombstone() {
        assertThat(mapper.map(null)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                    "{}",
                    "{\"op\":\"d\"}",
                    "{\"op\":\"r\",\"after\":{}}",
                    "{",
                    "{\"op\":\"u\",\"after\":{\"video_id\":1,\"view_count\":1,\"version\":1.5}}"
            })
    void rejectsInvalidOrUnsupportedChanges(String json) {
        assertThatThrownBy(() -> mapper.map(bytes(json))).isInstanceOf(IllegalArgumentException.class);
    }

    private byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
