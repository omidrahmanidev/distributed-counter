package ro.midra.contracts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Instant;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

class JsonSerdeTest {
  @Test
  void allContractsRoundTripWithNanosecondTimestamps() {
    Instant time = Instant.parse("2026-01-01T00:00:00.123456789Z");
    roundTrip(new ViewEvent("a".repeat(64), 123, "b".repeat(64), 17, time, time), ViewEvent.class);
    roundTrip(new CounterShardSnapshot(123, 17, 99, 99, time), CounterShardSnapshot.class);
    roundTrip(new VideoTotalSnapshot(123, 99, 99, time), VideoTotalSnapshot.class);
  }

  @Test
  void rejectsInconsistentSnapshotVersion() {
    byte[] bytes =
        "{\"videoId\":123,\"viewCount\":100,\"version\":99,\"emittedAt\":\"2026-01-01T00:00:00Z\"}"
            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    assertThatExceptionOfType(SerializationException.class)
        .isThrownBy(
            () ->
                new JsonSerde<>(VideoTotalSnapshot.class)
                    .deserializer()
                    .deserialize(Topics.TOTALS, bytes));
  }

  private <T> void roundTrip(T value, Class<T> type) {
    var serde = new JsonSerde<>(type);
    assertThat(
            serde.deserializer().deserialize("test", serde.serializer().serialize("test", value)))
        .isEqualTo(value);
  }
}
