package ro.midra.sink.adapter.in;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ro.midra.contracts.JsonSerde;
import ro.midra.contracts.Topics;
import ro.midra.contracts.VideoTotalSnapshot;
import ro.midra.sink.application.PersistVideoTotal;

@Component
@RequiredArgsConstructor
public class SnapshotListener {
  private final PersistVideoTotal persist;
  private final JsonSerde<VideoTotalSnapshot> serde = new JsonSerde<>(VideoTotalSnapshot.class);

  @KafkaListener(topics = Topics.TOTALS, groupId = "${counter.sink-group:video-counter-sink-v1}")
  public void receive(ConsumerRecord<String, byte[]> record) {
    var snapshot = serde.deserializer().deserialize(record.topic(), record.value());
    if (snapshot == null || !Long.toString(snapshot.videoId()).equals(record.key()))
      throw new IllegalArgumentException("Invalid total snapshot key or tombstone");
    persist.persist(snapshot);
  }
}
