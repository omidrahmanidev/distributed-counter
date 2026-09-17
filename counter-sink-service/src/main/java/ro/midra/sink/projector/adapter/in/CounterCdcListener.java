package ro.midra.sink.projector.adapter.in;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ro.midra.sink.projector.application.ProjectCounter;

@Slf4j
@Component
@RequiredArgsConstructor
public class CounterCdcListener {
    private final CounterCdcMapper mapper;
    private final ProjectCounter projector;

    @KafkaListener(
            id = "counter-cdc",
            topics = "#{@projectionProperties.topic}",
            groupId = "#{@projectionProperties.consumerGroup}",
            containerFactory = "cdcListenerFactory")
    public void receive(ConsumerRecord<String, byte[]> record) {
        log.debug("CDC received partition={} offset={}", record.partition(), record.offset());
        mapper.map(record.value()).ifPresent(projector::apply);
    }
}
