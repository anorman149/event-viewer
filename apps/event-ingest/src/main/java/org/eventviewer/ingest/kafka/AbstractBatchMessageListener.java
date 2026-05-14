package org.eventviewer.ingest.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eventviewer.api.ingest.KafkaEventMessage;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;

import java.util.List;
import java.util.Objects;

public abstract class AbstractBatchMessageListener
        implements BatchAcknowledgingMessageListener<String, String> {

    private final ObjectMapper objectMapper;
    private final Counter parseFailureCounter;

    protected AbstractBatchMessageListener(ObjectMapper objectMapper,
                                            MeterRegistry meterRegistry,
                                            String parseFailureMetricName) {
        this.objectMapper = objectMapper;
        this.parseFailureCounter = Counter.builder(parseFailureMetricName)
                .description("Number of Kafka messages that failed JSON deserialization")
                .register(meterRegistry);
    }

    protected List<KafkaEventMessage> parseRecords(List<ConsumerRecord<String, String>> records) {
        return records.stream()
                .map(record -> {
                    try {
                        return objectMapper.readValue(record.value(), KafkaEventMessage.class);
                    } catch (JsonProcessingException e) {
                        parseFailureCounter.increment();
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .toList();
    }
}
