package org.eventviewer.ingest.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.eventviewer.api.ingest.KafkaEventMessage;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

@Component
public class KafkaEventPublisher {

    static final String TOPIC = "event-raw";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final DistributionSummary messageSizeSummary;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                               ObjectMapper objectMapper,
                               MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.messageSizeSummary = DistributionSummary.builder("kafka.event.message.bytes")
                .description("Size in bytes of the serialized Kafka event message after JSON serialization")
                .baseUnit("bytes")
                .publishPercentileHistogram()
                .tag("topic", TOPIC)
                .register(meterRegistry);
    }

    @Timed(value = "kafka.event.publish", description = "Time to serialize and publish an event to Kafka", histogram = true)
    public void publish(UUID eventId, String schemaType, Instant timestamp, Instant ingestTs, Object payload) {
        KafkaEventMessage message = new KafkaEventMessage(eventId, schemaType, timestamp, ingestTs, payload);
        try {
            String json = objectMapper.writeValueAsString(message);
            messageSizeSummary.record(json.getBytes(StandardCharsets.UTF_8).length);
            kafkaTemplate.send(TOPIC, eventId.toString(), json);
        } catch (JsonProcessingException e) {
            throw new KafkaException("Failed to serialize event for publishing", e);
        }
    }
}
