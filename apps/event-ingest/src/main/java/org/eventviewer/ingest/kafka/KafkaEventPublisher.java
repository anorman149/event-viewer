package org.eventviewer.ingest.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.eventviewer.api.ingest.KafkaEventMessage;
import org.eventviewer.ingest.config.EventKafkaProperties;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Component
public class KafkaEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final DistributionSummary messageSizeSummary;
    private final List<String> topicNames;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                               ObjectMapper objectMapper,
                               MeterRegistry meterRegistry,
                               EventKafkaProperties eventKafkaProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topicNames = eventKafkaProperties.topics().stream()
                .map(EventKafkaProperties.TopicDefinition::name)
                .toList();
        this.messageSizeSummary = DistributionSummary.builder("kafka.event.message.bytes")
                .description("Size in bytes of the serialized Kafka event message after JSON serialization")
                .baseUnit("bytes")
                .publishPercentileHistogram()
                .register(meterRegistry);
    }

    @Timed(value = "kafka.event.publish", description = "Time to serialize and publish an event to Kafka", histogram = true)
    public void publish(UUID eventId, String schemaType, Instant timestamp, Instant ingestTs, Object payload) {
        KafkaEventMessage message = new KafkaEventMessage(eventId, schemaType, timestamp, ingestTs, payload);
        try {
            String json = objectMapper.writeValueAsString(message);
            messageSizeSummary.record(json.getBytes(StandardCharsets.UTF_8).length);
            String topic = selectTopic(eventId);
            kafkaTemplate.send(topic, eventId.toString(), json);
        } catch (JsonProcessingException e) {
            throw new KafkaException("Failed to serialize event for publishing", e);
        }
    }

    private String selectTopic(UUID eventId) {
        int index = Math.floorMod(eventId.hashCode(), topicNames.size());
        return topicNames.get(index);
    }
}
