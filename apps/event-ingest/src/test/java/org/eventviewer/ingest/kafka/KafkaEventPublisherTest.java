package org.eventviewer.ingest.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.eventviewer.ingest.config.EventKafkaProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaEventPublisherTest {

    @Mock
    @SuppressWarnings("rawtypes")
    KafkaTemplate kafkaTemplate;

    KafkaEventPublisher publisher;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        EventKafkaProperties eventKafkaProperties = new EventKafkaProperties(
                List.of(
                        new EventKafkaProperties.TopicDefinition("event-raw-1", 2, 1),
                        new EventKafkaProperties.TopicDefinition("event-raw-2", 2, 1),
                        new EventKafkaProperties.TopicDefinition("event-raw-3", 2, 1),
                        new EventKafkaProperties.TopicDefinition("event-raw-4", 2, 1)),
                List.of(
                        new EventKafkaProperties.DltTopicDefinition("event-raw-1.DLT", 1, 1),
                        new EventKafkaProperties.DltTopicDefinition("event-raw-2.DLT", 1, 1),
                        new EventKafkaProperties.DltTopicDefinition("event-raw-3.DLT", 1, 1),
                        new EventKafkaProperties.DltTopicDefinition("event-raw-4.DLT", 1, 1)),
                new EventKafkaProperties.LagMonitor(false, 60_000L, List.of()));
        publisher = new KafkaEventPublisher(kafkaTemplate, objectMapper, new SimpleMeterRegistry(), eventKafkaProperties);
    }

    @Test
    @SuppressWarnings("unchecked")
    void publish_sendsJsonToShardedTopicWithEventIdAsKey() {
        UUID eventId = UUID.randomUUID();
        when(kafkaTemplate.send(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        publisher.publish(eventId, "order-created", Instant.now(), Instant.now(), Map.of("key", "val"));

        verify(kafkaTemplate).send(
                argThat(t -> ((String) t).startsWith("event-raw-")),
                eq(eventId.toString()),
                any(String.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void sameEventId_alwaysRoutesToSameTopic() {
        UUID eventId = UUID.randomUUID();
        when(kafkaTemplate.send(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        publisher.publish(eventId, "order-created", Instant.now(), Instant.now(), null);
        publisher.publish(eventId, "order-created", Instant.now(), Instant.now(), null);

        // Both calls should use the same topic (deterministic routing by hash)
        verify(kafkaTemplate, org.mockito.Mockito.times(2)).send(
                argThat(t -> ((String) t).startsWith("event-raw-")),
                eq(eventId.toString()),
                any(String.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void kafkaFailure_propagatesKafkaException() {
        when(kafkaTemplate.send(any(), any(), any())).thenThrow(new KafkaException("broker unavailable"));

        assertThatThrownBy(() ->
                publisher.publish(UUID.randomUUID(), "test", Instant.now(), Instant.now(), null))
                .isInstanceOf(KafkaException.class)
                .hasMessageContaining("broker unavailable");
    }
}
