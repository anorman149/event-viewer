package org.eventviewer.ingest.service;

import org.eventviewer.api.ingest.IngestRequest;
import org.eventviewer.api.ingest.IngestResponse;
import org.eventviewer.ingest.kafka.KafkaEventPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class EventIngestServiceTest {

    @Mock
    KafkaEventPublisher kafkaEventPublisher;

    EventIngestService service;

    @BeforeEach
    void setUp() {
        service = new EventIngestService(kafkaEventPublisher);
    }

    @Test
    void ingest_delegatesToPublisher_andReturnsResponse() {
        IngestRequest request = new IngestRequest(UUID.randomUUID(), "order-created", null, Map.of("key", "val"));

        IngestResponse response = service.ingest(request);

        assertThat(response.eventId()).isEqualTo(request.eventId());
        assertThat(response.ingestTs()).isNotNull();
        verify(kafkaEventPublisher).publish(
                eq(request.eventId()), eq("order-created"), any(Instant.class), any(Instant.class), eq(request.payload()));
    }

    @Test
    void ingest_whenTimestampAbsent_defaultsToIngestTime() {
        Instant before = Instant.now();
        IngestRequest request = new IngestRequest(UUID.randomUUID(), "ping", null, null);

        IngestResponse response = service.ingest(request);

        assertThat(response.ingestTs()).isBetween(before, before.plusSeconds(2));
        verify(kafkaEventPublisher).publish(
                any(), any(), any(Instant.class), any(Instant.class), any());
    }
}
