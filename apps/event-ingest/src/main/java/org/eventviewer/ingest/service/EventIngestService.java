package org.eventviewer.ingest.service;

import io.micrometer.core.annotation.Timed;
import org.eventviewer.api.ingest.IngestRequest;
import org.eventviewer.api.ingest.IngestResponse;
import org.eventviewer.ingest.kafka.KafkaEventPublisher;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class EventIngestService {

    private final KafkaEventPublisher kafkaEventPublisher;

    public EventIngestService(KafkaEventPublisher kafkaEventPublisher) {
        this.kafkaEventPublisher = kafkaEventPublisher;
    }

    @Timed(value = "event.ingest.service.ingest", description = "Time to process an event through the ingest service layer", histogram = true)
    public IngestResponse ingest(IngestRequest request) {
        Instant ingestTs = Instant.now();
        Instant timestamp = request.timestamp() != null ? request.timestamp() : ingestTs;

        kafkaEventPublisher.publish(request.eventId(), request.schemaType(), timestamp, ingestTs, request.payload());

        return new IngestResponse(request.eventId(), ingestTs);
    }
}
