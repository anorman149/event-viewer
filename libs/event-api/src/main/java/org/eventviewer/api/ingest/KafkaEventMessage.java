package org.eventviewer.api.ingest;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

public record KafkaEventMessage(
        @JsonProperty("event_id") UUID eventId,
        @JsonProperty("schema_type") String schemaType,
        Instant timestamp,
        @JsonProperty("ingest_ts") Instant ingestTs,
        Object payload
) {}
