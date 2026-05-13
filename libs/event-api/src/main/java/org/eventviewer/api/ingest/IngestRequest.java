package org.eventviewer.api.ingest;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.UUID;

public record IngestRequest(
        @NotNull @JsonProperty("event_id") UUID eventId,
        @NotBlank @JsonProperty("schema_type") String schemaType,
        Instant timestamp,
        Object payload
) {}
