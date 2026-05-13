package org.eventviewer.api.ingest;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.eventviewer.api.validation.ValidUUID;

import java.time.Instant;

public record IngestRequest(
        @NotNull @ValidUUID @JsonProperty("event_id") String eventId,
        @NotBlank @JsonProperty("schema_type") String schemaType,
        Instant timestamp,
        Object payload
) {}
