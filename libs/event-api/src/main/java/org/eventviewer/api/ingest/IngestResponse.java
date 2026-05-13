package org.eventviewer.api.ingest;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

public record IngestResponse(
        @JsonProperty("event_id") UUID eventId,
        @JsonProperty("ingest_ts") Instant ingestTs
) {}
