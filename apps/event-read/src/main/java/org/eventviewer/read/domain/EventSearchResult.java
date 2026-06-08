package org.eventviewer.read.domain;

import java.time.Instant;

public record EventSearchResult(
        String eventId,
        int schemaType,
        Instant timestamp
) {}
