package org.eventviewer.ingest.domain;

import java.time.Instant;
import java.util.UUID;

public record EventCoordinates(
        UUID eventId,
        Instant timestamp,
        String s3FileName,
        String podId,
        long batchOffset,
        int batchLength
) {}
