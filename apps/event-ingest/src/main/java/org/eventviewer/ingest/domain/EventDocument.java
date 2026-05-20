package org.eventviewer.ingest.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.eventviewer.opensearch.Alias;
import org.eventviewer.opensearch.OsIndex;

import java.time.Instant;
import java.util.List;

@OsIndex(
        indexPattern = "<events-{now/d}-000001>",
        templateName = "events-template",
        alias = @Alias(write = "events_write", read = "events_read")
)
public record EventDocument(
        String eventId,
        String schemaType,
        Instant timestamp,
        String s3FileName,
        String podId,
        long batchOffset,
        int batchLength,
        @JsonInclude(JsonInclude.Include.NON_EMPTY) List<RuleResult> ruleResults
) {
    public EventDocument {
        ruleResults = ruleResults != null ? ruleResults : List.of();
    }
}
