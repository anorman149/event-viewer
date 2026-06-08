package org.eventviewer.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.eventviewer.opensearch.Alias;
import org.eventviewer.opensearch.Id;
import org.eventviewer.opensearch.OsIndex;
import org.eventviewer.opensearch.Template;

import java.time.Instant;
import java.util.List;

@OsIndex(
        indexPattern = "<events-{now/d}-000001>",
        template = @Template(name = "events-template", pattern = "events-*"),
        alias = @Alias(write = "events_write", read = "events_read")
)
public record EventDocument(
        @Id String eventId,
        int schemaType,
        Instant timestamp,
        String s3FileName,
        String podId,
        long batchOffset,
        int batchLength,
        @JsonInclude(JsonInclude.Include.NON_EMPTY) List<String> ruleResults
) {
    public EventDocument {
        ruleResults = ruleResults != null ? ruleResults : List.of();
    }
}
