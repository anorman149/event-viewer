package org.eventviewer.opensearch.itest;

import org.eventviewer.opensearch.Alias;
import org.eventviewer.opensearch.OsIndex;

@OsIndex(
        indexPattern  = "it-test-events-000001",
        templateName  = "it-test-events-template",
        alias = @Alias(write = "it_test_events_write", read = "it_test_events_read")
)
public record TestEventDocument(String id, String type, String payload) {}
