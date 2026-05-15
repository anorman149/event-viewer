package org.eventviewer.opensearch;

@OsIndex(
        indexPattern  = "<test-events-{now/d}-000001>",
        templateName  = "test-events-template",
        alias = @Alias(write = "test_events_write", read = "test_events_read")
)
public record TestDocument(String id) {}
