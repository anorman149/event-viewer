package org.eventviewer.opensearch;

@OsIndex(
        indexPattern  = "<test-events-{now/d}-000001>",
        template = @Template(name = "test-events-template", pattern = "test-events-*"),
        alias = @Alias(write = "test_events_write", read = "test_events_read")
)
public record TestDocument(String id) {}
