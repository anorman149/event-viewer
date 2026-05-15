package org.eventviewer.opensearch;

public record OsIndexMetadata(
        Class<?> documentClass,
        String indexPattern,
        String templateName,
        String writeAlias,
        String readAlias
) {}
