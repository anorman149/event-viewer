package org.eventviewer.opensearch;

import java.util.List;

public record SearchResult<T>(List<T> hits, long total) {}
