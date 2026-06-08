package org.eventviewer.api.search;

import java.util.List;
import java.util.Map;

public record SearchResponse<T>(
        List<T> hits,
        long totalHits,
        CursorPageable nextPage,
        Map<String, AggregationResult> aggregations
) {}
