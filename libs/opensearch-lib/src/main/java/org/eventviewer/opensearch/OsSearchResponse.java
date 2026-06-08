package org.eventviewer.opensearch;

import java.util.List;
import java.util.Map;

public record OsSearchResponse<T>(
        List<T> hits,
        long totalHits,
        OsCursorPageable nextPage,
        Map<String, OsAggregationResult> aggregations
) {}
