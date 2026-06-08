package org.eventviewer.opensearch;

import java.util.Map;

public record OsAggregationBucket(
        Object key,
        long docCount,
        Map<String, OsAggregationResult> subAggregations
) {}
