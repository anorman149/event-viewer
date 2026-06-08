package org.eventviewer.api.search;

import java.util.Map;

public record AggregationBucket(
        Object key,
        long docCount,
        Map<String, AggregationResult> subAggregations
) {}
