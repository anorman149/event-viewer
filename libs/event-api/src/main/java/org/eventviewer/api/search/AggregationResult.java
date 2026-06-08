package org.eventviewer.api.search;

import java.util.List;

public record AggregationResult(
        String name,
        List<AggregationBucket> buckets
) {}
