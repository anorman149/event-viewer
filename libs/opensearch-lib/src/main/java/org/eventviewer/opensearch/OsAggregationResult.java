package org.eventviewer.opensearch;

import java.util.List;

public record OsAggregationResult(
        String name,
        List<OsAggregationBucket> buckets
) {}
