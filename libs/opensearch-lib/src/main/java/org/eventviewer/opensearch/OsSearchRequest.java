package org.eventviewer.opensearch;

import org.opensearch.client.opensearch._types.SortOptions;
import org.opensearch.client.opensearch._types.aggregations.Aggregation;
import org.opensearch.client.opensearch._types.query_dsl.Query;

import java.util.List;
import java.util.Map;

public record OsSearchRequest(
        Query query,
        List<SortOptions> sort,
        Map<String, Aggregation> aggregations,
        int size,
        List<Object> searchAfter,
        int pageNumber
) {}
