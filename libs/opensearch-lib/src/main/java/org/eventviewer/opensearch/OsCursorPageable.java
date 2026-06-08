package org.eventviewer.opensearch;

import org.opensearch.client.opensearch._types.SortOptions;

import java.util.List;

public record OsCursorPageable(
        List<SortOptions> sort,
        int size,
        int pageNumber,
        List<Object> searchAfter
) {}
