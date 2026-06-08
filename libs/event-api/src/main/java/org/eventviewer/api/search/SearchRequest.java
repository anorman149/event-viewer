package org.eventviewer.api.search;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public record SearchRequest(
        Expression expression,
        @NotNull @Valid CursorPageable cursorPageable,
        @Valid List<AggregationRequest> aggregations
) {
    public SearchRequest() {
        this(null, new CursorPageable(), List.of());
    }
}
