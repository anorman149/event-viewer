package org.eventviewer.api.search;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record AggregationRequest(
        @NotBlank String name,
        @NotNull AggregationType type,
        @NotNull SearchField field,
        String interval
) {
    @AssertTrue(message = "field does not support aggregation")
    public boolean isFieldAggregatable() {
        return field == null || field.allowedAggregation() != null;
    }
}
