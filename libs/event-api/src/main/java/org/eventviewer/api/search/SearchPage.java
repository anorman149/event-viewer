package org.eventviewer.api.search;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public record SearchPage(
        @Min(0) int page,
        @Min(1) @Max(1000) int size
) {
    public SearchPage() {
        this(0, 20);
    }
}
