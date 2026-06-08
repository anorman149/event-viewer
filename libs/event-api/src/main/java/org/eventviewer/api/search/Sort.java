package org.eventviewer.api.search;

import jakarta.validation.constraints.NotNull;

public record Sort(
        @NotNull SearchField field,
        @NotNull SortDirection direction
) {}
