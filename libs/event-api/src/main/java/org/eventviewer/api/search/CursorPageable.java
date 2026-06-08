package org.eventviewer.api.search;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public record CursorPageable(
        @Valid SearchPage page,
        @NotNull @Valid Sort sort,
        List<Object> searchAfter
) {
    public CursorPageable() {
        this(new SearchPage(), null, null);
    }
}
