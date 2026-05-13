package org.eventviewer.api.error;

import java.util.List;

public record ErrorResponse(List<String> errors) {

    public static ErrorResponse of(String message) {
        return new ErrorResponse(List.of(message));
    }
}
