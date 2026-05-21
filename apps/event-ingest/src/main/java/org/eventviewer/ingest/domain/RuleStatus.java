package org.eventviewer.ingest.domain;

public enum RuleStatus {
    UNKNOWN(0),
    SUCCESS(1),
    FAILURE(2);

    private final int code;

    RuleStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
