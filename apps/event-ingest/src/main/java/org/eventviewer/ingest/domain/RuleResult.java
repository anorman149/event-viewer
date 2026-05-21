package org.eventviewer.ingest.domain;

import com.fasterxml.jackson.annotation.JsonValue;

public record RuleResult(String ruleId, RuleStatus status) {

    @JsonValue
    public String toComposite() {
        return ruleId + "_" + status.getCode();
    }
}
