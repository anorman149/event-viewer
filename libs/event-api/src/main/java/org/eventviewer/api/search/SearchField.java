package org.eventviewer.api.search;

public enum SearchField {
    EVENT_ID("eventId", FieldType.KEYWORD, null),
    SCHEMA_TYPE("schemaType", FieldType.INTEGER, AggregationType.TERMS),
    TIMESTAMP("timestamp", FieldType.DATE, AggregationType.DATE_HISTOGRAM),
    RULE_RESULT_STATUS("ruleResults", FieldType.KEYWORD, null);

    private final String fieldName;
    private final FieldType fieldType;
    private final AggregationType allowedAggregation;

    SearchField(String fieldName, FieldType fieldType, AggregationType allowedAggregation) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.allowedAggregation = allowedAggregation;
    }

    public String fieldName() {
        return fieldName;
    }

    public FieldType fieldType() {
        return fieldType;
    }

    public AggregationType allowedAggregation() {
        return allowedAggregation;
    }
}
