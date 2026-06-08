package org.eventviewer.api.search;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "expressionType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ConditionExpr.class, name = "condition"),
        @JsonSubTypes.Type(value = BooleanExpr.class, name = "boolean")
})
public sealed interface Expression permits ConditionExpr, BooleanExpr {}
