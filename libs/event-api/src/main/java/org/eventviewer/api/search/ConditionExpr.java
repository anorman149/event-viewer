package org.eventviewer.api.search;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public final class ConditionExpr implements Expression {

    public enum Type { EQ, IN, BETWEEN, EXISTS, NOT_EXISTS }

    private final Type type;
    private final SearchField field;
    private final Object value;
    private final List<?> values;
    private final Object from;
    private final Object to;

    @JsonCreator
    private ConditionExpr(
            @JsonProperty("type") Type type,
            @JsonProperty("field") SearchField field,
            @JsonProperty("value") Object value,
            @JsonProperty("values") List<?> values,
            @JsonProperty("from") Object from,
            @JsonProperty("to") Object to) {
        this.type = type;
        this.field = field;
        this.value = value;
        this.values = values;
        this.from = from;
        this.to = to;
    }

    public static ConditionExpr eq(SearchField field, Object value) {
        return new ConditionExpr(Type.EQ, field, value, null, null, null);
    }

    public static ConditionExpr in(SearchField field, List<?> values) {
        return new ConditionExpr(Type.IN, field, null, values, null, null);
    }

    public static ConditionExpr between(SearchField field, Object from, Object to) {
        return new ConditionExpr(Type.BETWEEN, field, null, null, from, to);
    }

    public static ConditionExpr exists(SearchField field) {
        return new ConditionExpr(Type.EXISTS, field, null, null, null, null);
    }

    public static ConditionExpr notExists(SearchField field) {
        return new ConditionExpr(Type.NOT_EXISTS, field, null, null, null, null);
    }

    public Type type() { return type; }
    public SearchField field() { return field; }
    public Object value() { return value; }
    public List<?> values() { return values; }
    public Object from() { return from; }
    public Object to() { return to; }
}
