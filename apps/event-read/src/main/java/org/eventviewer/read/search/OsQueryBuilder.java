package org.eventviewer.read.search;

import org.eventviewer.api.search.BooleanExpr;
import org.eventviewer.api.search.ConditionExpr;
import org.eventviewer.api.search.Expression;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Component
public class OsQueryBuilder {

    public Query build(Expression expression) {
        if (expression == null) {
            return Query.of(q -> q.matchAll(m -> m));
        }
        return switch (expression) {
            case ConditionExpr cond -> buildCondition(cond);
            case BooleanExpr bool -> buildBoolean(bool);
        };
    }

    private Query buildCondition(ConditionExpr cond) {
        String fieldName = cond.field().fieldName();
        return switch (cond.type()) {
            case EQ -> {
                FieldValue fv = toFieldValue(cond.value());
                yield Query.of(q -> q.term(t -> t.field(fieldName).value(fv)));
            }
            case IN -> {
                List<FieldValue> fieldValues = cond.values().stream()
                        .map(this::toFieldValue)
                        .toList();
                yield Query.of(q -> q.terms(t -> t
                        .field(fieldName)
                        .terms(tv -> tv.value(fieldValues))));
            }
            case BETWEEN -> {
                JsonData gte = toJsonData(cond.from());
                JsonData lte = toJsonData(cond.to());
                yield Query.of(q -> q.range(r -> r.field(fieldName).gte(gte).lte(lte)));
            }
            case EXISTS -> Query.of(q -> q.exists(e -> e.field(fieldName)));
            case NOT_EXISTS -> Query.of(q -> q.bool(b -> b
                    .mustNot(mn -> mn.exists(e -> e.field(fieldName)))));
        };
    }

    private Query buildBoolean(BooleanExpr bool) {
        return Query.of(q -> q.bool(b -> {
            if (!bool.must().isEmpty()) {
                b.must(bool.must().stream().map(this::build).toList());
            }
            if (!bool.should().isEmpty()) {
                b.should(bool.should().stream().map(this::build).toList());
            }
            if (!bool.mustNot().isEmpty()) {
                b.mustNot(bool.mustNot().stream().map(this::build).toList());
            }
            return b;
        }));
    }

    private FieldValue toFieldValue(Object val) {
        if (val instanceof String s) return FieldValue.of(f -> f.stringValue(s));
        if (val instanceof Long l) return FieldValue.of(f -> f.longValue(l));
        if (val instanceof Integer i) return FieldValue.of(f -> f.longValue(i.longValue()));
        if (val instanceof Double d) return FieldValue.of(f -> f.doubleValue(d));
        if (val instanceof Boolean b) return FieldValue.of(f -> f.booleanValue(b));
        return FieldValue.of(f -> f.stringValue(String.valueOf(val)));
    }

    private JsonData toJsonData(Object val) {
        if (val instanceof Instant instant) {
            return JsonData.of(DateTimeFormatter.ISO_INSTANT.format(instant));
        }
        if (val instanceof String s) return JsonData.of(s);
        if (val instanceof Number n) return JsonData.of(n.toString());
        return JsonData.of(String.valueOf(val));
    }
}
