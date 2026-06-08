package org.eventviewer.api.search;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ConditionExprTest {

    @Test
    void eq_storesFieldAndValue() {
        ConditionExpr expr = ConditionExpr.eq(SearchField.SCHEMA_TYPE, 1);

        assertThat(expr.type()).isEqualTo(ConditionExpr.Type.EQ);
        assertThat(expr.field()).isEqualTo(SearchField.SCHEMA_TYPE);
        assertThat(expr.value()).isEqualTo(1);
        assertThat(expr.values()).isNull();
        assertThat(expr.from()).isNull();
        assertThat(expr.to()).isNull();
    }

    @Test
    void in_storesFieldAndValues() {
        List<Integer> vals = List.of(1, 2, 3);
        ConditionExpr expr = ConditionExpr.in(SearchField.SCHEMA_TYPE, vals);

        assertThat(expr.type()).isEqualTo(ConditionExpr.Type.IN);
        assertThat(expr.field()).isEqualTo(SearchField.SCHEMA_TYPE);
        assertThat(expr.values()).isEqualTo(vals);
        assertThat(expr.value()).isNull();
    }

    @Test
    void between_storesFieldFromAndTo() {
        ConditionExpr expr = ConditionExpr.between(SearchField.TIMESTAMP, "2024-01-01", "2024-12-31");

        assertThat(expr.type()).isEqualTo(ConditionExpr.Type.BETWEEN);
        assertThat(expr.field()).isEqualTo(SearchField.TIMESTAMP);
        assertThat(expr.from()).isEqualTo("2024-01-01");
        assertThat(expr.to()).isEqualTo("2024-12-31");
    }

    @Test
    void exists_storesField() {
        ConditionExpr expr = ConditionExpr.exists(SearchField.RULE_RESULT_STATUS);

        assertThat(expr.type()).isEqualTo(ConditionExpr.Type.EXISTS);
        assertThat(expr.field()).isEqualTo(SearchField.RULE_RESULT_STATUS);
        assertThat(expr.value()).isNull();
        assertThat(expr.values()).isNull();
    }

    @Test
    void notExists_storesField() {
        ConditionExpr expr = ConditionExpr.notExists(SearchField.RULE_RESULT_STATUS);

        assertThat(expr.type()).isEqualTo(ConditionExpr.Type.NOT_EXISTS);
        assertThat(expr.field()).isEqualTo(SearchField.RULE_RESULT_STATUS);
    }
}
