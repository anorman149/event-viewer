package org.eventviewer.api.search;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BooleanExprTest {

    @Test
    void must_factoryMethod_populatesMustOnly() {
        ConditionExpr inner = ConditionExpr.eq(SearchField.SCHEMA_TYPE, 1);
        BooleanExpr expr = BooleanExpr.must(inner);

        assertThat(expr.must()).containsExactly(inner);
        assertThat(expr.should()).isEmpty();
        assertThat(expr.mustNot()).isEmpty();
    }

    @Test
    void should_factoryMethod_populatesShouldOnly() {
        ConditionExpr inner = ConditionExpr.eq(SearchField.SCHEMA_TYPE, 2);
        BooleanExpr expr = BooleanExpr.should(inner);

        assertThat(expr.should()).containsExactly(inner);
        assertThat(expr.must()).isEmpty();
        assertThat(expr.mustNot()).isEmpty();
    }

    @Test
    void mustNot_factoryMethod_populatesMustNotOnly() {
        ConditionExpr inner = ConditionExpr.exists(SearchField.RULE_RESULT_STATUS);
        BooleanExpr expr = BooleanExpr.mustNot(inner);

        assertThat(expr.mustNot()).containsExactly(inner);
        assertThat(expr.must()).isEmpty();
        assertThat(expr.should()).isEmpty();
    }

    @Test
    void builder_combinesAllClauses() {
        ConditionExpr mustExpr = ConditionExpr.eq(SearchField.SCHEMA_TYPE, 1);
        ConditionExpr shouldExpr = ConditionExpr.eq(SearchField.EVENT_ID, "abc");
        ConditionExpr mustNotExpr = ConditionExpr.exists(SearchField.RULE_RESULT_STATUS);

        BooleanExpr expr = BooleanExpr.builder()
                .must(mustExpr)
                .should(shouldExpr)
                .mustNot(mustNotExpr)
                .build();

        assertThat(expr.must()).containsExactly(mustExpr);
        assertThat(expr.should()).containsExactly(shouldExpr);
        assertThat(expr.mustNot()).containsExactly(mustNotExpr);
    }

    @Test
    void sort_record_storesFieldAndDirection() {
        Sort sort = new Sort(SearchField.TIMESTAMP, SortDirection.DESC);

        assertThat(sort.field()).isEqualTo(SearchField.TIMESTAMP);
        assertThat(sort.direction()).isEqualTo(SortDirection.DESC);
    }
}
