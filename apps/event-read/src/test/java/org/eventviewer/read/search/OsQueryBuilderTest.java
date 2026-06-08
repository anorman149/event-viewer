package org.eventviewer.read.search;

import org.eventviewer.api.search.BooleanExpr;
import org.eventviewer.api.search.ConditionExpr;
import org.eventviewer.api.search.SearchField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch._types.query_dsl.Query;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OsQueryBuilderTest {

    private OsQueryBuilder builder;

    @BeforeEach
    void setUp() {
        builder = new OsQueryBuilder();
    }

    @Test
    void nullExpression_producesMatchAllQuery() {
        Query q = builder.build(null);
        assertThat(q.isMatchAll()).isTrue();
    }

    @Test
    void eq_producesTermQuery() {
        Query q = builder.build(ConditionExpr.eq(SearchField.SCHEMA_TYPE, 1));
        assertThat(q.isTerm()).isTrue();
        assertThat(q.term().field()).isEqualTo(SearchField.SCHEMA_TYPE.fieldName());
    }

    @Test
    void in_producesTermsQuery() {
        Query q = builder.build(ConditionExpr.in(SearchField.SCHEMA_TYPE, List.of(1, 2)));
        assertThat(q.isTerms()).isTrue();
        assertThat(q.terms().field()).isEqualTo(SearchField.SCHEMA_TYPE.fieldName());
    }

    @Test
    void between_producesRangeQuery() {
        Query q = builder.build(ConditionExpr.between(SearchField.TIMESTAMP, "2024-01-01T00:00:00Z", "2024-12-31T23:59:59Z"));
        assertThat(q.isRange()).isTrue();
        assertThat(q.range().field()).isEqualTo(SearchField.TIMESTAMP.fieldName());
    }

    @Test
    void exists_producesExistsQuery() {
        Query q = builder.build(ConditionExpr.exists(SearchField.RULE_RESULT_STATUS));
        assertThat(q.isExists()).isTrue();
        assertThat(q.exists().field()).isEqualTo(SearchField.RULE_RESULT_STATUS.fieldName());
    }

    @Test
    void notExists_producesBoolMustNotExistsQuery() {
        Query q = builder.build(ConditionExpr.notExists(SearchField.RULE_RESULT_STATUS));
        assertThat(q.isBool()).isTrue();
        assertThat(q.bool().mustNot()).hasSize(1);
        assertThat(q.bool().mustNot().get(0).isExists()).isTrue();
        assertThat(q.bool().mustNot().get(0).exists().field()).isEqualTo(SearchField.RULE_RESULT_STATUS.fieldName());
    }

    @Test
    void booleanExpr_must_populatesMustClause() {
        BooleanExpr bool = BooleanExpr.must(
                ConditionExpr.eq(SearchField.SCHEMA_TYPE, 1),
                ConditionExpr.exists(SearchField.RULE_RESULT_STATUS));
        Query q = builder.build(bool);
        assertThat(q.isBool()).isTrue();
        assertThat(q.bool().must()).hasSize(2);
        assertThat(q.bool().should()).isEmpty();
        assertThat(q.bool().mustNot()).isEmpty();
    }

    @Test
    void booleanExpr_should_populatesShouldClause() {
        BooleanExpr bool = BooleanExpr.should(ConditionExpr.eq(SearchField.SCHEMA_TYPE, 2));
        Query q = builder.build(bool);
        assertThat(q.isBool()).isTrue();
        assertThat(q.bool().should()).hasSize(1);
    }

    @Test
    void booleanExpr_mustAndMustNot_populatesBothClauses() {
        BooleanExpr bool = BooleanExpr.builder()
                .must(ConditionExpr.eq(SearchField.SCHEMA_TYPE, 1))
                .mustNot(ConditionExpr.exists(SearchField.RULE_RESULT_STATUS))
                .build();
        Query q = builder.build(bool);
        assertThat(q.bool().must()).hasSize(1);
        assertThat(q.bool().mustNot()).hasSize(1);
    }

    @Test
    void eq_eventIdField_usesCorrectFieldName() {
        Query q = builder.build(ConditionExpr.eq(SearchField.EVENT_ID, "some-uuid"));
        assertThat(q.term().field()).isEqualTo("eventId");
    }
}
