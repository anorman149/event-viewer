package org.eventviewer.read.search;

import org.eventviewer.api.search.AggregationRequest;
import org.eventviewer.api.search.AggregationType;
import org.eventviewer.api.search.SearchField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch._types.aggregations.Aggregation;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OsAggregationBuilderTest {

    private OsAggregationBuilder builder;

    @BeforeEach
    void setUp() {
        builder = new OsAggregationBuilder();
    }

    @Test
    void terms_producesTermsAggregation() {
        AggregationRequest req = new AggregationRequest("by_schema", AggregationType.TERMS, SearchField.SCHEMA_TYPE, null);
        Map<String, Aggregation> result = builder.build(List.of(req));

        assertThat(result).containsKey("by_schema");
        assertThat(result.get("by_schema").isTerms()).isTrue();
        assertThat(result.get("by_schema").terms().field()).isEqualTo(SearchField.SCHEMA_TYPE.fieldName());
    }

    @Test
    void dateHistogram_producesDateHistogramAggregationWithInterval() {
        AggregationRequest req = new AggregationRequest("by_hour", AggregationType.DATE_HISTOGRAM, SearchField.TIMESTAMP, "1h");
        Map<String, Aggregation> result = builder.build(List.of(req));

        assertThat(result).containsKey("by_hour");
        assertThat(result.get("by_hour").isDateHistogram()).isTrue();
        assertThat(result.get("by_hour").dateHistogram().field()).isEqualTo(SearchField.TIMESTAMP.fieldName());
    }

    @Test
    void valueCount_producesValueCountAggregation() {
        AggregationRequest req = new AggregationRequest("total", AggregationType.VALUE_COUNT, SearchField.EVENT_ID, null);
        Map<String, Aggregation> result = builder.build(List.of(req));

        assertThat(result).containsKey("total");
        assertThat(result.get("total").isValueCount()).isTrue();
        assertThat(result.get("total").valueCount().field()).isEqualTo(SearchField.EVENT_ID.fieldName());
    }

    @Test
    void emptyList_returnsEmptyMap() {
        assertThat(builder.build(List.of())).isEmpty();
    }
}
