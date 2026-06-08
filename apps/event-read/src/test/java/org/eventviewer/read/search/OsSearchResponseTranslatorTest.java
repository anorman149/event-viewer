package org.eventviewer.read.search;

import org.eventviewer.api.search.CursorPageable;
import org.eventviewer.api.search.SearchField;
import org.eventviewer.api.search.SearchResponse;
import org.eventviewer.api.search.SortDirection;
import org.eventviewer.opensearch.OsAggregationBucket;
import org.eventviewer.opensearch.OsAggregationResult;
import org.eventviewer.opensearch.OsCursorPageable;
import org.eventviewer.opensearch.OsSearchResponse;
import org.eventviewer.model.EventDocument;
import org.eventviewer.read.domain.EventSearchResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch._types.SortOptions;
import org.opensearch.client.opensearch._types.SortOrder;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OsSearchResponseTranslatorTest {

    private OsSearchResponseTranslator translator;

    private static final Instant NOW = Instant.parse("2024-06-01T12:00:00Z");

    @BeforeEach
    void setUp() {
        translator = new OsSearchResponseTranslator();
    }

    @Test
    void translate_projectsEventDocumentToEventSearchResult() {
        EventDocument doc = new EventDocument("uuid-1", 1, NOW, "s3-key", "pod-1", 0, 100, List.of());
        OsSearchResponse<EventDocument> osResponse = new OsSearchResponse<>(
                List.of(doc), 1L, null, Map.of());

        SearchResponse<EventSearchResult> response = translator.translate(osResponse);

        assertThat(response.hits()).hasSize(1);
        EventSearchResult result = response.hits().get(0);
        assertThat(result.eventId()).isEqualTo("uuid-1");
        assertThat(result.schemaType()).isEqualTo(1);
        assertThat(result.timestamp()).isEqualTo(NOW);
    }

    @Test
    void translate_eventSearchResult_doesNotContainS3Fields() {
        EventDocument doc = new EventDocument("uuid-1", 1, NOW, "s3-key", "pod-1", 0, 100, List.of());
        OsSearchResponse<EventDocument> osResponse = new OsSearchResponse<>(
                List.of(doc), 1L, null, Map.of());

        SearchResponse<EventSearchResult> response = translator.translate(osResponse);

        EventSearchResult result = response.hits().get(0);
        // EventSearchResult only has eventId, schemaType, timestamp — no s3Key/batchOffset/batchLength
        assertThat(result).isInstanceOf(EventSearchResult.class);
        assertThat(EventSearchResult.class.getDeclaredFields()).hasSize(3);
    }

    @Test
    void translate_nullNextPage_propagatesNull() {
        OsSearchResponse<EventDocument> osResponse = new OsSearchResponse<>(
                List.of(), 0L, null, Map.of());

        SearchResponse<EventSearchResult> response = translator.translate(osResponse);

        assertThat(response.nextPage()).isNull();
    }

    @Test
    void translate_nextPage_mapsCorrectly() {
        SortOptions sortOptions = SortOptions.of(s -> s.field(f -> f
                .field(SearchField.TIMESTAMP.fieldName())
                .order(SortOrder.Desc)));
        OsCursorPageable osCursor = new OsCursorPageable(List.of(sortOptions), 20, 1, List.of("2024-01-01T00:00:00Z"));

        EventDocument doc = new EventDocument("uuid-1", 1, NOW, "s3-key", "pod-1", 0, 100, List.of());
        OsSearchResponse<EventDocument> osResponse = new OsSearchResponse<>(
                List.of(doc), 1L, osCursor, Map.of());

        SearchResponse<EventSearchResult> response = translator.translate(osResponse);

        CursorPageable nextPage = response.nextPage();
        assertThat(nextPage).isNotNull();
        assertThat(nextPage.sort().field()).isEqualTo(SearchField.TIMESTAMP);
        assertThat(nextPage.sort().direction()).isEqualTo(SortDirection.DESC);
        assertThat(nextPage.page().page()).isEqualTo(1);
        assertThat(nextPage.page().size()).isEqualTo(20);
        assertThat(nextPage.searchAfter()).containsExactly("2024-01-01T00:00:00Z");
    }

    @Test
    void translate_aggregationBuckets_mappedCorrectly() {
        OsAggregationBucket bucket = new OsAggregationBucket("type-a", 5L, Map.of());
        OsAggregationResult osAgg = new OsAggregationResult("by_schema", List.of(bucket));
        OsSearchResponse<EventDocument> osResponse = new OsSearchResponse<>(
                List.of(), 0L, null, Map.of("by_schema", osAgg));

        SearchResponse<EventSearchResult> response = translator.translate(osResponse);

        assertThat(response.aggregations()).containsKey("by_schema");
        assertThat(response.aggregations().get("by_schema").buckets()).hasSize(1);
        assertThat(response.aggregations().get("by_schema").buckets().get(0).key()).isEqualTo("type-a");
        assertThat(response.aggregations().get("by_schema").buckets().get(0).docCount()).isEqualTo(5L);
    }
}
