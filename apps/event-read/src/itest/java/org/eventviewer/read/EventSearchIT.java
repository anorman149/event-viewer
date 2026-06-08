package org.eventviewer.read;

import org.eventviewer.api.search.AggregationRequest;
import org.eventviewer.api.search.AggregationType;
import org.eventviewer.api.search.BooleanExpr;
import org.eventviewer.api.search.ConditionExpr;
import org.eventviewer.api.search.CursorPageable;
import org.eventviewer.api.search.SearchField;
import org.eventviewer.api.search.SearchPage;
import org.eventviewer.api.search.SearchRequest;
import org.eventviewer.api.search.SearchResponse;
import org.eventviewer.api.search.Sort;
import org.eventviewer.api.search.SortDirection;
import org.eventviewer.read.domain.EventSearchResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.mapping.DynamicMapping;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EventSearchIT extends BaseTest {

    @Autowired
    OpenSearchClient openSearchClient;

    private static final Instant BASE_TIME = Instant.parse("2024-06-01T12:00:00Z");

    // 7 fixture docs:
    //  id-1..3: schemaType=1, id-1 and id-2 have ruleResults="rule-1_1"
    //  id-4..5: schemaType=2, id-4 has ruleResults="rule-1_1"
    //  id-6..7: schemaType=3, no ruleResults
    private final List<Map<String, Object>> FIXTURE = buildFixture();

    @BeforeAll
    void setupFixture() throws Exception {
        // event-read has no OsSchemaManager — create the index and aliases directly.
        // Mappings mirror EventStorageMigration so term/range queries behave identically to production.
        TypeMapping mapping = TypeMapping.of(m -> m
                .dynamic(DynamicMapping.False)
                .properties("eventId",     p -> p.keyword(k -> k.norms(false)))
                .properties("schemaType",  p -> p.integer(i -> i))
                .properties("timestamp",   p -> p.date(d -> d))
                .properties("s3FileName",  p -> p.keyword(k -> k.docValues(false).index(false)))
                .properties("podId",       p -> p.keyword(k -> k.docValues(false).index(false)))
                .properties("batchOffset", p -> p.long_(l -> l.docValues(false).index(false)))
                .properties("batchLength", p -> p.long_(l -> l.docValues(false).index(false)))
                .properties("ruleResults", p -> p.keyword(k -> k.docValues(false)))
        );
        openSearchClient.indices().create(req -> req
                .index("events-test-000001")
                .mappings(mapping)
                .aliases(Map.of(
                        "events_write", org.opensearch.client.opensearch.indices.Alias.of(a -> a.isWriteIndex(true)),
                        "events_read", org.opensearch.client.opensearch.indices.Alias.of(a -> a)
                )));

        for (Map<String, Object> doc : FIXTURE) {
            String id = (String) doc.get("eventId");
            openSearchClient.index(req -> req
                    .index("events_write")
                    .id(id)
                    .document(doc));
        }

        openSearchClient.indices().refresh(req -> req.index("events_write"));
    }

    // ── eq ───────────────────────────────────────────────────────────────────────

    @Test
    void eq_schemaType_returnsOnlyMatchingDocs() {
        SearchRequest req = searchRequest(ConditionExpr.eq(SearchField.SCHEMA_TYPE, 1), 20);

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.hits()).hasSize(3);
        assertThat(response.totalHits()).isEqualTo(3L);
        assertThat(response.hits()).allSatisfy(r -> assertThat(r.schemaType()).isEqualTo(1));
    }

    @Test
    void eq_eventId_returnsExactlyOneDoc() {
        SearchRequest req = searchRequest(ConditionExpr.eq(SearchField.EVENT_ID, "id-3"), 20);

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.hits()).hasSize(1);
        assertThat(response.totalHits()).isEqualTo(1L);
        assertThat(response.hits().get(0).eventId()).isEqualTo("id-3");
        assertThat(response.hits().get(0).schemaType()).isEqualTo(1);
    }

    @Test
    void eq_ruleResults_returnsOnlyDocsWithMatchingRule() {
        SearchRequest req = searchRequest(ConditionExpr.eq(SearchField.RULE_RESULT_STATUS, "rule-1_1"), 20);

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.hits()).hasSize(3);
        assertThat(response.totalHits()).isEqualTo(3L);
        assertThat(response.hits()).extracting(EventSearchResult::eventId)
                .containsExactlyInAnyOrder("id-1", "id-2", "id-4");
    }

    @Test
    void in_schemaTypes_returnsDocsMatchingAnyValue() {
        SearchRequest req = searchRequest(ConditionExpr.in(SearchField.SCHEMA_TYPE, List.of(1, 2)), 20);

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.hits()).hasSize(5);
        assertThat(response.totalHits()).isEqualTo(5L);
        assertThat(response.hits()).allSatisfy(r -> assertThat(r.schemaType()).isIn(1, 2));
    }

    @Test
    void between_timestamp_returnsOnlyDocsWithinRange() {
        Instant from = BASE_TIME.minusSeconds(1);
        Instant to = BASE_TIME.plusSeconds(3601);
        SearchRequest req = searchRequest(ConditionExpr.between(SearchField.TIMESTAMP, from.toString(), to.toString()), 20);

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.hits()).hasSize(7);
        assertThat(response.totalHits()).isEqualTo(7L);
        response.hits().forEach(r -> assertThat(r.timestamp()).isBetween(from, to));
    }

    @Test
    void between_schemaType_integerRange_returnsCorrectDocs() {
        SearchRequest req = searchRequest(ConditionExpr.between(SearchField.SCHEMA_TYPE, "1", "2"), 20);

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.hits()).hasSize(5);
        assertThat(response.totalHits()).isEqualTo(5L);
        assertThat(response.hits()).allSatisfy(r -> assertThat(r.schemaType()).isIn(1, 2));
    }

    @Test
    void exists_ruleResultStatus_returnsOnlyDocsWithField() {
        SearchRequest req = searchRequest(ConditionExpr.exists(SearchField.RULE_RESULT_STATUS), 20);

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.hits()).hasSize(3);
        assertThat(response.totalHits()).isEqualTo(3L);
    }

    @Test
    void notExists_ruleResultStatus_returnsOnlyDocsWithoutField() {
        SearchRequest req = searchRequest(ConditionExpr.notExists(SearchField.RULE_RESULT_STATUS), 20);

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.hits()).hasSize(4);
        assertThat(response.totalHits()).isEqualTo(4L);
    }

    @Test
    void booleanExpr_mustAndMustNot_returnsCorrectIntersection() {
        BooleanExpr expr = BooleanExpr.builder()
                .must(ConditionExpr.eq(SearchField.SCHEMA_TYPE, 1))
                .mustNot(ConditionExpr.exists(SearchField.RULE_RESULT_STATUS))
                .build();
        SearchRequest req = searchRequest(expr, 20);

        SearchResponse<EventSearchResult> response = post(req);

        // schemaType=1 AND no ruleResults → only id-3
        assertThat(response.hits()).hasSize(1);
        assertThat(response.totalHits()).isEqualTo(1L);
        assertThat(response.hits().get(0).schemaType()).isEqualTo(1);
    }

    @Test
    void booleanExpr_should_returnsUnionOfBothClauses() {
        BooleanExpr expr = BooleanExpr.should(
                ConditionExpr.eq(SearchField.SCHEMA_TYPE, 1),
                ConditionExpr.eq(SearchField.SCHEMA_TYPE, 3));
        SearchRequest req = searchRequest(expr, 20);

        SearchResponse<EventSearchResult> response = post(req);

        // schemaType=1 (3 docs) OR schemaType=3 (2 docs) → 5 docs
        assertThat(response.hits()).hasSize(5);
        assertThat(response.totalHits()).isEqualTo(5L);
        assertThat(response.hits()).allSatisfy(r -> assertThat(r.schemaType()).isIn(1, 3));
    }

    @Test
    void emptyResultSet_expressionMatchesNoDocs() {
        SearchRequest req = searchRequest(ConditionExpr.eq(SearchField.SCHEMA_TYPE, 999), 20);

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.hits()).isEmpty();
        assertThat(response.totalHits()).isEqualTo(0L);
        assertThat(response.nextPage()).isNull();
    }

    @Test
    void nullExpression_matchAll_returnsAllFixtureDocs() {
        SearchRequest req = searchRequest(null, 20);

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.hits()).hasSize(7);
        assertThat(response.totalHits()).isEqualTo(7L);
    }

    // ── Pagination ────────────────────────────────────────────────────────────

    @Test
    void searchAfter_paginatesCorrectly() {
        Sort sort = new Sort(SearchField.TIMESTAMP, SortDirection.ASC);
        SearchRequest first = new SearchRequest(null,
                new CursorPageable(new SearchPage(0, 2), sort, null), List.of());

        SearchResponse<EventSearchResult> page1 = post(first);
        assertThat(page1.hits()).hasSize(2);
        assertThat(page1.nextPage()).isNotNull();

        SearchRequest second = new SearchRequest(null, page1.nextPage(), List.of());
        SearchResponse<EventSearchResult> page2 = post(second);
        assertThat(page2.hits()).hasSize(2);

        List<String> page1Ids = page1.hits().stream().map(EventSearchResult::eventId).toList();
        List<String> page2Ids = page2.hits().stream().map(EventSearchResult::eventId).toList();
        assertThat(page1Ids).doesNotContainAnyElementsOf(page2Ids);

        // page2 timestamps all come after page1 timestamps
        Instant page1LastTimestamp = page1.hits().getLast().timestamp();
        page2.hits().forEach(r -> assertThat(r.timestamp()).isAfterOrEqualTo(page1LastTimestamp));
    }

    @Test
    void searchAfter_lastPageHasNullNextPage() {
        Sort sort = new Sort(SearchField.TIMESTAMP, SortDirection.ASC);
        SearchRequest req = new SearchRequest(null,
                new CursorPageable(new SearchPage(0, 100), sort, null), List.of());

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.nextPage()).isNull();
    }

    @Test
    void searchAfter_exhaustAllPages_collectsAllDocs() {
        Sort sort = new Sort(SearchField.TIMESTAMP, SortDirection.ASC);
        SearchRequest req = new SearchRequest(null,
                new CursorPageable(new SearchPage(0, 2), sort, null), List.of());

        Set<String> allIds = new HashSet<>();
        int pageCount = 0;
        SearchResponse<EventSearchResult> response;

        do {
            response = post(req);
            response.hits().forEach(r -> allIds.add(r.eventId()));
            pageCount++;
            if (response.nextPage() != null) {
                req = new SearchRequest(null, response.nextPage(), List.of());
            }
        } while (response.nextPage() != null);

        // 7 docs, size=2 → pages 1-3 have 2 docs, page 4 has 1 doc
        assertThat(pageCount).isEqualTo(4);
        assertThat(allIds).hasSize(7);
        assertThat(response.hits()).hasSize(1);
    }

    @Test
    void search_sortDescending_resultsInDescendingTimestampOrder() {
        Sort sort = new Sort(SearchField.TIMESTAMP, SortDirection.DESC);
        SearchRequest req = new SearchRequest(null,
                new CursorPageable(new SearchPage(0, 7), sort, null), List.of());

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.hits()).hasSize(7);
        List<Instant> timestamps = response.hits().stream().map(EventSearchResult::timestamp).toList();
        for (int i = 0; i < timestamps.size() - 1; i++) {
            assertThat(timestamps.get(i)).isAfterOrEqualTo(timestamps.get(i + 1));
        }
    }

    @Test
    void totalHits_reflectsEntireMatchingSet_notJustPage() {
        SearchRequest req = searchRequest(null, 1);

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.hits()).hasSize(1);
        assertThat(response.totalHits()).isEqualTo(7L);
    }

    // ── Aggregations ─────────────────────────────────────────────────────────

    @Test
    void terms_aggregation_onSchemaType_returnsExactBuckets() {
        AggregationRequest agg = new AggregationRequest("by_schema", AggregationType.TERMS, SearchField.SCHEMA_TYPE, null);
        SearchRequest req = new SearchRequest(null,
                new CursorPageable(new SearchPage(0, 1), new Sort(SearchField.TIMESTAMP, SortDirection.ASC), null),
                List.of(agg));

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.aggregations()).containsKey("by_schema");
        var buckets = response.aggregations().get("by_schema").buckets();
        assertThat(buckets).hasSize(3);

        // Use toString() on key to avoid Integer/Long type ambiguity from Jackson deserialization
        Map<String, Long> bucketMap = new HashMap<>();
        buckets.forEach(b -> bucketMap.put(b.key().toString(), b.docCount()));
        assertThat(bucketMap).containsEntry("1", 3L);
        assertThat(bucketMap).containsEntry("2", 2L);
        assertThat(bucketMap).containsEntry("3", 2L);
    }

    @Test
    void dateHistogram_aggregation_onTimestamp_returnsOneBucketWithAllDocs() {
        AggregationRequest agg = new AggregationRequest("by_hour", AggregationType.DATE_HISTOGRAM, SearchField.TIMESTAMP, "1h");
        SearchRequest req = new SearchRequest(null,
                new CursorPageable(new SearchPage(0, 1), new Sort(SearchField.TIMESTAMP, SortDirection.ASC), null),
                List.of(agg));

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.aggregations()).containsKey("by_hour");
        var buckets = response.aggregations().get("by_hour").buckets();
        // All 7 docs fall within the same 1-hour window starting at 2024-06-01T12:00:00Z
        assertThat(buckets).hasSize(1);
        assertThat(buckets.get(0).docCount()).isEqualTo(7L);
    }

    @Test
    void aggregation_combinedWithExpressionFilter_bucketsReflectFilter() {
        AggregationRequest agg = new AggregationRequest("by_hour", AggregationType.DATE_HISTOGRAM, SearchField.TIMESTAMP, "1h");
        SearchRequest req = new SearchRequest(
                ConditionExpr.eq(SearchField.SCHEMA_TYPE, 1),
                new CursorPageable(new SearchPage(0, 1), new Sort(SearchField.TIMESTAMP, SortDirection.ASC), null),
                List.of(agg));

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.aggregations()).containsKey("by_hour");
        var buckets = response.aggregations().get("by_hour").buckets();
        // Only the 3 docs with schemaType=1 match the filter
        long totalInBuckets = buckets.stream().mapToLong(b -> b.docCount()).sum();
        assertThat(totalInBuckets).isEqualTo(3L);
    }

    // ── EventSearchResult contract ────────────────────────────────────────────

    @Test
    void eventSearchResult_doesNotExposeInternalStorageFields() throws Exception {
        SearchRequest req = searchRequest(null, 1);
        SearchResponse<EventSearchResult> response = post(req);

        String json = objectMapper.writeValueAsString(response.hits().get(0));
        assertThat(json).doesNotContain("s3FileName", "batchOffset", "batchLength");
    }

    // ── Validation ───────────────────────────────────────────────────────────

    @Test
    void missingCursorPageable_returns400() throws Exception {
        String body = objectMapper.writeValueAsString(Map.of("expression", Map.of()));
        ResponseEntity<String> response = restTemplate.exchange(
                baseUrl() + "/search/v1/events",
                HttpMethod.POST,
                new HttpEntity<>(body, authHeaders()),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void missingSortInCursorPageable_returns400() {
        // sort is @NotNull inside CursorPageable
        String body = """
                {"cursorPageable":{"page":{"page":0,"size":10}}}
                """;
        ResponseEntity<String> response = restTemplate.exchange(
                baseUrl() + "/search/v1/events",
                HttpMethod.POST,
                new HttpEntity<>(body, authHeaders()),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void invalidPageSize_zeroValue_returns400() throws Exception {
        SearchRequest req = new SearchRequest(null,
                new CursorPageable(new SearchPage(0, 0), new Sort(SearchField.TIMESTAMP, SortDirection.ASC), null),
                List.of());
        ResponseEntity<String> response = restTemplate.exchange(
                baseUrl() + "/search/v1/events",
                HttpMethod.POST,
                new HttpEntity<>(objectMapper.writeValueAsString(req), authHeaders()),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void malformedJsonBody_returns400() {
        ResponseEntity<String> response = restTemplate.exchange(
                baseUrl() + "/search/v1/events",
                HttpMethod.POST,
                new HttpEntity<>("{broken json", authHeaders()),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void aggregationOnNonAggregatableField_returns400() throws Exception {
        // EVENT_ID has allowedAggregation=null so TERMS fails isFieldAggregatable()
        AggregationRequest badAgg = new AggregationRequest("bad", AggregationType.TERMS, SearchField.EVENT_ID, null);
        SearchRequest req = new SearchRequest(null,
                new CursorPageable(new SearchPage(0, 10), new Sort(SearchField.TIMESTAMP, SortDirection.ASC), null),
                List.of(badAgg));

        ResponseEntity<String> response = restTemplate.exchange(
                baseUrl() + "/search/v1/events",
                HttpMethod.POST,
                new HttpEntity<>(objectMapper.writeValueAsString(req), authHeaders()),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void aggregation_wrongTypeOnAggregatableField_returns400() throws Exception {
        // TIMESTAMP only allows DATE_HISTOGRAM; TERMS must be rejected after isFieldAggregatable() fix
        AggregationRequest wrongType = new AggregationRequest("bad", AggregationType.TERMS, SearchField.TIMESTAMP, null);
        SearchRequest req = new SearchRequest(null,
                new CursorPageable(new SearchPage(0, 10), new Sort(SearchField.TIMESTAMP, SortDirection.ASC), null),
                List.of(wrongType));

        ResponseEntity<String> response = restTemplate.exchange(
                baseUrl() + "/search/v1/events",
                HttpMethod.POST,
                new HttpEntity<>(objectMapper.writeValueAsString(req), authHeaders()),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void noAuthorizationHeader_returns401() throws Exception {
        SearchRequest req = searchRequest(null, 10);
        org.springframework.http.HttpHeaders noAuth = new org.springframework.http.HttpHeaders();
        noAuth.setContentType(org.springframework.http.MediaType.APPLICATION_JSON);

        ResponseEntity<String> response = restTemplate.exchange(
                baseUrl() + "/search/v1/events",
                HttpMethod.POST,
                new HttpEntity<>(objectMapper.writeValueAsString(req), noAuth),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private SearchRequest searchRequest(org.eventviewer.api.search.Expression expression, int size) {
        return new SearchRequest(
                expression,
                new CursorPageable(new SearchPage(0, size), new Sort(SearchField.TIMESTAMP, SortDirection.ASC), null),
                List.of());
    }

    private SearchResponse<EventSearchResult> post(SearchRequest req) {
        try {
            ResponseEntity<SearchResponse<EventSearchResult>> response = restTemplate.exchange(
                    baseUrl() + "/search/v1/events",
                    HttpMethod.POST,
                    new HttpEntity<>(objectMapper.writeValueAsString(req), authHeaders()),
                    new ParameterizedTypeReference<>() {});
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
            return response.getBody();
        } catch (Exception e) {
            throw new RuntimeException("Failed to post search request", e);
        }
    }

    private static List<Map<String, Object>> buildFixture() {
        List<Map<String, Object>> docs = new ArrayList<>();

        // 3 docs with schemaType=1, id-1 and id-2 have ruleResults
        docs.add(doc("id-1", 1, BASE_TIME, "s3/key1", true));
        docs.add(doc("id-2", 1, BASE_TIME.plusSeconds(60), "s3/key2", true));
        docs.add(doc("id-3", 1, BASE_TIME.plusSeconds(120), "s3/key3", false));

        // 2 docs with schemaType=2, id-4 has ruleResults
        docs.add(doc("id-4", 2, BASE_TIME.plusSeconds(180), "s3/key4", true));
        docs.add(doc("id-5", 2, BASE_TIME.plusSeconds(240), "s3/key5", false));

        // 2 docs with schemaType=3, no ruleResults
        docs.add(doc("id-6", 3, BASE_TIME.plusSeconds(300), "s3/key6", false));
        docs.add(doc("id-7", 3, BASE_TIME.plusSeconds(360), "s3/key7", false));

        return docs;
    }

    private static Map<String, Object> doc(String id, int schemaType, Instant timestamp,
                                            String s3FileName, boolean withRuleResults) {
        Map<String, Object> m = new HashMap<>();
        m.put("eventId", id);
        m.put("schemaType", schemaType);
        m.put("timestamp", timestamp.toString());
        m.put("s3FileName", s3FileName);
        m.put("podId", "test-pod");
        m.put("batchOffset", 0L);
        m.put("batchLength", 100);
        if (withRuleResults) {
            m.put("ruleResults", List.of("rule-1_1"));
        }
        return m;
    }
}
