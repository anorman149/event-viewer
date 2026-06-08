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
import org.eventviewer.opensearch.OsAdminClient;
import org.eventviewer.model.EventDocument;
import org.eventviewer.read.domain.EventSearchResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EventSearchIT extends BaseTest {

    @Autowired
    OpenSearchClient openSearchClient;

    @Autowired
    OsAdminClient osAdminClient;

    private static final String SCHEMA_TYPE_A = "1";
    private static final String SCHEMA_TYPE_B = "2";

    private static final Instant BASE_TIME = Instant.parse("2024-06-01T12:00:00Z");

    private final List<Map<String, Object>> FIXTURE = buildFixture();

    @BeforeAll
    void setupFixture() throws Exception {
        // Wait for schema manager to have created the index
        int retries = 0;
        while (!osAdminClient.indexExists(EventDocument.class) && retries++ < 20) {
            Thread.sleep(500);
        }

        // Index test documents directly with OS field names
        for (Map<String, Object> doc : FIXTURE) {
            String id = (String) doc.get("eventId");
            openSearchClient.index(req -> req
                    .index("events_write")
                    .id(id)
                    .document(doc));
        }

        // Refresh so documents are immediately searchable
        openSearchClient.indices().refresh(req -> req.index("events_write"));
    }

    // ── eq ───────────────────────────────────────────────────────────────────────

    @Test
    void eq_schemaType_returnsOnlyMatchingDocs() {
        SearchRequest req = searchRequest(ConditionExpr.eq(SearchField.SCHEMA_TYPE, 1), 20);

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.hits()).isNotEmpty();
        assertThat(response.hits()).allSatisfy(r -> assertThat(r.schemaType()).isEqualTo(1));
    }

    @Test
    void in_schemaTypes_returnsDocsMatchingAnyValue() {
        SearchRequest req = searchRequest(ConditionExpr.in(SearchField.SCHEMA_TYPE, List.of(1, 2)), 20);

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.hits()).isNotEmpty();
        assertThat(response.hits()).allSatisfy(r ->
                assertThat(r.schemaType()).isIn(1, 2));
    }

    @Test
    void between_timestamp_returnsOnlyDocsWithinRange() {
        Instant from = BASE_TIME.minusSeconds(1);
        Instant to = BASE_TIME.plusSeconds(3601);
        SearchRequest req = searchRequest(ConditionExpr.between(SearchField.TIMESTAMP, from.toString(), to.toString()), 20);

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.hits()).isNotEmpty();
        response.hits().forEach(r ->
                assertThat(r.timestamp()).isBetween(from, to));
    }

    @Test
    void exists_ruleResultStatus_returnsOnlyDocsWithField() {
        SearchRequest req = searchRequest(ConditionExpr.exists(SearchField.RULE_RESULT_STATUS), 20);

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.hits()).isNotEmpty();
        // All returned docs should have ruleResults populated (verified by fixture design)
        assertThat(response.totalHits()).isEqualTo(3L); // 3 docs have ruleResults
    }

    @Test
    void notExists_ruleResultStatus_returnsOnlyDocsWithoutField() {
        SearchRequest req = searchRequest(ConditionExpr.notExists(SearchField.RULE_RESULT_STATUS), 20);

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.hits()).isNotEmpty();
        assertThat(response.totalHits()).isEqualTo(4L); // 4 docs without ruleResults
    }

    @Test
    void booleanExpr_mustAndMustNot_returnsCorrectIntersection() {
        BooleanExpr expr = BooleanExpr.builder()
                .must(ConditionExpr.eq(SearchField.SCHEMA_TYPE, 1))
                .mustNot(ConditionExpr.exists(SearchField.RULE_RESULT_STATUS))
                .build();
        SearchRequest req = searchRequest(expr, 20);

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.hits()).isNotEmpty();
        assertThat(response.hits()).allSatisfy(r -> assertThat(r.schemaType()).isEqualTo(1));
    }

    @Test
    void nullExpression_matchAll_returnsAllFixtureDocs() {
        SearchRequest req = searchRequest(null, 20);

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.totalHits()).isEqualTo(7L); // total fixture size
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

        // Verify no duplicate IDs across pages
        List<String> page1Ids = page1.hits().stream().map(EventSearchResult::eventId).toList();
        List<String> page2Ids = page2.hits().stream().map(EventSearchResult::eventId).toList();
        assertThat(page1Ids).doesNotContainAnyElementsOf(page2Ids);
    }

    @Test
    void searchAfter_lastPageHasNullNextPage() {
        Sort sort = new Sort(SearchField.TIMESTAMP, SortDirection.ASC);
        // Page size larger than fixture — first and only page
        SearchRequest req = new SearchRequest(null,
                new CursorPageable(new SearchPage(0, 100), sort, null), List.of());

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.nextPage()).isNull();
    }

    // ── Aggregations ─────────────────────────────────────────────────────────

    @Test
    void terms_aggregation_onSchemaType_returnsCorrectBuckets() {
        AggregationRequest agg = new AggregationRequest("by_schema", AggregationType.TERMS, SearchField.SCHEMA_TYPE, null);
        SearchRequest req = new SearchRequest(null,
                new CursorPageable(new SearchPage(0, 1), new Sort(SearchField.TIMESTAMP, SortDirection.ASC), null),
                List.of(agg));

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.aggregations()).containsKey("by_schema");
        long totalBucketDocs = response.aggregations().get("by_schema").buckets().stream()
                .mapToLong(b -> b.docCount())
                .sum();
        assertThat(totalBucketDocs).isEqualTo(7L);
    }

    @Test
    void dateHistogram_aggregation_onTimestamp_returnsNonEmptyBuckets() {
        AggregationRequest agg = new AggregationRequest("by_hour", AggregationType.DATE_HISTOGRAM, SearchField.TIMESTAMP, "1h");
        SearchRequest req = new SearchRequest(null,
                new CursorPageable(new SearchPage(0, 1), new Sort(SearchField.TIMESTAMP, SortDirection.ASC), null),
                List.of(agg));

        SearchResponse<EventSearchResult> response = post(req);

        assertThat(response.aggregations()).containsKey("by_hour");
        assertThat(response.aggregations().get("by_hour").buckets()).isNotEmpty();
    }

    // ── EventSearchResult contract ────────────────────────────────────────────

    @Test
    void eventSearchResult_doesNotContainS3OrBatchFields() throws Exception {
        SearchRequest req = searchRequest(null, 1);
        SearchResponse<EventSearchResult> response = post(req);

        String json = objectMapper.writeValueAsString(response.hits().get(0));
        assertThat(json).doesNotContain("s3Key", "s3FileName", "batchOffset", "batchLength");
    }

    // ── Error responses ───────────────────────────────────────────────────────

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
    void aggregationOnNonAggregatableField_returns400() throws Exception {
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

    @SuppressWarnings("unchecked")
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

        // 3 docs with schemaType=1, 2 with ruleResults
        docs.add(doc("id-1", 1, BASE_TIME, "s3/key1", true));
        docs.add(doc("id-2", 1, BASE_TIME.plusSeconds(60), "s3/key2", true));
        docs.add(doc("id-3", 1, BASE_TIME.plusSeconds(120), "s3/key3", false));

        // 2 docs with schemaType=2, 1 with ruleResults
        docs.add(doc("id-4", 2, BASE_TIME.plusSeconds(180), "s3/key4", true));
        docs.add(doc("id-5", 2, BASE_TIME.plusSeconds(240), "s3/key5", false));

        // 2 docs with schemaType=3, none with ruleResults
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
