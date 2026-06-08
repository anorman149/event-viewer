package org.eventviewer.opensearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.eventviewer.opensearch.autoconfigure.OsClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.SortOptions;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.aggregations.Aggregation;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.HitsMetadata;
import org.opensearch.client.opensearch.core.search.TotalHits;
import org.opensearch.client.opensearch.core.search.TotalHitsRelation;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OsDocumentClientSearchTest {

    @Mock
    OpenSearchClient openSearchClient;

    @Mock
    OsSchemaRegistry registry;

    OsClient client;

    @BeforeEach
    void setUp() {
        OsIndexMetadata metadata = new OsIndexMetadata();
        metadata.setReadAlias("test_events_read");
        metadata.setWriteAlias("test_events_write");
        metadata.setDocumentClass(TestDocument.class);

        when(registry.getMetadata(TestDocument.class)).thenReturn(metadata);

        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        client = new OsClient(openSearchClient, registry, new SimpleMeterRegistry(), new FieldNameMapper(), objectMapper);
    }

    @Test
    @SuppressWarnings("unchecked")
    void search_setsIndexToReadAlias() throws Exception {
        SearchResponse<ObjectNode> mockResponse = buildEmptyResponse();
        when(openSearchClient.search(any(SearchRequest.class), eq(ObjectNode.class))).thenReturn(mockResponse);

        Query query = Query.of(q -> q.matchAll(m -> m));
        SortOptions sort = SortOptions.of(s -> s.field(f -> f.field("eventId").order(SortOrder.Asc)));
        OsSearchRequest request = new OsSearchRequest(query, List.of(sort), Map.of(), 10, null, 0);

        client.search(TestDocument.class, request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(openSearchClient).search(captor.capture(), eq(ObjectNode.class));
        assertThat(captor.getValue().index()).containsExactly("test_events_read");
    }

    @Test
    @SuppressWarnings("unchecked")
    void search_assemblesQuerySortAndAggregations() throws Exception {
        SearchResponse<ObjectNode> mockResponse = buildEmptyResponse();
        when(openSearchClient.search(any(SearchRequest.class), eq(ObjectNode.class))).thenReturn(mockResponse);

        Query query = Query.of(q -> q.matchAll(m -> m));
        SortOptions sort = SortOptions.of(s -> s.field(f -> f.field("timestamp").order(SortOrder.Desc)));
        Aggregation agg = Aggregation.of(a -> a.terms(t -> t.field("schemaType")));
        Map<String, Aggregation> aggregations = Map.of("by_schema", agg);

        OsSearchRequest request = new OsSearchRequest(query, List.of(sort), aggregations, 20, null, 0);

        client.search(TestDocument.class, request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(openSearchClient).search(captor.capture(), eq(ObjectNode.class));

        SearchRequest built = captor.getValue();
        assertThat(built.query()).isNotNull();
        assertThat(built.sort()).hasSize(1);
        assertThat(built.aggregations()).containsKey("by_schema");
        assertThat(built.size()).isEqualTo(20);
    }

    @Test
    @SuppressWarnings("unchecked")
    void search_searchAfterNull_notSetOnRequest() throws Exception {
        SearchResponse<ObjectNode> mockResponse = buildEmptyResponse();
        when(openSearchClient.search(any(SearchRequest.class), eq(ObjectNode.class))).thenReturn(mockResponse);

        OsSearchRequest request = new OsSearchRequest(
                Query.of(q -> q.matchAll(m -> m)), List.of(), Map.of(), 10, null, 0);

        client.search(TestDocument.class, request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(openSearchClient).search(captor.capture(), eq(ObjectNode.class));
        assertThat(captor.getValue().searchAfter()).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    void search_searchAfterNonNull_isSetOnRequest() throws Exception {
        SearchResponse<ObjectNode> mockResponse = buildEmptyResponse();
        when(openSearchClient.search(any(SearchRequest.class), eq(ObjectNode.class))).thenReturn(mockResponse);

        OsSearchRequest request = new OsSearchRequest(
                Query.of(q -> q.matchAll(m -> m)), List.of(), Map.of(), 10, List.of("abc"), 1);

        client.search(TestDocument.class, request);

        ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(openSearchClient).search(captor.capture(), eq(ObjectNode.class));
        assertThat(captor.getValue().searchAfter()).isNotEmpty();
    }

    @SuppressWarnings("unchecked")
    private SearchResponse<ObjectNode> buildEmptyResponse() {
        TotalHits total = TotalHits.of(t -> t.value(0).relation(TotalHitsRelation.Eq));
        HitsMetadata<ObjectNode> hitsMetadata = Mockito.mock(HitsMetadata.class);
        when(hitsMetadata.total()).thenReturn(total);
        when(hitsMetadata.hits()).thenReturn(List.of());
        SearchResponse<ObjectNode> response = Mockito.mock(SearchResponse.class);
        when(response.hits()).thenReturn(hitsMetadata);
        when(response.aggregations()).thenReturn(Map.of());
        return response;
    }
}
