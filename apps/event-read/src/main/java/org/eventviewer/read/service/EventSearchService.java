package org.eventviewer.read.service;

import io.micrometer.core.annotation.Timed;
import org.eventviewer.api.search.AggregationRequest;
import org.eventviewer.api.search.Expression;
import org.eventviewer.api.search.SearchResponse;
import org.eventviewer.api.search.Sort;
import org.eventviewer.model.EventDocument;
import org.eventviewer.opensearch.OsDocumentClient;
import org.eventviewer.opensearch.OsException;
import org.eventviewer.opensearch.OsSearchRequest;
import org.eventviewer.opensearch.OsSearchResponse;
import org.eventviewer.read.domain.EventSearchException;
import org.eventviewer.read.domain.EventSearchResult;
import org.eventviewer.read.search.OsAggregationBuilder;
import org.eventviewer.read.search.OsQueryBuilder;
import org.eventviewer.read.search.OsSearchResponseTranslator;
import org.eventviewer.read.search.OsSortBuilder;
import org.opensearch.client.opensearch._types.SortOptions;
import org.opensearch.client.opensearch._types.aggregations.Aggregation;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class EventSearchService {

    private final OsDocumentClient osDocumentClient;
    private final OsQueryBuilder queryBuilder;
    private final OsSortBuilder sortBuilder;
    private final OsAggregationBuilder aggregationBuilder;
    private final OsSearchResponseTranslator responseTranslator;

    public EventSearchService(OsDocumentClient osDocumentClient,
                               OsQueryBuilder queryBuilder,
                               OsSortBuilder sortBuilder,
                               OsAggregationBuilder aggregationBuilder,
                               OsSearchResponseTranslator responseTranslator) {
        this.osDocumentClient = osDocumentClient;
        this.queryBuilder = queryBuilder;
        this.sortBuilder = sortBuilder;
        this.aggregationBuilder = aggregationBuilder;
        this.responseTranslator = responseTranslator;
    }

    @Timed(value = "event.read.service.search", histogram = true)
    public SearchResponse<EventSearchResult> search(Expression expression,
                                                     Sort sort,
                                                     List<Object> searchAfter,
                                                     int page,
                                                     int size,
                                                     List<AggregationRequest> aggregations) {
        try {
            Query query = queryBuilder.build(expression);
            List<SortOptions> sortOptions = sortBuilder.build(sort);
            Map<String, Aggregation> osAggregations = aggregationBuilder.build(aggregations);

            OsSearchRequest osRequest = new OsSearchRequest(query, sortOptions, osAggregations, size, searchAfter, page);
            OsSearchResponse<EventDocument> osResponse = osDocumentClient.search(EventDocument.class, osRequest);

            return responseTranslator.translate(osResponse);
        } catch (OsException e) {
            throw new EventSearchException("Search failed", e);
        }
    }
}
