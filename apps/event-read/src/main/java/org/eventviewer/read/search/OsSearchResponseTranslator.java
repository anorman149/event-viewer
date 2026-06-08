package org.eventviewer.read.search;

import org.eventviewer.api.search.AggregationBucket;
import org.eventviewer.api.search.AggregationResult;
import org.eventviewer.api.search.CursorPageable;
import org.eventviewer.api.search.SearchField;
import org.eventviewer.api.search.SearchPage;
import org.eventviewer.api.search.SearchResponse;
import org.eventviewer.api.search.Sort;
import org.eventviewer.api.search.SortDirection;
import org.eventviewer.opensearch.OsAggregationBucket;
import org.eventviewer.opensearch.OsAggregationResult;
import org.eventviewer.opensearch.OsCursorPageable;
import org.eventviewer.opensearch.OsSearchResponse;
import org.eventviewer.model.EventDocument;
import org.eventviewer.read.domain.EventSearchResult;
import org.opensearch.client.opensearch._types.SortOptions;
import org.opensearch.client.opensearch._types.SortOrder;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class OsSearchResponseTranslator {

    public SearchResponse<EventSearchResult> translate(OsSearchResponse<EventDocument> osResponse) {
        List<EventSearchResult> hits = osResponse.hits().stream()
                .map(doc -> new EventSearchResult(doc.eventId(), doc.schemaType(), doc.timestamp()))
                .toList();

        CursorPageable nextPage = null;
        if (osResponse.nextPage() != null) {
            nextPage = translateCursor(osResponse.nextPage());
        }

        Map<String, AggregationResult> aggregations = translateAggregations(osResponse.aggregations());

        return new SearchResponse<>(hits, osResponse.totalHits(), nextPage, aggregations);
    }

    private CursorPageable translateCursor(OsCursorPageable osCursor) {
        List<SortOptions> sortOptions = osCursor.sort();
        String primaryFieldName = sortOptions.get(0).field().field();

        SearchField searchField = Arrays.stream(SearchField.values())
                .filter(sf -> sf.fieldName().equals(primaryFieldName))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "Cannot resolve SearchField for sort field: " + primaryFieldName));

        SortOrder order = sortOptions.get(0).field().order();
        SortDirection direction = (order == SortOrder.Asc) ? SortDirection.ASC : SortDirection.DESC;

        Sort sort = new Sort(searchField, direction);
        SearchPage page = new SearchPage(osCursor.pageNumber(), osCursor.size());

        return new CursorPageable(page, sort, osCursor.searchAfter());
    }

    private Map<String, AggregationResult> translateAggregations(Map<String, OsAggregationResult> osAggs) {
        if (osAggs == null || osAggs.isEmpty()) return Map.of();

        Map<String, AggregationResult> result = new LinkedHashMap<>();
        for (Map.Entry<String, OsAggregationResult> entry : osAggs.entrySet()) {
            result.put(entry.getKey(), translateAggResult(entry.getValue()));
        }
        return result;
    }

    private AggregationResult translateAggResult(OsAggregationResult osResult) {
        List<AggregationBucket> buckets = osResult.buckets().stream()
                .map(this::translateBucket)
                .toList();
        return new AggregationResult(osResult.name(), buckets);
    }

    private AggregationBucket translateBucket(OsAggregationBucket osBucket) {
        Map<String, AggregationResult> subAggs = Map.of();
        if (osBucket.subAggregations() != null && !osBucket.subAggregations().isEmpty()) {
            subAggs = new LinkedHashMap<>();
            for (Map.Entry<String, OsAggregationResult> entry : osBucket.subAggregations().entrySet()) {
                subAggs.put(entry.getKey(), translateAggResult(entry.getValue()));
            }
        }
        return new AggregationBucket(osBucket.key(), osBucket.docCount(), subAggs);
    }
}
