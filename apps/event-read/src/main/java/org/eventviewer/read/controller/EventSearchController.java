package org.eventviewer.read.controller;

import io.micrometer.core.annotation.Timed;
import org.eventviewer.api.search.AggregationRequest;
import org.eventviewer.api.search.SearchRequest;
import org.eventviewer.api.search.SearchResponse;
import org.eventviewer.api.search.Sort;
import org.eventviewer.read.domain.EventSearchResult;
import org.eventviewer.read.service.EventSearchService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/search/v1")
public class EventSearchController {

    private final EventSearchService eventSearchService;

    public EventSearchController(EventSearchService eventSearchService) {
        this.eventSearchService = eventSearchService;
    }

    @PostMapping("/events")
    @Timed(value = "event.read.controller.search", histogram = true)
    public ResponseEntity<SearchResponse<EventSearchResult>> search(
            @Validated @RequestBody SearchRequest request) {
        Sort sort = request.cursorPageable().sort();
        List<Object> searchAfter = request.cursorPageable().searchAfter();
        int page = request.cursorPageable().page() != null ? request.cursorPageable().page().page() : 0;
        int size = request.cursorPageable().page() != null ? request.cursorPageable().page().size() : 20;
        List<AggregationRequest> aggregations = request.aggregations() != null
                ? request.aggregations() : List.of();

        SearchResponse<EventSearchResult> result = eventSearchService.search(
                request.expression(), sort, searchAfter, page, size, aggregations);

        return ResponseEntity.status(HttpStatus.OK).body(result);
    }
}
