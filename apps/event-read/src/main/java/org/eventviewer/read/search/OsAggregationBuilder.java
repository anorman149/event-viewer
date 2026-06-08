package org.eventviewer.read.search;

import org.eventviewer.api.search.AggregationRequest;
import org.opensearch.client.opensearch._types.aggregations.Aggregation;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class OsAggregationBuilder {

    public Map<String, Aggregation> build(List<AggregationRequest> requests) {
        if (requests == null || requests.isEmpty()) return Map.of();

        Map<String, Aggregation> result = new LinkedHashMap<>();
        for (AggregationRequest req : requests) {
            result.put(req.name(), buildOne(req));
        }
        return result;
    }

    private Aggregation buildOne(AggregationRequest req) {
        String fieldName = req.field().fieldName();
        return switch (req.type()) {
            case TERMS -> Aggregation.of(a -> a.terms(t -> t.field(fieldName)));
            case DATE_HISTOGRAM -> {
                String interval = req.interval() != null ? req.interval() : "1h";
                yield Aggregation.of(a -> a.dateHistogram(dh -> dh
                        .field(fieldName)
                        .fixedInterval(fi -> fi.time(interval))));
            }
            case VALUE_COUNT -> Aggregation.of(a -> a.valueCount(vc -> vc.field(fieldName)));
        };
    }
}
