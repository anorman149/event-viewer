package org.eventviewer.read.search;

import org.eventviewer.api.search.Sort;
import org.eventviewer.api.search.SortDirection;
import org.opensearch.client.opensearch._types.SortOptions;
import org.opensearch.client.opensearch._types.SortOrder;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class OsSortBuilder {

    public List<SortOptions> build(Sort sort) {
        SortOrder order = sort.direction() == SortDirection.ASC ? SortOrder.Asc : SortOrder.Desc;

        SortOptions primary = SortOptions.of(s -> s.field(f -> f
                .field(sort.field().fieldName())
                .order(order)));

        SortOptions secondary = SortOptions.of(s -> s.field(f -> f
                .field("_id")
                .order(order)));

        return List.of(primary, secondary);
    }
}
