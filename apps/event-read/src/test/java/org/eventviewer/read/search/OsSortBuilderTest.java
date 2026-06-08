package org.eventviewer.read.search;

import org.eventviewer.api.search.SearchField;
import org.eventviewer.api.search.Sort;
import org.eventviewer.api.search.SortDirection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch._types.SortOptions;
import org.opensearch.client.opensearch._types.SortOrder;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OsSortBuilderTest {

    private OsSortBuilder builder;

    @BeforeEach
    void setUp() {
        builder = new OsSortBuilder();
    }

    @Test
    void build_ascSort_primaryFieldAndDirectionCorrect() {
        List<SortOptions> sort = builder.build(new Sort(SearchField.TIMESTAMP, SortDirection.ASC));

        assertThat(sort).hasSize(2);
        assertThat(sort.get(0).field().field()).isEqualTo(SearchField.TIMESTAMP.fieldName());
        assertThat(sort.get(0).field().order()).isEqualTo(SortOrder.Asc);
    }

    @Test
    void build_descSort_primaryFieldAndDirectionCorrect() {
        List<SortOptions> sort = builder.build(new Sort(SearchField.SCHEMA_TYPE, SortDirection.DESC));

        assertThat(sort.get(0).field().field()).isEqualTo(SearchField.SCHEMA_TYPE.fieldName());
        assertThat(sort.get(0).field().order()).isEqualTo(SortOrder.Desc);
    }

    @Test
    void build_alwaysAppendsSecondaryIdSort() {
        List<SortOptions> sort = builder.build(new Sort(SearchField.TIMESTAMP, SortDirection.ASC));

        assertThat(sort).hasSize(2);
        assertThat(sort.get(1).field().field()).isEqualTo("_id");
    }

    @Test
    void build_secondarySortMatchesPrimaryDirection() {
        List<SortOptions> sortAsc = builder.build(new Sort(SearchField.TIMESTAMP, SortDirection.ASC));
        assertThat(sortAsc.get(1).field().order()).isEqualTo(SortOrder.Asc);

        List<SortOptions> sortDesc = builder.build(new Sort(SearchField.TIMESTAMP, SortDirection.DESC));
        assertThat(sortDesc.get(1).field().order()).isEqualTo(SortOrder.Desc);
    }
}
