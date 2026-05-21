package org.eventviewer.ingest.migration;

import org.eventviewer.ingest.domain.EventDocument;
import org.eventviewer.opensearch.*;
import org.opensearch.client.opensearch._types.mapping.DynamicMapping;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;

import java.time.Duration;
import java.util.List;

public abstract class EventStorageMigration implements OsMigration {

    protected abstract int shards();
    protected abstract int replicas();
    protected abstract int refreshIntervalSecs();

    @Override
    public int version() {
        return 1;
    }

    @Override
    public String description() {
        return "Initial Event Storage Migration";
    }

    @Override
    public List<MigrationData> data() {
        ClusterSettings clusterSettings = new ClusterSettings();
        clusterSettings.setSearchMaxBuckets(10000);
        clusterSettings.setSearchCancelerAfter(Duration.ofSeconds(30));

        TypeMapping mapping = TypeMapping.of(m -> m
                .dynamic(DynamicMapping.False)
                .dateDetection(false)
                .properties("eventId",     p -> p.keyword(k -> k.norms(false)))
                .properties("schemaType",  p -> p.integer(i -> i))
                .properties("timestamp",   p -> p.date(d -> d))
                .properties("s3FileName",  p -> p.keyword(k -> k.docValues(false).index(false)))
                .properties("podId",       p -> p.keyword(k -> k.docValues(false).index(false)))
                .properties("batchOffset", p -> p.long_(l -> l.docValues(false).index(false)))
                .properties("batchLength", p -> p.long_(l -> l.docValues(false).index(false)))
                .properties("ruleResults", p -> p.keyword(k -> k.docValues(false)))
        );

        IndexSettings indexSettings = new IndexSettings();
        indexSettings.setEntity(EventDocument.class);
        indexSettings.setShards(shards());
        indexSettings.setReplicas(replicas());
        indexSettings.setRefreshIntervalSecs(refreshIntervalSecs());
        indexSettings.setTypeMapping(mapping);

        MigrationData data = new MigrationData();
        data.setIndexSettings(indexSettings);
        data.setClusterSettings(clusterSettings);
        return List.of(data);
    }
}
