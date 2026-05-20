package org.eventviewer.ingest.migration;

import org.eventviewer.ingest.domain.EventDocument;
import org.eventviewer.opensearch.IlmPolicySettings;
import org.eventviewer.opensearch.IndexSettings;
import org.eventviewer.opensearch.MigrationData;
import org.eventviewer.opensearch.OsMigration;
import org.opensearch.client.opensearch._types.mapping.DynamicMapping;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;

import java.time.Duration;
import java.util.List;

public abstract class EventStorageMigration implements OsMigration {

    protected abstract int shards();
    protected abstract int replicas();
    protected abstract int refreshIntervalSecs();

    @Override
    public int order() {
        return 2;
    }

    @Override
    public String name() {
        return "002_events_template_ilm_index";
    }

    @Override
    public List<MigrationData> data() {
        IlmPolicySettings ilm = new IlmPolicySettings();
        ilm.setPolicyName("events-ilm-policy");
        ilm.setRolloverMaxSizeGb(130);
        ilm.setRolloverMaxAge(Duration.ofHours(12));
        ilm.setWarmRetention(Duration.ofDays(4));

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
        indexSettings.setTemplatePattern("events-*");
        indexSettings.setShards(shards());
        indexSettings.setReplicas(replicas());
        indexSettings.setRefreshIntervalSecs(refreshIntervalSecs());
        indexSettings.setLifecycleName("events-ilm-policy");
        indexSettings.setTypeMapping(mapping);

        MigrationData data = new MigrationData();
        data.setIlmPolicySettings(ilm);
        data.setIndexSettings(indexSettings);
        return List.of(data);
    }
}
