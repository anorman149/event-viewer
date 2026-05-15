package org.eventviewer.opensearch.itest;

import org.eventviewer.opensearch.IndexSettings;
import org.eventviewer.opensearch.MigrationData;
import org.eventviewer.opensearch.OsMigration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class TestMigrationConfig {

    @Bean
    public OsMigration testEventsMigration() {
        IndexSettings settings = new IndexSettings();
        settings.setEntity(TestEventDocument.class);
        settings.setShards(1);
        settings.setReplicas(0);

        MigrationData data = new MigrationData();
        data.setIndexSettings(settings);

        return new OsMigration() {
            @Override public int order() { return 1; }
            @Override public String name() { return "001_create_it_test_events"; }
            @Override public List<MigrationData> data() { return List.of(data); }
        };
    }
}
