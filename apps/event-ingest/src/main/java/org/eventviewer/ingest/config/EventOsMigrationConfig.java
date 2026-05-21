package org.eventviewer.ingest.config;

import org.eventviewer.ingest.migration.EventStorageMigration;
import org.eventviewer.ingest.migration.dev.DevEventStorageMigration;
import org.eventviewer.ingest.migration.local.LocalEventStorageMigration;
import org.eventviewer.ingest.migration.prod.ProdEventStorageMigration;
import org.eventviewer.ingest.migration.staging.StagingEventStorageMigration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
public class EventOsMigrationConfig {
    @Bean
    @Profile({"local", "default", "test"})
    public EventStorageMigration localEventStorageMigration() {
        return new LocalEventStorageMigration();
    }

    @Bean
    @Profile("dev")
    public EventStorageMigration devEventStorageMigration() {
        return new DevEventStorageMigration();
    }

    @Bean
    @Profile("staging")
    public EventStorageMigration stagingEventStorageMigration() {
        return new StagingEventStorageMigration();
    }

    @Bean
    @Profile("prod")
    public EventStorageMigration prodEventStorageMigration() {
        return new ProdEventStorageMigration();
    }
}
