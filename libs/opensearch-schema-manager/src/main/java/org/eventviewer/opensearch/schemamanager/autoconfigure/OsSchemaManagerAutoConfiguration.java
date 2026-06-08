package org.eventviewer.opensearch.schemamanager.autoconfigure;

import io.micrometer.core.instrument.MeterRegistry;
import org.eventviewer.opensearch.OsAdminClient;
import org.eventviewer.opensearch.OsDocumentClient;
import org.eventviewer.opensearch.OsMigration;
import org.eventviewer.opensearch.schemamanager.OsSchemaManager;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

import java.util.List;

@AutoConfiguration
@ConditionalOnBean(OsAdminClient.class)
public class OsSchemaManagerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public OsSchemaManager osSchemaManager(OsAdminClient adminClient,
                                           OsDocumentClient osDocumentClient,
                                           List<OsMigration> migrations,
                                           MeterRegistry meterRegistry) {
        return new OsSchemaManager(adminClient, osDocumentClient, migrations, meterRegistry);
    }
}
