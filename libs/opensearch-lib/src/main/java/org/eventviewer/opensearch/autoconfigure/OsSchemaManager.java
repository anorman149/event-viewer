package org.eventviewer.opensearch.autoconfigure;

import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.eventviewer.leader.LeaderListener;
import org.eventviewer.opensearch.*;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class OsSchemaManager implements LeaderListener {
    private static final Logger log = LoggerFactory.getLogger(OsSchemaManager.class);

    private final OsAdminClient adminClient;
    private final OsDocumentClient osDocumentClient;
    private final List<OsMigration> migrations;
    private final Counter migrationsApplied;
    private final Counter migrationsSkipped;

    private final AtomicBoolean executed = new AtomicBoolean(false);

    public OsSchemaManager(OsAdminClient adminClient,
                           OsDocumentClient osDocumentClient,
                           List<OsMigration> migrations,
                           MeterRegistry meterRegistry) {
        this.adminClient = adminClient;
        this.osDocumentClient = osDocumentClient;
        this.migrations = migrations;
        this.migrationsApplied = Counter.builder("os.schema.migrations.applied")
                .description("Migrations applied in this startup")
                .register(meterRegistry);
        this.migrationsSkipped = Counter.builder("os.schema.migrations.skipped")
                .description("Migrations skipped (already applied)")
                .register(meterRegistry);
    }

    @Override
    @Timed(value = "os.schema.manager.on.leader", histogram = true)
    public void onElected() {
        if(!executed.compareAndSet(false, true)) {
            return;
        }

        try {
            List<OsMigration> sorted = migrations.stream()
                    .sorted(Comparator.comparingInt(OsMigration::version).reversed())
                    .toList();

            if (sorted.isEmpty()) {
                log.info("No OsMigration beans registered; skipping schema management.");
                return;
            }

            OsMigration latest = sorted.stream().findFirst().orElseThrow(() -> new RuntimeException("No Search Migration Found for Search Migration Manager"));

            log.info("Found Latest Search Migration {} For Version {}", latest.getClass().getSimpleName(), latest.version());

            ensureMigrationsIndex();

            boolean versionExists = versionExists(latest.version());
            if(versionExists) {
                log.info("Latest migration version={} already applied; skipping.", latest.version());
                migrationsSkipped.increment();
                return;
            }

            log.info("Applying migration version={} ", latest.version());
            for (MigrationData data : latest.data()) {

                if (data.getIndexSettings() != null) {
                    if (!adminClient.templateExists(data.getIndexSettings().getEntity())) {
                        adminClient.createTemplate(data.getIndexSettings());
                    }
                    if (!adminClient.indexExists(data.getIndexSettings().getEntity())) {
                        adminClient.createIndex(data.getIndexSettings());
                    }
                }
                if (data.getClusterSettings() != null) {
                    adminClient.clusterSettings(data.getClusterSettings());
                }

                migrationsApplied.increment();
                log.info("Migration version={} applied successfully.", latest.version());
            }

            indexMigrationRecord(latest);
        } catch (Exception e) {
            log.error("Schema migration failed: {}", e.getMessage(), e);
        }
    }

    @Timed(value = "os.schema.manager.ensure.migrations.index", histogram = true)
    protected void ensureMigrationsIndex() throws OsException {
        if (!adminClient.indexExists(MigrationDocument.class)) {
            TypeMapping mappings =
                    TypeMapping.builder()
                            .properties("version", p -> p.keyword(builder -> builder.index(true).docValues(true)))
                            .properties("timestamp", p -> p.date(d -> d.format("strict_date_time").index(true)))
                            .build();

            IndexSettings settings = new IndexSettings();
            settings.setShards(1);
            settings.setReplicas(1);
            settings.setTypeMapping(mappings);
            settings.setEntity(MigrationDocument.class);
            settings.setRefreshIntervalSecs(10);

            adminClient.createIndex(settings);
        }
    }

    @Timed(value = "os.schema.manager.query.current.version", histogram = true)
    protected boolean versionExists(int version) throws OsException {
        return osDocumentClient.get(String.valueOf(version), MigrationDocument.class) != null;
    }

    @Timed(value = "os.schema.manager.index.migration.record", histogram = true)
    protected void indexMigrationRecord(OsMigration migration) throws OsException {
        MigrationDocument doc = new MigrationDocument(
                String.valueOf(migration.version()),
                ZonedDateTime.now()
        );

        osDocumentClient.save(List.of(doc));
    }
}
