package org.eventviewer.opensearch.autoconfigure;

import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.eventviewer.leader.LeaderListener;
import org.eventviewer.opensearch.MigrationData;
import org.eventviewer.opensearch.OsAdminClient;
import org.eventviewer.opensearch.OsException;
import org.eventviewer.opensearch.OsMigration;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch.core.search.Hit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;

public class OsSchemaManager implements LeaderListener {

    private static final Logger log = LoggerFactory.getLogger(OsSchemaManager.class);

    private final OsAdminClient adminClient;
    private final OpenSearchClient openSearchClient;
    private final OsProperties properties;
    private final List<OsMigration> migrations;
    private final Counter migrationsApplied;
    private final Counter migrationsSkipped;

    public OsSchemaManager(OsAdminClient adminClient,
                            OpenSearchClient openSearchClient,
                            OsProperties properties,
                            List<OsMigration> migrations,
                            MeterRegistry meterRegistry) {
        this.adminClient = adminClient;
        this.openSearchClient = openSearchClient;
        this.properties = properties;
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
    public void onLeader() {
        String migrationsIndex = properties.getMigration().getIndexName();
        try {
            ensureMigrationsIndex(migrationsIndex);

            int currentVersion = queryCurrentVersion(migrationsIndex);

            List<OsMigration> sorted = migrations.stream()
                    .sorted(Comparator.comparingInt(OsMigration::order))
                    .toList();

            if (sorted.isEmpty()) {
                log.debug("No OsMigration beans registered; skipping schema management.");
                return;
            }

            OsMigration latest = sorted.get(sorted.size() - 1);

            if (latest.order() <= currentVersion) {
                log.debug("Latest migration order={} already applied (currentVersion={}); skipping.",
                        latest.order(), currentVersion);
                migrationsSkipped.increment();
                return;
            }

            log.info("Applying migration order={} name={}", latest.order(), latest.name());
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
            }

            indexMigrationRecord(migrationsIndex, latest);
            migrationsApplied.increment();
            log.info("Migration order={} name={} applied successfully.", latest.order(), latest.name());

        } catch (OsException | IOException e) {
            log.error("Schema migration failed: {}", e.getMessage(), e);
        }
    }

    @Override
    @Timed(value = "os.schema.manager.on.leader.loss", histogram = true)
    public void onLeaderLoss() {
        // no-op: migrations are one-time startup actions
    }

    @Timed(value = "os.schema.manager.ensure.migrations.index", histogram = true)
    protected void ensureMigrationsIndex(String indexName) throws OsException {
        if (!adminClient.indexExists(indexName)) {
            try {
                openSearchClient.indices().create(req -> req
                        .index(indexName)
                        .settings(s -> s.numberOfShards(1).numberOfReplicas(1)));
                log.debug("Created migrations tracking index: {}", indexName);
            } catch (IOException e) {
                throw new OsException("Failed to create migrations tracking index: " + indexName, e);
            }
        }
    }

    @Timed(value = "os.schema.manager.query.current.version", histogram = true)
    protected int queryCurrentVersion(String migrationsIndex) throws IOException {
        var response = openSearchClient.search(req -> req
                .index(migrationsIndex)
                .size(1)
                .sort(s -> s.field(f -> f.field("version").order(SortOrder.Desc))),
                MigrationDocument.class);

        if (response == null || response.hits() == null) {
            return 0;
        }
        List<Hit<MigrationDocument>> hits = response.hits().hits();
        if (hits == null || hits.isEmpty()) {
            return 0;
        }
        Hit<MigrationDocument> topHit = hits.get(0);
        if (topHit == null || topHit.source() == null) {
            return 0;
        }
        return topHit.source().getVersion();
    }

    @Timed(value = "os.schema.manager.index.migration.record", histogram = true)
    protected void indexMigrationRecord(String migrationsIndex, OsMigration migration) throws IOException {
        MigrationDocument doc = new MigrationDocument(
                migration.order(),
                migration.name(),
                Instant.now().toString()
        );
        openSearchClient.index(req -> req
                .index(migrationsIndex)
                .id(String.valueOf(migration.order()))
                .document(doc));
    }
}
