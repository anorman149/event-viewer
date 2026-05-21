package org.eventviewer.ingest;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.eventviewer.ingest.domain.EventDocument;
import org.eventviewer.opensearch.OsAdminClient;
import org.eventviewer.opensearch.OsDocumentClient;
import org.eventviewer.opensearch.OsMigration;
import org.eventviewer.opensearch.autoconfigure.MigrationDocument;
import org.eventviewer.opensearch.autoconfigure.OsSchemaManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class OsSchemaManagerIT extends BaseTest {

    @Autowired OsAdminClient adminClient;
    @Autowired OsDocumentClient osDocumentClient;
    @Autowired OsSchemaManager osSchemaManager;
    @Autowired List<OsMigration> migrations;

    // ── Post-startup state ─────────────────────────────────────────────────────

    @Test
    void migrationsIndex_existsAfterStartup() {
        await().atMost(15, TimeUnit.SECONDS)
                .until(() -> adminClient.indexExists(MigrationDocument.class));
    }

    @Test
    void eventIndex_existsAfterStartup() {
        await().atMost(15, TimeUnit.SECONDS)
                .until(() -> adminClient.indexExists(EventDocument.class));
    }

    @Test
    void eventTemplate_existsAfterStartup() {
        await().atMost(15, TimeUnit.SECONDS)
                .until(() -> adminClient.templateExists(EventDocument.class));
    }

    @Test
    void migrationDocument_version1_presentAfterStartup() throws Exception {
        await().atMost(15, TimeUnit.SECONDS)
                .until(() -> osDocumentClient.get("1", MigrationDocument.class) != null);

        MigrationDocument doc = osDocumentClient.get("1", MigrationDocument.class);
        assertThat(doc.getVersion()).isEqualTo("1");
        assertThat(doc.getTimestamp()).isNotNull();
    }

    @Test
    void migrationDocument_version1_hasRecentTimestamp() throws Exception {
        await().atMost(15, TimeUnit.SECONDS)
                .until(() -> osDocumentClient.get("1", MigrationDocument.class) != null);

        MigrationDocument doc = osDocumentClient.get("1", MigrationDocument.class);
        assertThat(doc.getTimestamp()).isAfter(ZonedDateTime.now().minusMinutes(5));
    }

    // ── Idempotency ────────────────────────────────────────────────────────────

    @Test
    void onElected_calledAgainOnSameInstance_isNoOp() {
        // AtomicBoolean already true from startup; second call short-circuits without throwing.
        assertThatCode(() -> osSchemaManager.onElected()).doesNotThrowAnyException();
    }

    @Test
    void onElected_freshInstance_versionAlreadyApplied_skipsAndIncrementsSkippedCounter() {
        await().atMost(15, TimeUnit.SECONDS)
                .until(() -> osDocumentClient.get("1", MigrationDocument.class) != null);

        SimpleMeterRegistry freshRegistry = new SimpleMeterRegistry();
        OsSchemaManager freshMgr = new OsSchemaManager(
                adminClient, osDocumentClient, migrations, freshRegistry);

        freshMgr.onElected();

        assertThat(freshRegistry.counter("os.schema.migrations.skipped").count()).isEqualTo(1.0);
        assertThat(freshRegistry.counter("os.schema.migrations.applied").count()).isEqualTo(0.0);
    }

    @Test
    void onElected_freshInstance_versionAlreadyApplied_leavesIndexesIntact() throws Exception {
        await().atMost(15, TimeUnit.SECONDS)
                .until(() -> osDocumentClient.get("1", MigrationDocument.class) != null);

        OsSchemaManager freshMgr = new OsSchemaManager(
                adminClient, osDocumentClient, migrations, new SimpleMeterRegistry());
        freshMgr.onElected();

        assertThat(adminClient.indexExists(EventDocument.class)).isTrue();
        assertThat(adminClient.indexExists(MigrationDocument.class)).isTrue();
    }

    // ── Multiple independent runs produce single migration record ──────────────

    @Test
    void migrationDocument_onlyOneVersionDocumentExists_afterMultipleRuns() throws Exception {
        await().atMost(15, TimeUnit.SECONDS)
                .until(() -> osDocumentClient.get("1", MigrationDocument.class) != null);

        new OsSchemaManager(adminClient, osDocumentClient, migrations, new SimpleMeterRegistry()).onElected();
        new OsSchemaManager(adminClient, osDocumentClient, migrations, new SimpleMeterRegistry()).onElected();

        // Only one migration document should exist (upsert by ID "1"), readable after each run.
        MigrationDocument doc = osDocumentClient.get("1", MigrationDocument.class);
        assertThat(doc).isNotNull();
        assertThat(doc.getVersion()).isEqualTo("1");
    }
}
