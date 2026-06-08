package org.eventviewer.opensearch.schemamanager;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.eventviewer.opensearch.ClusterSettings;
import org.eventviewer.opensearch.IndexSettings;
import org.eventviewer.opensearch.MigrationData;
import org.eventviewer.opensearch.OsAdminClient;
import org.eventviewer.opensearch.OsDocumentClient;
import org.eventviewer.opensearch.OsException;
import org.eventviewer.opensearch.OsMigration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OsSchemaManagerTest {

    @Mock OsAdminClient adminClient;
    @Mock OsDocumentClient osDocumentClient;

    SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
    }

    // ── helpers ────────────────────────────────────────────────────────────────

    private OsSchemaManager manager(List<OsMigration> migrations) {
        return new OsSchemaManager(adminClient, osDocumentClient, migrations, meterRegistry);
    }

    private OsMigration migration(int version, List<MigrationData> data) {
        return new OsMigration() {
            public int version() { return version; }
            public String description() { return "v" + version; }
            public List<MigrationData> data() { return data; }
        };
    }

    private MigrationData withIndexOnly() {
        IndexSettings settings = new IndexSettings();
        settings.setEntity(String.class);
        MigrationData d = new MigrationData();
        d.setIndexSettings(settings);
        return d;
    }

    private MigrationData withClusterOnly() {
        MigrationData d = new MigrationData();
        d.setClusterSettings(new ClusterSettings());
        return d;
    }

    private MigrationData empty() {
        return new MigrationData();
    }

    // ── Idempotency ────────────────────────────────────────────────────────────

    @Test
    void onElected_calledTwice_onlyExecutesOnce() throws OsException {
        when(adminClient.indexExists(any())).thenReturn(true);
        when(osDocumentClient.get(any(), any())).thenReturn(null);

        OsSchemaManager mgr = manager(List.of(migration(1, List.of(empty()))));
        mgr.onElected();
        mgr.onElected();

        verify(osDocumentClient, times(1)).get(any(), any());
        verify(osDocumentClient, times(1)).save(any());
    }

    // ── Empty migrations ───────────────────────────────────────────────────────

    @Test
    void onElected_noMigrations_makesNoClientCalls() {
        manager(List.of()).onElected();
        verifyNoInteractions(adminClient, osDocumentClient);
    }

    // ── Version selection ──────────────────────────────────────────────────────

    @Test
    void onElected_picksHighestVersionMigration() throws OsException {
        when(adminClient.indexExists(any())).thenReturn(true);
        when(osDocumentClient.get(any(), any())).thenReturn(null);

        manager(List.of(
                migration(1, List.of(empty())),
                migration(3, List.of(empty())),
                migration(2, List.of(empty()))
        )).onElected();

        verify(osDocumentClient).get(eq("3"), any());
        verify(osDocumentClient, never()).get(eq("1"), any());
        verify(osDocumentClient, never()).get(eq("2"), any());
    }

    // ── Version already applied ────────────────────────────────────────────────

    @Test
    void onElected_versionAlreadyApplied_incrementsSkippedCounter() throws OsException {
        when(adminClient.indexExists(any())).thenReturn(true);
        when(osDocumentClient.get(any(), any())).thenReturn(new MigrationDocument());

        manager(List.of(migration(1, List.of(withIndexOnly())))).onElected();

        assertThat(meterRegistry.counter("os.schema.migrations.skipped").count()).isEqualTo(1.0);
        assertThat(meterRegistry.counter("os.schema.migrations.applied").count()).isEqualTo(0.0);
    }

    @Test
    void onElected_versionAlreadyApplied_noWriteCallsMade() throws OsException {
        when(adminClient.indexExists(any())).thenReturn(true);
        when(osDocumentClient.get(any(), any())).thenReturn(new MigrationDocument());

        manager(List.of(migration(1, List.of(withIndexOnly())))).onElected();

        verify(adminClient, never()).createIndex(any());
        verify(adminClient, never()).createTemplate(any());
        verify(osDocumentClient, never()).save(any());
    }

    // ── Template creation ──────────────────────────────────────────────────────

    @Test
    void onElected_newMigration_templateAbsent_createsTemplate() throws OsException {
        when(adminClient.indexExists(any())).thenReturn(true);
        when(osDocumentClient.get(any(), any())).thenReturn(null);
        when(adminClient.templateExists(any())).thenReturn(false);

        manager(List.of(migration(1, List.of(withIndexOnly())))).onElected();

        verify(adminClient).createTemplate(any());
    }

    @Test
    void onElected_newMigration_templatePresent_skipsTemplateCreation() throws OsException {
        when(adminClient.indexExists(any())).thenReturn(true);
        when(osDocumentClient.get(any(), any())).thenReturn(null);
        when(adminClient.templateExists(any())).thenReturn(true);

        manager(List.of(migration(1, List.of(withIndexOnly())))).onElected();

        verify(adminClient, never()).createTemplate(any());
    }

    // ── Index creation ─────────────────────────────────────────────────────────

    @Test
    void onElected_newMigration_indexAbsent_createsDataIndex() throws OsException {
        when(adminClient.indexExists(any())).thenReturn(true);
        when(adminClient.indexExists(String.class)).thenReturn(false);
        when(osDocumentClient.get(any(), any())).thenReturn(null);
        when(adminClient.templateExists(any())).thenReturn(true);

        manager(List.of(migration(1, List.of(withIndexOnly())))).onElected();

        verify(adminClient).createIndex(any());
    }

    @Test
    void onElected_newMigration_indexPresent_skipsIndexCreation() throws OsException {
        when(adminClient.indexExists(any())).thenReturn(true);
        when(osDocumentClient.get(any(), any())).thenReturn(null);
        when(adminClient.templateExists(any())).thenReturn(true);

        manager(List.of(migration(1, List.of(withIndexOnly())))).onElected();

        verify(adminClient, never()).createIndex(any());
    }

    @Test
    void onElected_newMigration_nullIndexSettings_skipsTemplateAndIndexCalls() throws OsException {
        when(adminClient.indexExists(any())).thenReturn(true);
        when(osDocumentClient.get(any(), any())).thenReturn(null);

        manager(List.of(migration(1, List.of(empty())))).onElected();

        verify(adminClient, never()).createIndex(any());
        verify(adminClient, never()).createTemplate(any());
        verify(adminClient, never()).templateExists(any());
    }

    // ── Cluster settings ───────────────────────────────────────────────────────

    @Test
    void onElected_newMigration_withClusterSettings_callsClusterSettings() throws OsException {
        when(adminClient.indexExists(any())).thenReturn(true);
        when(osDocumentClient.get(any(), any())).thenReturn(null);

        manager(List.of(migration(1, List.of(withClusterOnly())))).onElected();

        verify(adminClient).clusterSettings(any());
    }

    @Test
    void onElected_newMigration_nullClusterSettings_skipsClusterSettingsCall() throws OsException {
        when(adminClient.indexExists(any())).thenReturn(true);
        when(osDocumentClient.get(any(), any())).thenReturn(null);
        when(adminClient.templateExists(any())).thenReturn(true);

        manager(List.of(migration(1, List.of(withIndexOnly())))).onElected();

        verify(adminClient, never()).clusterSettings(any());
    }

    // ── Multiple data items ────────────────────────────────────────────────────

    @Test
    void onElected_multipleDataItems_appliedCounterIncrementedPerItem() throws OsException {
        when(adminClient.indexExists(any())).thenReturn(true);
        when(osDocumentClient.get(any(), any())).thenReturn(null);

        manager(List.of(migration(1, List.of(empty(), empty(), empty())))).onElected();

        assertThat(meterRegistry.counter("os.schema.migrations.applied").count()).isEqualTo(3.0);
    }

    @Test
    void onElected_multipleDataItems_indexMigrationRecordCalledOnce() throws OsException {
        when(adminClient.indexExists(any())).thenReturn(true);
        when(osDocumentClient.get(any(), any())).thenReturn(null);

        manager(List.of(migration(1, List.of(empty(), empty())))).onElected();

        verify(osDocumentClient, times(1)).save(any());
    }

    // ── Exception handling ─────────────────────────────────────────────────────

    @Test
    void onElected_adminClientThrows_doesNotPropagateException() throws OsException {
        when(adminClient.indexExists(any())).thenThrow(new OsException("boom"));

        assertThatCode(() -> manager(List.of(migration(1, List.of(withIndexOnly())))).onElected())
                .doesNotThrowAnyException();
    }

    @Test
    void onElected_documentClientThrows_doesNotPropagateException() throws OsException {
        when(adminClient.indexExists(any())).thenReturn(true);
        when(osDocumentClient.get(any(), any())).thenThrow(new OsException("boom"));

        assertThatCode(() -> manager(List.of(migration(1, List.of(withIndexOnly())))).onElected())
                .doesNotThrowAnyException();
    }

    // ── ensureMigrationsIndex (protected — same package) ──────────────────────

    @Test
    void ensureMigrationsIndex_indexNotExists_createsIndexWithCorrectSettings() throws OsException {
        when(adminClient.indexExists(MigrationDocument.class)).thenReturn(false);
        ArgumentCaptor<IndexSettings> captor = ArgumentCaptor.forClass(IndexSettings.class);

        manager(List.of()).ensureMigrationsIndex();

        verify(adminClient).createIndex(captor.capture());
        IndexSettings s = captor.getValue();
        assertThat(s.getShards()).isEqualTo(1);
        assertThat(s.getReplicas()).isEqualTo(1);
        assertThat(s.getRefreshIntervalSecs()).isEqualTo(10);
        assertThat(s.getEntity()).isEqualTo(MigrationDocument.class);
        assertThat(s.getTypeMapping()).isNotNull();
    }

    @Test
    void ensureMigrationsIndex_indexExists_doesNotCreateIndex() throws OsException {
        when(adminClient.indexExists(MigrationDocument.class)).thenReturn(true);

        manager(List.of()).ensureMigrationsIndex();

        verify(adminClient, never()).createIndex(any());
    }

    // ── versionExists (protected — same package) ───────────────────────────────

    @Test
    void versionExists_documentFound_returnsTrue() throws OsException {
        when(osDocumentClient.get("3", MigrationDocument.class)).thenReturn(new MigrationDocument());

        assertThat(manager(List.of()).versionExists(3)).isTrue();
    }

    @Test
    void versionExists_documentNotFound_returnsFalse() throws OsException {
        when(osDocumentClient.get("3", MigrationDocument.class)).thenReturn(null);

        assertThat(manager(List.of()).versionExists(3)).isFalse();
    }

    // ── indexMigrationRecord (protected — same package) ───────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void indexMigrationRecord_savesDocumentWithCorrectVersion() throws OsException {
        ArgumentCaptor<Collection> captor = ArgumentCaptor.forClass(Collection.class);

        manager(List.of()).indexMigrationRecord(migration(7, List.of()));

        verify(osDocumentClient).save(captor.capture());
        MigrationDocument doc = (MigrationDocument) captor.getValue().iterator().next();
        assertThat(doc.getVersion()).isEqualTo("7");
    }

    @Test
    @SuppressWarnings("unchecked")
    void indexMigrationRecord_savesDocumentWithTimestamp() throws OsException {
        ArgumentCaptor<Collection> captor = ArgumentCaptor.forClass(Collection.class);
        java.time.ZonedDateTime before = java.time.ZonedDateTime.now().minusSeconds(1);

        manager(List.of()).indexMigrationRecord(migration(1, List.of()));

        java.time.ZonedDateTime after = java.time.ZonedDateTime.now().plusSeconds(1);
        verify(osDocumentClient).save(captor.capture());
        MigrationDocument doc = (MigrationDocument) captor.getValue().iterator().next();
        assertThat(doc.getTimestamp()).isBetween(before, after);
    }
}
