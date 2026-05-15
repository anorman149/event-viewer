package org.eventviewer.opensearch;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.eventviewer.opensearch.autoconfigure.OsProperties;
import org.eventviewer.opensearch.autoconfigure.OsSchemaManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.client.opensearch.OpenSearchClient;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
class OsSchemaManagerTest {

    @Mock private OsAdminClient adminClient;
    @Mock private OpenSearchClient openSearchClient;

    private SimpleMeterRegistry meterRegistry;
    private OsProperties properties;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        properties = new OsProperties();
        properties.getMigration().setIndexName("test-migrations");
    }

    @Test
    void onLeader_selectsLatestMigrationOnly_callsItsDataItems() throws Exception {
        when(adminClient.indexExists("test-migrations")).thenReturn(true);

        MigrationData data3 = new MigrationData();
        data3.setClusterSettings(new ClusterSettings());

        OsMigration m1 = stubMigration(1, "m1", List.of(new MigrationData()));
        OsMigration m2 = stubMigration(2, "m2", List.of(new MigrationData()));
        OsMigration m3 = stubMigration(3, "m3", List.of(data3));

        var manager = new TestableOsSchemaManager(0, List.of(m3, m1, m2));
        manager.onLeader();

        // Only migration3 is the latest; its data iterates exactly once (ClusterSettings)
        verify(adminClient).clusterSettings(any());
        // migration1 and migration2 data never iterated
        verify(m1, never()).data();
        verify(m2, never()).data();

        assertThat(meterRegistry.counter("os.schema.migrations.applied").count()).isEqualTo(1.0);
    }

    @Test
    void onLeader_migrationsAlreadyApplied_skipsAndIncrementsSkippedCounter() throws Exception {
        when(adminClient.indexExists("test-migrations")).thenReturn(true);

        OsMigration m3 = stubMigration(3, "m3", List.of(new MigrationData()));

        var manager = new TestableOsSchemaManager(3, List.of(m3));
        manager.onLeader();

        verify(adminClient, never()).createIndex(any());
        verify(adminClient, never()).createTemplate(any());
        verify(adminClient, never()).clusterSettings(any());

        assertThat(meterRegistry.counter("os.schema.migrations.skipped").count()).isEqualTo(1.0);
    }

    @Test
    void onLeaderLoss_isNoOp() throws Exception {
        var manager = new TestableOsSchemaManager(0, List.of(stubMigration(1, "m1", List.of())));
        manager.onLeaderLoss();

        verify(adminClient, never()).createIndex(any());
        verify(adminClient, never()).clusterSettings(any());
    }

    @Test
    void follower_onLeaderNeverCalled_noAdminClientInteractions() throws Exception {
        OsMigration m1 = stubMigration(1, "m1", List.of());
        OsMigration m2 = stubMigration(2, "m2", List.of());
        OsMigration m3 = stubMigration(3, "m3", List.of());
        new TestableOsSchemaManager(0, List.of(m1, m2, m3));

        verify(adminClient, never()).indexExists(any(String.class));
        verify(adminClient, never()).createIndex(any());
        verify(adminClient, never()).clusterSettings(any());
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private OsMigration stubMigration(int order, String name, List<MigrationData> data) {
        OsMigration m = org.mockito.Mockito.mock(OsMigration.class);
        when(m.order()).thenReturn(order);
        when(m.name()).thenReturn(name);
        when(m.data()).thenReturn(data);
        return m;
    }

    private class TestableOsSchemaManager extends OsSchemaManager {

        private final int fakeCurrentVersion;

        TestableOsSchemaManager(int fakeCurrentVersion, List<OsMigration> migrations) {
            super(adminClient, openSearchClient, properties, migrations, meterRegistry);
            this.fakeCurrentVersion = fakeCurrentVersion;
        }

        @Override
        protected void ensureMigrationsIndex(String indexName) throws OsException {}

        @Override
        protected int queryCurrentVersion(String migrationsIndex) throws IOException {
            return fakeCurrentVersion;
        }

        @Override
        protected void indexMigrationRecord(String migrationsIndex, OsMigration migration) throws IOException {}
    }
}
