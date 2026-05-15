package org.eventviewer.opensearch.itest;

import org.eventviewer.opensearch.OsAdminClient;
import org.eventviewer.opensearch.OsDocumentClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.junit.jupiter.api.Assertions.fail;

class OsSchemaManagerIT {

    private static final String MIGRATIONS_INDEX = "it-test-migrations";
    private static final int MIGRATION_STEPS = 1;

    static ConfigurableApplicationContext ctx1;
    static ConfigurableApplicationContext ctx2;

    @BeforeAll
    static void startContexts() throws Exception {
        cleanupState();

        ctx1 = new SpringApplicationBuilder(TestApplication.class)
                .web(WebApplicationType.NONE)
                .run();
        ctx2 = new SpringApplicationBuilder(TestApplication.class)
                .web(WebApplicationType.NONE)
                .run();

        waitForMigrations(ctx1.getBean(OpenSearchClient.class), MIGRATION_STEPS);
    }

    @AfterAll
    static void stopContexts() {
        if (ctx1 != null && ctx1.isActive()) ctx1.close();
        if (ctx2 != null && ctx2.isActive()) ctx2.close();
    }

    @Test
    void leaderAppliesExactlyOneMigrationDocument() throws Exception {
        long count = countMigrationDocs(ctx1.getBean(OpenSearchClient.class));
        assertThat(count).isEqualTo(MIGRATION_STEPS);
    }

    @Test
    void migrationIsNotAppliedTwice() throws Exception {
        // Both contexts share the same OpenSearch; only the leader runs migrations.
        // Run count must equal the defined number of migration steps, no duplicates.
        long count = countMigrationDocs(ctx1.getBean(OpenSearchClient.class));
        assertThat(count).isEqualTo(MIGRATION_STEPS);
    }

    @Test
    void templateIsVisibleInOpenSearch() throws Exception {
        OpenSearchClient client = ctx1.getBean(OpenSearchClient.class);
        boolean templateExists = client.indices()
                .existsIndexTemplate(req -> req.name("it-test-events-template"))
                .value();
        assertThat(templateExists).isTrue();
    }

    @Test
    void indexIsCreatedWithAliases() throws Exception {
        OsAdminClient adminClient = ctx1.getBean(OsAdminClient.class);
        boolean indexExists = adminClient.indexExists(TestEventDocument.class);
        assertThat(indexExists).isTrue();
    }

    @Test
    void idempotency_restartDoesNotApplyMigrationAgain() throws Exception {
        // Stop both contexts
        ctx1.close();
        ctx2.close();

        // Restart — migrations index already has the tracking document
        ConfigurableApplicationContext rCtx1 = new SpringApplicationBuilder(TestApplication.class)
                .web(WebApplicationType.NONE)
                .run();
        ConfigurableApplicationContext rCtx2 = new SpringApplicationBuilder(TestApplication.class)
                .web(WebApplicationType.NONE)
                .run();

        try {
            // Wait long enough for the leader election to fire and attempt migrations
            Thread.sleep(3000);

            // Document count must still be MIGRATION_STEPS — not doubled
            long count = countMigrationDocs(rCtx1.getBean(OpenSearchClient.class));
            assertThat(count).isEqualTo(MIGRATION_STEPS);
        } finally {
            rCtx1.close();
            rCtx2.close();
            // Restore contexts for any remaining tests (they were already closed above)
            ctx1 = new SpringApplicationBuilder(TestApplication.class)
                    .web(WebApplicationType.NONE)
                    .run();
            ctx2 = new SpringApplicationBuilder(TestApplication.class)
                    .web(WebApplicationType.NONE)
                    .run();
        }
    }

    @Test
    void save_documentsAreIndexedUnderWriteAlias() throws Exception {
        OsAdminClient adminClient = ctx1.getBean(OsAdminClient.class);
        OsDocumentClient documentClient = ctx1.getBean(OsDocumentClient.class);

        TestEventDocument doc = new TestEventDocument("save-test-1", "test-event", "payload-data");

        assertThatNoException().isThrownBy(() -> documentClient.save(List.of(doc)));

        // Allow BulkIngester to flush (flush-interval-ms = 500 in itest application.yml)
        Thread.sleep(1500);
        adminClient.refresh(TestEventDocument.class);

        long count = ctx1.getBean(OpenSearchClient.class)
                .count(req -> req.index("it_test_events_write"))
                .count();
        assertThat(count).isGreaterThanOrEqualTo(1);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static void cleanupState() throws IOException {
        // Connect directly to clean up state from previous test runs
        org.apache.hc.core5.http.HttpHost host =
                new org.apache.hc.core5.http.HttpHost("http", "localhost", 9200);
        OpenSearchClient client = new OpenSearchClient(
                org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder
                        .builder(host)
                        .setMapper(new org.opensearch.client.json.jackson.JacksonJsonpMapper())
                        .build());

        deleteIndexIfExists(client, MIGRATIONS_INDEX);
        deleteIndexIfExists(client, "it-test-events-000001");
        deleteTemplateIfExists(client, "it-test-events-template");
    }

    private static void deleteIndexIfExists(OpenSearchClient client, String name) {
        try {
            if (client.indices().exists(req -> req.index(name)).value()) {
                client.indices().delete(req -> req.index(name));
            }
        } catch (Exception ignored) {}
    }

    private static void deleteTemplateIfExists(OpenSearchClient client, String name) {
        try {
            if (client.indices().existsIndexTemplate(req -> req.name(name)).value()) {
                client.indices().deleteIndexTemplate(req -> req.name(name));
            }
        } catch (Exception ignored) {}
    }

    private static void waitForMigrations(OpenSearchClient client, int expectedDocs) throws Exception {
        for (int i = 0; i < 30; i++) {
            try {
                boolean exists = client.indices().exists(req -> req.index(MIGRATIONS_INDEX)).value();
                if (exists) {
                    long count = client.count(req -> req.index(MIGRATIONS_INDEX)).count();
                    if (count >= expectedDocs) return;
                }
            } catch (Exception ignored) {}
            Thread.sleep(500);
        }
        fail("Timed out waiting for " + expectedDocs + " migration document(s) in index " + MIGRATIONS_INDEX);
    }

    private long countMigrationDocs(OpenSearchClient client) throws IOException {
        return client.count(req -> req.index(MIGRATIONS_INDEX)).count();
    }
}
