package org.eventviewer.opensearch.autoconfigure;

import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.eventviewer.opensearch.*;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._helpers.bulk.BulkIngester;
import org.opensearch.client.opensearch._helpers.bulk.BulkListener;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.generic.Requests;
import org.opensearch.client.opensearch.indices.ExistsIndexTemplateRequest;
import org.opensearch.client.transport.BackoffPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OsClient implements OsAdminClient, OsDocumentClient {

    private static final Logger log = LoggerFactory.getLogger(OsClient.class);

    private final OpenSearchClient client;
    private final OsSchemaRegistry registry;
    private final DistributionSummary bulkDocuments;
    private final Counter bulkFlushFailures;
    private final MeterRegistry meterRegistry;

    public OsClient(OpenSearchClient client,
                    OsSchemaRegistry registry,
                    MeterRegistry meterRegistry) {
        this.client = client;
        this.registry = registry;
        this.meterRegistry = meterRegistry;
        this.bulkDocuments = DistributionSummary.builder("os.bulk.documents")
                .description("Documents per bulk save call")
                .publishPercentileHistogram()
                .register(meterRegistry);
        this.bulkFlushFailures = Counter.builder("os.bulk.flush.failures")
                .description("Bulk save calls that completed with errors")
                .register(meterRegistry);
    }

    // ── OsAdminClient ─────────────────────────────────────────────────────────

    @Override
    @Timed(value = "os.admin.client.refresh", histogram = true)
    public <T> void refresh(Class<T> clazz) throws OsException {
        OsIndexMetadata metadata = registry.getMetadata(clazz);
        try {
            client.indices().refresh(req -> req.index(metadata.writeAlias()));
        } catch (IOException e) {
            throw new OsException("Failed to refresh index for " + clazz.getSimpleName(), e);
        }
    }

    @Override
    @Timed(value = "os.admin.client.index.exists", histogram = true)
    public <T> boolean indexExists(Class<T> clazz) throws OsException {
        OsIndexMetadata metadata = registry.getMetadata(clazz);
        return indexExists(metadata.indexPattern());
    }

    @Override
    @Timed(value = "os.admin.client.index.exists.by.name", histogram = true)
    public boolean indexExists(String name) throws OsException {
        try {
            return client.indices().exists(req -> req.index(name)).value();
        } catch (IOException e) {
            throw new OsException("Failed to check index existence for: " + name, e);
        }
    }

    @Override
    @Timed(value = "os.admin.client.create.index", histogram = true)
    public void createIndex(IndexSettings settings) throws OsException {
        OsIndexMetadata metadata = registry.getMetadata(settings.getEntity());
        try {
            client.indices().create(req -> {
                var b = req.index(metadata.indexPattern())
                        .aliases(Map.of(
                                metadata.writeAlias(), org.opensearch.client.opensearch.indices.Alias.of(a -> a.isWriteIndex(true)),
                                metadata.readAlias(), org.opensearch.client.opensearch.indices.Alias.of(a -> a)
                        ))
                        .settings(osIndexSettings(settings));
                if (settings.getTypeMapping() != null) {
                    b.mappings(settings.getTypeMapping());
                }
                return b;
            });
            log.debug("Created index: {}", metadata.indexPattern());
        } catch (IOException e) {
            throw new OsException("Failed to create index for " + settings.getEntity().getSimpleName(), e);
        }
    }

    @Override
    @Timed(value = "os.admin.client.create.template", histogram = true)
    public void createTemplate(IndexSettings settings) throws OsException {
        OsIndexMetadata metadata = registry.getMetadata(settings.getEntity());
        String pattern = settings.getTemplatePattern() != null
                ? settings.getTemplatePattern()
                : metadata.indexPattern();
        try {
            client.indices().putIndexTemplate(req -> req
                    .name(metadata.templateName())
                    .indexPatterns(List.of(pattern))
                    .template(t -> {
                        t.settings(osIndexSettings(settings));
                        if (settings.getTypeMapping() != null) {
                            t.mappings(settings.getTypeMapping());
                        }
                        t.aliases(metadata.writeAlias(),
                                org.opensearch.client.opensearch.indices.Alias.of(a -> a.isWriteIndex(true)));
                        t.aliases(metadata.readAlias(),
                                org.opensearch.client.opensearch.indices.Alias.of(a -> a));
                        return t;
                    }));
            log.debug("Created index template: {}", metadata.templateName());
        } catch (IOException e) {
            throw new OsException("Failed to create template for " + settings.getEntity().getSimpleName(), e);
        }
    }

    @Override
    @Timed(value = "os.admin.client.template.exists", histogram = true)
    public <T> boolean templateExists(Class<T> clazz) throws OsException {
        OsIndexMetadata metadata = registry.getMetadata(clazz);
        try {
            return client.indices().existsIndexTemplate(
                    ExistsIndexTemplateRequest.of(req -> req.name(metadata.templateName()))
            ).value();
        } catch (IOException e) {
            throw new OsException("Failed to check template existence for " + clazz.getSimpleName(), e);
        }
    }

    @Override
    @Timed(value = "os.admin.client.cluster.settings", histogram = true)
    public void clusterSettings(ClusterSettings settings) throws OsException {
        try {
            String cancelerAfter = settings.getSearchCancelerAfter().toMillis() + "ms";
            client.cluster().putSettings(req -> req
                    .persistent(Map.of(
                            "search.max_buckets", JsonData.of(settings.getSearchMaxBuckets()),
                            "search.canceler.after", JsonData.of(cancelerAfter)
                    )));
        } catch (IOException e) {
            throw new OsException("Failed to apply cluster settings", e);
        }
    }

    @Override
    @Timed(value = "os.admin.client.put.ilm.policy", histogram = true)
    public void putIlmPolicy(IlmPolicySettings settings) throws OsException {
        String body = buildIlmPolicyJson(settings);
        try (var response = client.generic().execute(
                Requests.builder()
                        .method("PUT")
                        .endpoint("/_ilm/policy/" + settings.getPolicyName())
                        .json(body)
                        .build())) {
            if (response.getStatus() >= 400) {
                throw new OsException("ILM policy PUT failed with status " + response.getStatus()
                        + " for policy: " + settings.getPolicyName());
            }
            log.debug("ILM policy applied: {}", settings.getPolicyName());
        } catch (OsException e) {
            throw e;
        } catch (IOException e) {
            throw new OsException("Failed to create ILM policy: " + settings.getPolicyName(), e);
        }
    }

    private String buildIlmPolicyJson(IlmPolicySettings s) throws OsException {
        String maxAge = s.getRolloverMaxAge().toHours() + "h";
        String warmMin = s.getWarmRetention().toDays() + "d";
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("ilm-policy-template.json")) {
            if (is == null) {
                throw new OsException("ilm-policy-template.json not found on classpath");
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8)
                    .replace("{MAX_SIZE_GB}", String.valueOf(s.getRolloverMaxSizeGb()))
                    .replace("{MAX_AGE}", maxAge)
                    .replace("{WARM_MIN}", warmMin);
        } catch (OsException e) {
            throw e;
        } catch (IOException e) {
            throw new OsException("Failed to load ilm-policy-template.json", e);
        }
    }

    // ── OsDocumentClient ──────────────────────────────────────────────────────

    @Override
    @Timed(value = "os.document.client.save", histogram = true)
    public <T> void save(Collection<T> items) throws OsException {
        if (items.isEmpty()) return;
        try {
            T first = items.iterator().next();
            OsIndexMetadata metadata = registry.getMetadata(first.getClass());
            String writeAlias = metadata.writeAlias();

            BulkListener<Void> listener = new BulkListener<>() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request, List<Void> contexts) {}

                @Override
                public void afterBulk(long executionId, BulkRequest request, List<Void> contexts, BulkResponse response) {
                    bulkDocuments.record(request.operations().size());
                    if (response.errors()) {
                        bulkFlushFailures.increment();
                        log.warn("BulkIngester flush had errors (executionId={})", executionId);
                    }
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, List<Void> contexts, Throwable failure) {
                    bulkFlushFailures.increment();
                    log.error("BulkIngester flush failed (executionId={}): {}", executionId, failure.getMessage());
                }
            };

            BulkIngester<Void> ingester = BulkIngester.of(b -> b
                    .client(client)
                    .maxConcurrentRequests(17)
                    .maxSize(4000000)
                    .backoffPolicy(BackoffPolicy.exponentialBackoff(5L, 2))
                    .maxOperations(6000)
                    .flushInterval(100, TimeUnit.MILLISECONDS)
                    .listener(listener));

            for (T item : items) {
                final T doc = item;
                ingester.add(BulkOperation.of(op -> op.index(idx -> idx.index(writeAlias).document(doc))));
            }

            //Close to flush all pending items.
            ingester.close();

            DistributionSummary.builder("os.document.client.save.batch.size")
                    .tag("index", writeAlias)
                    .register(meterRegistry)
                    .record(items.size());
        } catch (IllegalArgumentException e) {
            throw new OsException("Failed to resolve @OsIndex metadata for save", e);
        } catch (Exception e) {
            throw new OsException("Failed to bulk save documents", e);
        }
    }

    @Override
    @Timed(value = "os.document.client.get", histogram = true)
    public <T> T get(String id, Class<T> clazz) throws OsException {
        OsIndexMetadata metadata = registry.getMetadata(clazz);
        try {
            var response = client.get(req -> req.index(metadata.readAlias()).id(id), clazz);
            if (response == null || !response.found()) {
                return null;
            }
            return response.source();
        } catch (IOException e) {
            throw new OsException("Failed to get document id=" + id + " from " + metadata.readAlias(), e);
        }
    }

    @Override
    @Timed(value = "os.document.client.search", histogram = true)
    public <T> SearchResult<T> search(Search search, Class<T> entityClass) throws OsException {
        registry.getMetadata(entityClass);
        throw new UnsupportedOperationException("search() is implemented in Phase 10");
    }

    @Override
    @Timed(value = "os.document.client.delete.by.query", histogram = true)
    public <T> void deleteByQuery(Search search, Class<T> entityClass) throws OsException {
        registry.getMetadata(entityClass);
        throw new UnsupportedOperationException("deleteByQuery() is implemented in Phase 10");
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private org.opensearch.client.opensearch.indices.IndexSettings osIndexSettings(IndexSettings settings) {
        return org.opensearch.client.opensearch.indices.IndexSettings.of(s -> {
            s.numberOfShards(settings.getShards())
             .numberOfReplicas(settings.getReplicas())
             .refreshInterval(t -> t.time(settings.getRefreshIntervalSecs() + "s"))
             .codec(settings.getCodec());
            if (settings.getLifecycleName() != null) {
                s.lifecycleName(settings.getLifecycleName());
            }
            return s;
        });
    }
}
