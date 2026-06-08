package org.eventviewer.opensearch.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.eventviewer.opensearch.*;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._helpers.bulk.BulkIngester;
import org.opensearch.client.opensearch._helpers.bulk.BulkListener;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.SortOptions;
import org.opensearch.client.opensearch._types.aggregations.Aggregate;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.Hit;
import org.opensearch.client.opensearch.indices.ExistsIndexTemplateRequest;
import org.opensearch.client.transport.BackoffPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OsClient implements OsAdminClient, OsDocumentClient {

    private static final Logger log = LoggerFactory.getLogger(OsClient.class);

    private final OpenSearchClient client;
    private final OsSchemaRegistry registry;
    private final FieldNameMapper fieldNameMapper;
    private final ObjectMapper objectMapper;
    private final DistributionSummary bulkDocuments;
    private final Counter bulkFlushFailures;
    private final DistributionSummary searchHits;
    private final Counter searchFailures;
    private final MeterRegistry meterRegistry;

    public OsClient(OpenSearchClient client,
                    OsSchemaRegistry registry,
                    MeterRegistry meterRegistry,
                    FieldNameMapper fieldNameMapper,
                    ObjectMapper objectMapper) {
        this.client = client;
        this.registry = registry;
        this.meterRegistry = meterRegistry;
        this.fieldNameMapper = fieldNameMapper;
        this.objectMapper = objectMapper;
        this.bulkDocuments = DistributionSummary.builder("os.bulk.documents")
                .description("Documents per bulk save call")
                .publishPercentileHistogram()
                .register(meterRegistry);
        this.bulkFlushFailures = Counter.builder("os.bulk.flush.failures")
                .description("Bulk save calls that completed with errors")
                .register(meterRegistry);
        this.searchHits = DistributionSummary.builder("opensearch.search.hits")
                .description("Hits per search response")
                .publishPercentileHistogram()
                .register(meterRegistry);
        this.searchFailures = Counter.builder("opensearch.search.failures")
                .description("Search calls that failed")
                .register(meterRegistry);
    }

    // ── OsAdminClient ─────────────────────────────────────────────────────────

    @Override
    @Timed(value = "os.admin.client.refresh", histogram = true)
    public <T> void refresh(Class<T> clazz) throws OsException {
        OsIndexMetadata metadata = registry.getMetadata(clazz);
        try {
            client.indices().refresh(req -> req.index(metadata.getWriteAlias()));
        } catch (IOException e) {
            throw new OsException("Failed to refresh index for " + clazz.getSimpleName(), e);
        }
    }

    @Override
    @Timed(value = "os.admin.client.index.exists", histogram = true)
    public <T> boolean indexExists(Class<T> clazz) throws OsException {
        OsIndexMetadata metadata = registry.getMetadata(clazz);
        try {
            return client.indices().exists(req -> req.index(metadata.getIndexPattern())).value();
        } catch (IOException e) {
            throw new OsException("Failed to check index existence for: " + metadata.getIndexPattern(), e);
        }
    }

    @Override
    @Timed(value = "os.admin.client.create.index", histogram = true)
    public void createIndex(IndexSettings settings) throws OsException {
        OsIndexMetadata metadata = registry.getMetadata(settings.getEntity());
        try {
            client.indices().create(req -> {
                var b = req.index(metadata.getIndexPattern())
                        .aliases(Map.of(
                                metadata.getWriteAlias(), org.opensearch.client.opensearch.indices.Alias.of(a -> a.isWriteIndex(true)),
                                metadata.getReadAlias(), org.opensearch.client.opensearch.indices.Alias.of(a -> a)
                        ))
                        .settings(osIndexSettings(settings));
                if (settings.getTypeMapping() != null) {
                    b.mappings(settings.getTypeMapping());
                }
                return b;
            });
            log.info("Created index: {}", metadata.getIndexPattern());
        } catch (IOException e) {
            throw new OsException("Failed to create index for " + settings.getEntity().getSimpleName(), e);
        }
    }

    @Override
    @Timed(value = "os.admin.client.create.template", histogram = true)
    public void createTemplate(IndexSettings settings) throws OsException {
        OsIndexMetadata metadata = registry.getMetadata(settings.getEntity());
        try {
            client.indices().putIndexTemplate(req -> req
                    .name(metadata.getTemplateName())
                    .indexPatterns(metadata.getTemplatePattern())
                    .template(t -> {
                        t.settings(osIndexSettings(settings));
                        if (settings.getTypeMapping() != null) {
                            t.mappings(settings.getTypeMapping());
                        }
                        t.aliases(metadata.getWriteAlias(),
                                org.opensearch.client.opensearch.indices.Alias.of(a -> a.isWriteIndex(true)));
                        t.aliases(metadata.getReadAlias(),
                                org.opensearch.client.opensearch.indices.Alias.of(a -> a));
                        return t;
                    }));
            log.info("Created index template: {}", metadata.getTemplateName());
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
                    ExistsIndexTemplateRequest.of(req -> req.name(metadata.getTemplateName()))
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
                            "search.cancel_after_time_interval", JsonData.of(cancelerAfter)
                    )));
        } catch (IOException e) {
            throw new OsException("Failed to apply cluster settings", e);
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
            String writeAlias = metadata.getWriteAlias();

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
                Object idFieldValue = metadata.getIdField().get(doc);

                ingester.add(op -> op.index(i -> {
                    i.index(writeAlias);
                    i.document(doc);

                    if(idFieldValue != null) {
                        i.id(idFieldValue.toString());
                    }

                    return i;
                }));
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
            var response = client.get(req -> req.index(metadata.getReadAlias()).id(id), clazz);
            if (response == null || !response.found()) {
                return null;
            }
            return response.source();
        } catch (IOException e) {
            throw new OsException("Failed to get document id=" + id + " from " + metadata.getReadAlias(), e);
        }
    }

    @Override
    @Timed(value = "opensearch.document.client.search", histogram = true)
    public <T> OsSearchResponse<T> search(Class<T> docClass, OsSearchRequest request) throws OsException {
        OsIndexMetadata metadata = registry.getMetadata(docClass);
        String readAlias = metadata.getReadAlias();

        try {
            SearchRequest.Builder reqBuilder = new SearchRequest.Builder()
                    .index(readAlias)
                    .query(request.query())
                    .sort(request.sort())
                    .size(request.size())
                    .aggregations(request.aggregations());

            if (request.searchAfter() != null && !request.searchAfter().isEmpty()) {
                List<FieldValue> fieldValues = request.searchAfter().stream()
                        .map(this::toFieldValue)
                        .toList();
                reqBuilder.searchAfter(fieldValues);
            }

            SearchResponse<ObjectNode> response = client.search(reqBuilder.build(), ObjectNode.class);

            List<T> hits = new ArrayList<>();
            List<Hit<ObjectNode>> rawHits = response.hits().hits();
            for (Hit<ObjectNode> hit : rawHits) {
                ObjectNode source = hit.source();
                if (source != null) {
                    ObjectNode processed = applyFieldNameMapping(source, docClass);
                    hits.add(objectMapper.treeToValue(processed, docClass));
                }
            }

            searchHits.record(hits.size());

            OsCursorPageable nextPage = null;
            if (!hits.isEmpty() && hits.size() >= request.size()) {
                Hit<ObjectNode> lastHit = rawHits.getLast();
                List<Object> searchAfterValues = extractSortValues(lastHit);
                int nextPageNumber = request.pageNumber() + 1;
                nextPage = new OsCursorPageable(request.sort(), request.size(), nextPageNumber, searchAfterValues);
            }

            long totalHits = response.hits().total() != null ? response.hits().total().value() : hits.size();

            Map<String, OsAggregationResult> aggregations = mapAggregations(response.aggregations());

            return new OsSearchResponse<>(hits, totalHits, nextPage, aggregations);

        } catch (Exception e) {
            searchFailures.increment();
            throw new OsException("Search failed for " + docClass.getSimpleName(), e);
        }
    }

    private <T> ObjectNode applyFieldNameMapping(ObjectNode source, Class<T> docClass) {
        Map<String, String> mappings = fieldNameMapper.getMappings(docClass);
        ObjectNode result = source.deepCopy();
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            String osName = entry.getKey();
            String javaName = entry.getValue();
            if (!osName.equals(javaName) && result.has(osName)) {
                result.set(javaName, result.get(osName));
                result.remove(osName);
            }
        }
        return result;
    }

    private List<Object> extractSortValues(Hit<ObjectNode> hit) {
        List<Object> values = new ArrayList<>();
        for (FieldValue fv : hit.sort()) {
            if (fv.isString()) values.add(fv._get());
            else if (fv.isLong()) values.add(fv.longValue());
            else if (fv.isDouble()) values.add(fv.doubleValue());
            else if (fv.isBoolean()) values.add(fv.booleanValue());
            else values.add(fv._get());
        }
        return values;
    }

    private FieldValue toFieldValue(Object val) {
        if (val instanceof String s) return FieldValue.of(f -> f.stringValue(s));
        if (val instanceof Long l) return FieldValue.of(f -> f.longValue(l));
        if (val instanceof Integer i) return FieldValue.of(f -> f.longValue(i.longValue()));
        if (val instanceof Double d) return FieldValue.of(f -> f.doubleValue(d));
        if (val instanceof Boolean b) return FieldValue.of(f -> f.booleanValue(b));
        return FieldValue.of(f -> f.stringValue(String.valueOf(val)));
    }

    private Map<String, OsAggregationResult> mapAggregations(Map<String, Aggregate> raw) {
        if (raw == null || raw.isEmpty()) return Map.of();
        Map<String, OsAggregationResult> result = new HashMap<>();
        for (Map.Entry<String, Aggregate> entry : raw.entrySet()) {
            result.put(entry.getKey(), mapAggregate(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    private OsAggregationResult mapAggregate(String name, Aggregate agg) {
        List<OsAggregationBucket> buckets = new ArrayList<>();
        if (agg.isSterms()) {
            for (var bucket : agg.sterms().buckets().array()) {
                buckets.add(new OsAggregationBucket(bucket.key(), bucket.docCount(), Map.of()));
            }
        } else if (agg.isLterms()) {
            for (var bucket : agg.lterms().buckets().array()) {
                // LongTermsBucketKey is a tagged union; extract the concrete value
                var k = bucket.key();
                Object keyValue = k.isSigned() ? k.signed() : k.unsigned();
                buckets.add(new OsAggregationBucket(keyValue, bucket.docCount(), Map.of()));
            }
        } else if (agg.isDterms()) {
            for (var bucket : agg.dterms().buckets().array()) {
                buckets.add(new OsAggregationBucket(bucket.key(), bucket.docCount(), Map.of()));
            }
        } else if (agg.isDateHistogram()) {
            for (var bucket : agg.dateHistogram().buckets().array()) {
                buckets.add(new OsAggregationBucket(bucket.key(), bucket.docCount(), Map.of()));
            }
        } else if (agg.isValueCount()) {
            buckets.add(new OsAggregationBucket("_value", agg.valueCount().value().longValue(), Map.of()));
        }
        return new OsAggregationResult(name, buckets);
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
            return s;
        });
    }
}
