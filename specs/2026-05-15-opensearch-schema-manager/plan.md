# Phase 6 — OpenSearch Schema Manager: Plan

Tasks are grouped by dependency. Complete each group before starting the next.

---

## Group 1 — Annotations & Schema Registry

**Goal:** Establish the metadata model and classpath scanner first. Every subsequent group depends on `OsSchemaRegistry` being available to resolve aliases and index names.

1. Define `@Alias` annotation (`write`, `read` string attributes; `@Target({})`, `@Retention(RUNTIME)` — nested within `@OsIndex`).
2. Define `@OsIndex` annotation (`indexPattern`, `templateName`, `alias = @Alias(...)`; `@Target(TYPE)`, `@Retention(RUNTIME)`).
3. Define `OsIndexMetadata` record — `Class<?> documentClass`, `String indexPattern`, `String templateName`, `String writeAlias`, `String readAlias`.
4. Add **ClassGraph** (`io.github.classgraph:classgraph`) to `libs/opensearch-lib/build.gradle`.
5. Implement `OsSchemaRegistry` as a Spring `@Component` — uses ClassGraph to scan all classpath types annotated `@OsIndex` on startup; builds and caches `Map<Class<?>, OsIndexMetadata>`; exposes `OsIndexMetadata getMetadata(Class<?> clazz)` (throws descriptive `IllegalArgumentException` for unregistered classes, not `NullPointerException`).

---

## Group 2 — Supporting Value Types & Custom Exception

**Goal:** Establish all shared value objects and the exception type before defining the interfaces that use them.

1. Define `OsException extends Exception` — wraps all opensearch-java SDK exceptions; constructor takes message + cause; used by all `OsAdminClient` and `OsDocumentClient` methods.
2. Define `IndexSettings` — fields: `TypeMapping typeMapping` (`org.opensearch.client.opensearch._types.mapping.TypeMapping`), `Class<?> entity` (the `@OsIndex`-annotated class; names resolved via registry), `int shards = 1`, `int replicas = 0`, `int refreshIntervalSecs = 60`, `String codec = "default"`.
3. Define `ClusterSettings` — fields: `int searchMaxBuckets = 10000`, `Duration searchCancelerAfter = Duration.ofSeconds(30)`.
4. Define `MigrationData` — plain class with `IndexSettings indexSettings` (nullable) and `ClusterSettings clusterSettings` (nullable); both may be set on a single instance.
5. Define `Search` — placeholder class with no fields; add a Javadoc line noting query-builder methods are added in Phase 10.
6. Define `SearchResult<T>` record — fields: `List<T> hits`, `long total`.

---

## Group 3 — Client Interfaces & Autoconfiguration Skeleton

**Goal:** Lock down the public API surface and wire up the Spring autoconfiguration before writing implementations.

1. Add `opensearch-java` and `jakarta.json-api` to `libs/opensearch-lib/build.gradle`.
2. Define `OsAdminClient` interface:
    ```java
    <T> void refresh(Class<T> clazz)            throws OsException;
    <T> boolean indexExists(Class<T> clazz)     throws OsException;
    boolean indexExists(String name)            throws OsException;
    void createIndex(IndexSettings settings)    throws OsException;
    void createTemplate(IndexSettings settings) throws OsException;
    <T> boolean templateExists(Class<T> clazz)  throws OsException;
    void clusterSettings(ClusterSettings s)     throws OsException;
    ```
3. Define `OsDocumentClient` interface:
    ```java
    <T> void save(Collection<T> items)                             throws OsException;
    <T> T get(String id, Class<T> clazz)                           throws OsException;
    <T> SearchResult<T> search(Search s, Class<T> entityClass)     throws OsException;
    <T> void deleteByQuery(Search s, Class<T> entityClass)         throws OsException;
    ```
4. Define `OsProperties` `@ConfigurationProperties(prefix = "opensearch")` — `host`, `port`, `useSsl`, `username`, `password`; nested `Bulk` record (`flushThreshold`, `flushIntervalMs`); nested `Migration` record (`indexName`, default `"migrations"`).
5. Stub out `OsAutoConfiguration` (registers `OsSchemaRegistry` and `OsProperties` beans; `OsAdminClient` and `OsDocumentClient` beans are wired in Group 4); declare in `AutoConfiguration.imports`.

---

## Group 4 — Client Implementations & Metrics

**Goal:** Provide working implementations of both client interfaces. Every method is timed.

1. Implement `OsAdminClientImpl`:
    - Instantiate `OpenSearchTransport` and `OpenSearchClient` from `OsProperties` in `OsAutoConfiguration`.
    - `indexExists(Class<T>)` and `templateExists(Class<T>)` call `registry.getMetadata(clazz)` to resolve the name before issuing the existence check.
    - `indexExists(String)` issues the existence check directly by name (used internally by `OsSchemaManager` for the migrations index).
    - `createIndex` / `createTemplate` build the request from `IndexSettings` fields.
    - `clusterSettings` applies `persistent` and `transient_` maps as separate entries.
    - All methods catch opensearch-java exceptions and wrap in `OsException`.
    - Annotate `createIndex` and `createTemplate` with `@Timed(histogram=true)` (`os.admin.client.create.index`, `os.admin.client.create.template`).
2. Implement `OsDocumentClientImpl` as a `SmartLifecycle` bean:
    - `start()` — creates `BulkIngester` with threshold and interval from `OsProperties.Bulk`.
    - `stop()` — calls `BulkIngester.close()`.
    - `save()` — calls `registry.getMetadata(itemClass)` to resolve write alias; submits each item to `BulkIngester`; `@Timed(histogram=true, value="os.document.client.save")`; `DistributionSummary("os.bulk.documents")` records count per flush; `Counter("os.bulk.flush.failures")` on flush error.
    - `get()` — resolves read alias from registry; calls `openSearchClient.get(req -> req.index(readAlias).id(id), clazz)`; returns `response.source()`; `@Timed(histogram=true, value="os.document.client.get")`.
    - `search()` — resolves read alias from registry; throws `UnsupportedOperationException("search() is implemented in Phase 10")`; `@Timed(histogram=true, value="os.document.client.search")`.
    - `deleteByQuery()` — resolves write alias from registry; throws `UnsupportedOperationException("deleteByQuery() is implemented in Phase 10")`; `@Timed(histogram=true, value="os.document.client.delete.by.query")`.
3. Complete `OsAutoConfiguration` — wire `OsAdminClientImpl` and `OsDocumentClientImpl` beans.

---

## Group 5 — Migration System

**Goal:** Declarative, ordered, version-tracked migrations run exactly once on the leader pod at startup.

1. Define `OsMigration` interface:
    ```java
    int order();
    String name();
    List<MigrationData> data();
    ```
2. Implement `OsSchemaManager` implementing `LeaderListener`:
    - Spring injects `List<OsMigration>` (all beans declared in consuming apps).
    - `onLeader()`:
        1. `adminClient.indexExists(props.getMigration().getIndexName())` — if false, `adminClient.createIndex(...)` with minimal settings (1 shard, 0 replicas, no aliases).
        2. Query migrations index for the current applied `version` value (default `0` if empty).
        3. Sort all injected migrations by `order()` ascending; take the last element — this is the `latestMigration`.
        4. If `latestMigration.order() > currentVersion`: iterate `latestMigration.data()`; for each `MigrationData` — if `indexSettings != null`: `templateExists(entity)` → if false `createTemplate(settings)`; `indexExists(entity)` → if false `createIndex(settings)`; if `clusterSettings != null`: call `clusterSettings()`; index a tracking document `{version, name, applied_at}` into the migrations index; increment `os.schema.migrations.applied` counter.
        5. If `latestMigration.order() <= currentVersion`: log at DEBUG; increment `os.schema.migrations.skipped`.
    - `onLeaderLoss()` — no-op.
3. Register `OsSchemaManager` as a Spring `@Component` so it is auto-discovered by `RedissonLeaderElectionService`'s `List<LeaderListener>` injection (Phase 3 contract).

---

## Group 6 — Tests

**Goal:** Verify registry, migration ordering, leader-only execution, and idempotency against real infrastructure.

1. Unit test `OsSchemaRegistryTest` — annotate a `TestDocument` record with `@OsIndex`; confirm `getMetadata(TestDocument.class)` returns correct metadata; confirm `getMetadata` on an unannotated class throws `IllegalArgumentException` with a useful message.
2. Unit test `OsSchemaManagerTest` — mock `OsAdminClient`; provide three `OsMigration` stubs with orders `[3, 1, 2]`; simulate follower (listener never called) and verify no admin client calls; simulate `onLeader()` and verify execution order is `[1, 2, 3]`; verify `os.schema.migrations.applied` counter equals 3.
3. itest — **leader-only migration:** two Spring contexts in the same JVM (distinct `MY_POD_NAME`, shared Docker Compose Redis and OpenSearch); both start; assert the OpenSearch migrations index contains exactly the expected number of documents (one per migration step); assert no migration is applied twice.
4. itest — **idempotency:** stop both contexts; restart them; assert no `OsException` is thrown; assert `os.schema.migrations.skipped` counter equals the number of migration steps on the leader context (all versions already applied).
