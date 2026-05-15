# Phase 6 — OpenSearch Schema Manager: Requirements

## Goal

A Liquibase-style schema manager library for OpenSearch provides ordered, idempotent, leader-gated migrations at startup. Document classes declare their index, template, and alias metadata via annotation. All subsequent OpenSearch phases consume this library.

---

## Scope

### In Scope

- `@Alias` annotation composed into `@OsIndex`; `OsSchemaRegistry` using **ClassGraph** for classpath scanning
- `OsAdminClient` and `OsDocumentClient` interfaces with concrete implementations backed by the **opensearch-java** transport client
- `IndexSettings`, `ClusterSettings`, `MigrationData`, `Search` (stub), and `SearchResult<T>` value types
- `OsException` — shared checked exception for all client and schema manager operations
- `OsDocumentClient.save()` backed by `BulkIngester`; all document client methods resolve aliases via `OsSchemaRegistry`
- `OsMigration` interface with `List<MigrationData> data()` — declarative; `OsSchemaManager` owns execution
- `OsSchemaManager` implementing `LeaderListener` (Phase 3); tracks applied version in a dedicated OpenSearch `migrations` index; version-gated execution
- Spring Autoconfiguration in `libs/opensearch-lib`
- itest: two Spring contexts in one JVM; verify only one applies migrations; verify idempotency on restart

### Out of Scope

- Full boolean search query-builder implementation (deferred to Phase 10; `search()` is a stub)
- `EventDocument` index template and ILM policy (deferred to Phase 7)
- Snapshots, cluster-level ILM, or OpenSearch repository configuration

---

## Annotations

### `@Alias`

```java
@Target({})
@Retention(RUNTIME)
public @interface Alias {
    String write();
    String read();
}
```

### `@OsIndex`

```java
@Target(TYPE)
@Retention(RUNTIME)
public @interface OsIndex {
    String indexPattern();
    String templateName();
    Alias alias();
}
```

Usage on a document class:

```java
@OsIndex(
    indexPattern  = "<events-{now/d}-000001>",
    templateName  = "events-template",
    alias = @Alias(write = "events_write", read = "events_read")
)
public record EventDocument(...) {}
```

---

## Supporting Types

### `OsIndexMetadata` record

Fields: `Class<?> documentClass`, `String indexPattern`, `String templateName`, `String writeAlias`, `String readAlias`.

### `IndexSettings`

```java
public class IndexSettings {
    private TypeMapping typeMapping;   // org.opensearch.client.opensearch._types.mapping.TypeMapping
    private Class<?>    entity;        // annotated with @OsIndex; OsSchemaManager resolves names via OsSchemaRegistry
    private int         shards             = 1;
    private int         replicas           = 0;
    private int         refreshIntervalSecs = 60;
    private String      codec              = "default";
}
```

`entity` is the `@OsIndex`-annotated class; `OsSchemaManager` calls `registry.getMetadata(entity)` to resolve `templateName`, `indexPattern`, `writeAlias`, and `readAlias` — no name strings are ever hardcoded in migration declarations.

When an `IndexSettings` is present in a `MigrationData`, `OsSchemaManager` performs two idempotent steps in order:
1. `templateExists(entity)` — if false, `createTemplate(settings)`
2. `indexExists(entity)` — if false, `createIndex(settings)`

### `ClusterSettings`

```java
public class ClusterSettings {
    private int      searchMaxBuckets      = 10000;
    private Duration searchCancelerAfter   = Duration.ofSeconds(30);
}
```

`OsSchemaManager` translates these to the OpenSearch persistent cluster settings API keys (`search.max_buckets`, `search.canceler.after`).

### `MigrationData`

```java
public class MigrationData {
    private IndexSettings   indexSettings;    // nullable
    private ClusterSettings clusterSettings;  // nullable
}
```

Plain class, not a sealed interface. `OsSchemaManager` checks each field for null independently — both may be set in a single `MigrationData` if a migration step needs both index and cluster changes.

### `Search` (stub)

A placeholder class in `libs/opensearch-lib` with no fields. Query-building logic is added in Phase 10.

### `SearchResult<T>` record

Fields: `List<T> hits`, `long total`.

### `OsException`

Checked exception extending `Exception`. Wraps all `opensearch-java` SDK exceptions. All `OsAdminClient` and `OsDocumentClient` methods declare `throws OsException`.

---

## `OsAdminClient` Interface

```java
public interface OsAdminClient {
    <T> void refresh(Class<T> clazz)                         throws OsException;
    <T> boolean indexExists(Class<T> clazz)                  throws OsException;
    boolean indexExists(String name)                         throws OsException;  // used internally by OsSchemaManager for the migrations index
    void createIndex(IndexSettings settings)                 throws OsException;
    void createTemplate(IndexSettings settings)              throws OsException;
    <T> boolean templateExists(Class<T> clazz)               throws OsException;
    void clusterSettings(ClusterSettings settings)           throws OsException;
}
```

The class-based overloads (`indexExists(Class<T>)`, `templateExists(Class<T>)`, `refresh(Class<T>)`) resolve the index or template name via `OsSchemaRegistry`. The `String`-based `indexExists(String)` overload is for internal use by `OsSchemaManager` when managing the migrations tracking index (which is not annotated with `@OsIndex`).

---

## `OsDocumentClient` Interface

```java
public interface OsDocumentClient {
    <T> void save(Collection<T> items)                               throws OsException;
    <T> T get(String id, Class<T> clazz)                             throws OsException;
    <T> SearchResult<T> search(Search search, Class<T> entityClass)  throws OsException;
    <T> void deleteByQuery(Search search, Class<T> entityClass)      throws OsException;
}
```

**Alias resolution:** every method calls `registry.getMetadata(entityClass)` first to obtain the write alias (for `save`) or read alias (for `get`, `search`, `deleteByQuery`). No alias strings are hardcoded anywhere in calling code.

**`get()` implementation note:** calls `openSearchClient.get(req -> req.index(readAlias).id(id), clazz)` and returns `response.source()`.

**`search()` stub:** throws `UnsupportedOperationException("search() is implemented in Phase 10")`.

---

## `OsMigration` Interface

```java
public interface OsMigration {
    int order();
    String name();
    List<MigrationData> data();
}
```

Implementations are declared as Spring `@Bean` methods inside `@Configuration` classes in the consuming app (`apps/event-ingest`). The library never defines concrete migrations.

---

## `OsSchemaManager` — LeaderListener + Version-Tracked Execution

`OsSchemaManager` implements `LeaderListener` (Phase 3). It is never invoked on a schedule — it runs exactly once, triggered by the leader acquiring the lock.

### Migration tracking index

An OpenSearch index named by `opensearch.migration.index-name` (default: `migrations`) stores one document per applied migration:

```json
{ "version": 1, "name": "001_create_events_template", "applied_at": "2026-05-15T..." }
```

### `onLeader()` flow

1. Check `indexExists("migrations")`; if not, `createIndex(...)` with minimal settings (1 shard, 0 replicas).
2. Query the migrations index for the highest applied `version` value. Default to `0` if the index is empty.
3. Sort all injected `OsMigration` beans by `order()` ascending.
4. Sort all injected migrations by `order()` ascending; take the last element — this is the `latestMigration`.
5. If `latestMigration.order() > currentVersion`: iterate `latestMigration.data()`; for each item — if `indexSettings != null` run the idempotent template-then-index steps (`templateExists` → `createTemplate`; `indexExists` → `createIndex`); if `clusterSettings != null` call `clusterSettings()`; index a tracking record into the migrations index; increment `os.schema.migrations.applied`.
6. If `latestMigration.order() <= currentVersion`: log at `DEBUG`; increment `os.schema.migrations.skipped`.
5. Emit `os.schema.migrations.applied` and `os.schema.migrations.skipped` counters.

### `onLeaderLoss()`

No-op. Migrations are one-time startup actions; no rollback on loss.

---

## `OsSchemaRegistry`

Uses **ClassGraph** (`io.github.classgraph:classgraph`) for classpath scanning at startup. Scans for all types annotated `@OsIndex`, builds an `OsIndexMetadata` per class, and caches in a `Map<Class<?>, OsIndexMetadata>`.

Exposes: `OsIndexMetadata getMetadata(Class<?> clazz)` — throws a descriptive `IllegalArgumentException` (not `NullPointerException`) if the class is not registered.

---

## Observability — Required Metrics

All metrics use dot notation per `specs/Rules.md`.

| Metric | Type | Method |
|---|---|---|
| `os.document.client.save` | `@Timed(histogram=true)` | `OsDocumentClient.save()` |
| `os.document.client.get` | `@Timed(histogram=true)` | `OsDocumentClient.get()` |
| `os.document.client.delete.by.query` | `@Timed(histogram=true)` | `OsDocumentClient.deleteByQuery()` |
| `os.document.client.search` | `@Timed(histogram=true)` | `OsDocumentClient.search()` |
| `os.bulk.documents` | `DistributionSummary` | Documents per BulkIngester flush |
| `os.bulk.flush.failures` | `Counter` | BulkIngester flush errors |
| `os.admin.client.create.index` | `@Timed(histogram=true)` | `OsAdminClient.createIndex()` |
| `os.admin.client.create.template` | `@Timed(histogram=true)` | `OsAdminClient.createTemplate()` |
| `os.schema.migrations.applied` | `Counter` | Migrations run in this startup |
| `os.schema.migrations.skipped` | `Counter` | Migrations skipped (already applied) |

---

## `OsProperties` Configuration Surface

```yaml
opensearch:
  host: localhost
  port: 9200
  use-ssl: false
  username: admin
  password: admin
  bulk:
    flush-threshold: 500      # docs per BulkIngester flush
    flush-interval-ms: 5000   # max ms between flushes
  migration:
    index-name: migrations    # name of the OpenSearch migration tracking index
```

---

## Context & Dependencies

- **Depends on Phase 3** (`libs/leader`): `OsSchemaManager` implements `LeaderListener`; Spring auto-discovers it via `List<LeaderListener>` injection in `RedissonLeaderElectionService`. Only the leader pod invokes `onLeader()`, so only it runs migrations.
- **Consumed by Phase 7** (`apps/event-ingest`): `EventDocumentIndexer` calls `OsDocumentClient.save()` with `@OsIndex`-annotated `EventDocument` records.
- **Consumed by Phase 10** (`apps/event-read`): `OsDocumentClient.search()` stub is replaced with a full boolean query implementation.

---

## Module Boundaries

| Artifact | Contents |
|---|---|
| `libs/opensearch-lib` | All interfaces, implementations, annotations, registry, schema manager, value types, autoconfiguration |
| `apps/event-ingest` | `@Configuration` classes declaring `OsMigration` `@Bean` methods for its own indices |
