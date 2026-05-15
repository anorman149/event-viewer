# Phase 6 — OpenSearch Schema Manager: Validation

This phase is complete and ready to merge when every item below is satisfied.

---

## Build

- [ ] `./gradlew build` passes with no compilation errors or warnings across all modules
- [ ] `./gradlew :libs:opensearch-lib:test` passes — all unit tests green
- [ ] `./gradlew :libs:opensearch-lib:itest` passes — all integration tests green against Docker Compose OpenSearch and Redis

---

## Unit Tests

- [ ] `OsSchemaRegistryTest` — `TestDocument` annotated `@OsIndex` is found by ClassGraph; `getMetadata(TestDocument.class)` returns correct `writeAlias`, `readAlias`, `indexPattern`, and `templateName`; calling `getMetadata` on an unannotated class throws `IllegalArgumentException` with a message identifying the class name (not a `NullPointerException`)
- [ ] `OsSchemaManagerTest` — three `OsMigration` stubs with orders `[3, 1, 2]` are provided; `OsSchemaManager` selects the one with order `3` as the latest; only that migration's `data()` is executed; `onLeaderLoss()` is a no-op (no admin client calls); follower (listener `onLeader()` never invoked) results in zero admin client calls

---

## Integration Tests

- [ ] **Leader-only migration:** Two Spring contexts start in the same JVM against shared Docker Compose Redis and OpenSearch. After both contexts have fully started, the OpenSearch migrations index contains exactly one document per defined migration step. No migration document is duplicated. The follower context emits no migration-apply log lines.
- [ ] **Idempotency:** Both contexts from the test above are stopped and restarted. No `OsException` is thrown on either context. The `os.schema.migrations.skipped` counter on the leader context is `1` (the latest migration is already applied). The migrations index document count is unchanged from the first run.

---

## Functional Correctness

- [ ] `OsAdminClient.createTemplate()` produces a template visible via `GET _cat/templates` in Docker Compose OpenSearch
- [ ] `OsAdminClient.indexExists(Class<T>)` returns `false` for a non-existent index and `true` after `createIndex()`; resolves the index name from `@OsIndex` via `OsSchemaRegistry` without any hardcoded string
- [ ] `OsAdminClient.indexExists(String)` returns `false` for a non-existent name — used by `OsSchemaManager` for the migrations tracking index
- [ ] `OsDocumentClient.save()` indexes a list of test documents under the correct write alias; documents are retrievable via `get()` after `refresh()` is called; the alias name comes from `@OsIndex`, not from any hardcoded string in calling code
- [ ] `OsDocumentClient.get()` returns `response.source()` from the opensearch-java client — the raw document, not a wrapper type
- [ ] `OsDocumentClient.search()` throws `UnsupportedOperationException` with a message referencing Phase 10
- [ ] `OsDocumentClient.deleteByQuery()` throws `UnsupportedOperationException` with a message referencing Phase 10
- [ ] `OsSchemaManager` correctly null-checks both `MigrationData` fields independently: `indexSettings != null` triggers idempotent template-then-index steps; `clusterSettings != null` triggers `clusterSettings()`; a `MigrationData` with both fields set applies both in one step

---

## Migration Version Tracking

- [ ] The migrations tracking index is created on first startup if it does not exist (1 shard, 0 replicas)
- [ ] Each applied migration writes a document `{version, name, applied_at}` to the migrations index
- [ ] On restart, migrations whose `order()` is already in the index are skipped; new migrations (higher `order()`) are applied
- [ ] The migrations index name is driven by `opensearch.migration.index-name` in `application.yml`; changing the value uses the new name

---

## Observability

- [ ] All metrics use dot notation (no underscores in metric names)
- [ ] `os.document.client.save` — `@Timed(histogram=true)` on `save()`
- [ ] `os.document.client.get` — `@Timed(histogram=true)` on `get()`
- [ ] `os.document.client.search` — `@Timed(histogram=true)` on `search()`
- [ ] `os.document.client.delete.by.query` — `@Timed(histogram=true)` on `deleteByQuery()`
- [ ] `os.bulk.documents` — `DistributionSummary` recording documents per BulkIngester flush
- [ ] `os.bulk.flush.failures` — `Counter` incremented on each BulkIngester flush error
- [ ] `os.admin.client.create.index` — `@Timed(histogram=true)` on `createIndex()`
- [ ] `os.admin.client.create.template` — `@Timed(histogram=true)` on `createTemplate()`
- [ ] `os.schema.migrations.applied` — `Counter` incremented once per migration step that runs
- [ ] `os.schema.migrations.skipped` — `Counter` incremented once per migration step that is skipped

---

## API Contract

- [ ] No opensearch-java SDK types appear in any `OsAdminClient` or `OsDocumentClient` method signature — all public API uses plain Java types, `OsException`, or types defined in `libs/opensearch-lib`
- [ ] `OsSchemaRegistry.getMetadata()` is the only place alias and index names are resolved from `@OsIndex`; no alias or index name strings are hardcoded in any calling code in `libs/opensearch-lib` or `apps/*`
- [ ] `MigrationData` is a plain class with two nullable fields; no SDK types appear in its public API

---

## Readiness for Phase 7

- [ ] `apps/event-ingest` can declare a `@Configuration` class with `@Bean` methods returning `OsMigration` instances (for `EventDocument` template and initial index), and `OsSchemaManager` picks them up automatically via `List<OsMigration>` injection — verified by adding a stub migration in the itest app context and confirming it executes and is recorded in the migrations index
