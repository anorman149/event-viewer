# Phase 7 — OpenSearch Storage: Validation

This phase is complete and ready to merge when every item below is satisfied.

---

## Build

- [ ] `./gradlew build` passes with no compilation errors or warnings across all modules
- [ ] `./gradlew :apps:event-ingest:test` passes — all unit tests green
- [ ] `./gradlew :apps:event-ingest:itest` passes — all integration tests green against Docker Compose OpenSearch and Redis

---

## Unit Tests

- [ ] `RuleResultTest` — `new RuleResult("rule-abc", RuleStatus.SUCCESS)` serialized via the application `ObjectMapper` produces the JSON string `"rule-abc_1"`; `new RuleResult("rule-xyz", RuleStatus.UNKNOWN)` produces `"rule-xyz_0"`; `new RuleResult("rule-def", RuleStatus.FAILURE)` produces `"rule-def_2"`; enum codes: `UNKNOWN=0`, `SUCCESS=1`, `FAILURE=2`
- [ ] `EventDocumentTest` — `EventDocument` with null `ruleResults` normalizes to `List.of()` (compact constructor); serialized JSON omits null fields per Jackson `non_null` config; `ruleResults` field is absent from JSON when empty (not `null`, not `[]` — follows `NON_EMPTY` semantics if configured, or `NON_NULL` if not — clarify during implementation)
- [ ] `EventDocumentTest` — `EventDocument` with a null `ruleResults` argument normalizes to `List.of()` (compact constructor enforces this); serialized JSON has no `ruleResults` field when the list is empty (Jackson `non_null` / `NON_EMPTY` — confirm which applies during implementation)

---

## Integration Tests

- [ ] **Field mappings:** Index 10 `EventDocument` records; call `OsAdminClient.refresh(EventDocument.class)`; HTTP `GET events_write/_mapping` confirms `eventId: keyword`, `schemaType: keyword`, `timestamp: date`, `s3Key: keyword`, `batchOffset: long`, `batchLength: long`, `ruleResults: keyword`; no additional fields are mapped (template enforces `dynamic: false`)
- [ ] **Write alias routing:** `GET _cat/aliases?v` shows `events_write` pointing to the active date-math-named `events-*` index; documents indexed via `events_write` are stored in that index
- [ ] **Read alias routing:** `GET _cat/aliases?v` shows `events_read` pointing to the same active index; `GET events_read/_search` returns the documents indexed above
- [ ] **ILM policy registered:** `GET _ilm/policy/events-ilm-policy` returns HTTP 200; the response body contains a hot phase with `max_size: 130gb` and `max_age: 12h` rollover conditions; a warm phase; a delete action triggered after 4 days in warm
- [ ] **ILM policy attached:** `GET events_write/_settings` (or the underlying `events-*` index settings) shows `index.lifecycle.name: events-ilm-policy`
- [ ] **ruleResults keyword round-trip:** Index a document with `ruleResults = [new RuleResult("rule-abc", SUCCESS), new RuleResult("rule-xyz", UNKNOWN)]`; after refresh, retrieve via `OsDocumentClient.get()`; confirm the stored value is `["rule-abc_1", "rule-xyz_0"]`

---

## Functional Correctness

- [ ] `RuleResult.toComposite()` uses underscore as separator and the numeric `status.getCode()` value — not the enum name (`SUCCESS`, `FAILURE`, `UNKNOWN`)
- [ ] `EventDocument` with a null `ruleResults` argument normalizes to `List.of()` in the compact constructor — the field is never null after construction
- [ ] `IngestPipelineService.process()` calls `osDocumentClient.save()` only after a successful S3 flush — if S3 fails, the save call is not reached for that batch
- [ ] `EventDocument.ruleResults` is always `List.of()` in this phase; Phase 8 populates it without any OpenSearch schema change

---

## Migrations

- [ ] On first startup, `OsSchemaManager` applies migration order=1 (cluster settings) then order=2 (ILM + template + index); both tracking documents appear in the `migrations` OpenSearch index
- [ ] On restart, the latest migration (order=2) is already applied; `os.schema.migrations.skipped` counter equals `1`; no `OsException` is thrown; the `migrations` index document count is unchanged
- [ ] `EventReplicaMigration` (order=1) applies `ClusterSettings` only — no ILM policy, no index creation
- [ ] `EventStorageMigration` (order=2) applies ILM policy first, then template, then initial index — in that order within a single `MigrationData` item; a single tracking document is written to the `migrations` index for this migration
- [ ] `IndexSettings.replicas` is driven by `opensearch.index.replicas`; the test profile sets it to `0`; changing the value and running migrations applies the new replica count to the template

---

## Observability

- [ ] All metrics use dot notation — no underscores in metric names
- [ ] `os.admin.client.put.ilm.policy` — `@Timed(histogram=true)` recorded during migration startup when `putIlmPolicy()` is called
- [ ] Phase 6 bulk indexing metrics continue to fire correctly via `OsDocumentClientImpl`: `os.document.client.save` timer records on each `save()` call; `os.bulk.documents` distribution summary records document count per flush; `os.bulk.flush.failures` counter increments on BulkIngester flush errors

---

## API & Library Contract

- [ ] No opensearch-java SDK types appear in any `OsAdminClient` method signature — `putIlmPolicy` accepts only `IlmPolicySettings` from `libs/opensearch-lib`
- [ ] `IlmPolicySettings` is a plain value type with no SDK imports in its public API
- [ ] `MigrationData.ilmPolicySettings` is nullable; existing callers that do not set it are unaffected

---

## Readiness for Phase 8

- [ ] `EventDocument.ruleResults` is declared as `List<RuleResult>` and indexed successfully as a `keyword` array; Phase 8 can pass a populated list without any OpenSearch mapping change
- [ ] `RuleStatus` and `RuleResult` are package-accessible in `apps/event-ingest`; Phase 8 imports them directly
- [ ] The `events_write` alias and `events_read` alias are stable and functional; Phase 8 indexed documents appear in the same aliases
