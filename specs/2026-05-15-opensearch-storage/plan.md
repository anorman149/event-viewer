# Phase 7 — OpenSearch Storage: Plan

Tasks are grouped by dependency. Complete each group before starting the next.

---

## Group 1 — Domain Types

**Goal:** Establish `RuleStatus`, `RuleResult`, and `EventDocument` before any migration or indexer can reference them.

1. Define `RuleStatus` enum in `apps/event-ingest` — values `UNKNOWN(0)`, `SUCCESS(1)`, `FAILURE(2)`; constructor stores `int code`; `getCode()` accessor.
2. Define `RuleResult` record in `apps/event-ingest` — fields: `String ruleId`, `RuleStatus status`; annotate with `@JsonValue` on a `toComposite()` method that returns `"{ruleId}_{status.getCode()}"` so Jackson serializes each element as a flat keyword string (e.g., `"rule-abc_1"`).
3. Define `EventDocument` record in `apps/event-ingest` annotated `@OsIndex(indexPattern="<events-{now/d}-000001>", templateName="events-template", alias=@Alias(write="events_write", read="events_read"))`; fields: `String eventId`, `String schemaType`, `Instant timestamp`, `String s3Key`, `long batchOffset`, `long batchLength`, `List<RuleResult> ruleResults`; `ruleResults` defaults to an empty list when null (Jackson `@JsonInclude(NON_NULL)` already suppresses null; the constructor should normalize null to `List.of()`).

---

## Group 2 — ILM Extension to libs/opensearch-lib

**Goal:** Extend the Phase 6 admin client and migration data model to support ILM policy creation, which the Phase 7 migration needs.

1. Define `IlmPolicySettings` value type in `libs/opensearch-lib` — fields: `String policyName`, `long rolloverMaxSizeGb` (default 130), `Duration rolloverMaxAge` (default `Duration.ofHours(12)`), `Duration warmRetention` (default `Duration.ofDays(4)`).
2. Add `void putIlmPolicy(IlmPolicySettings settings) throws OsException` to `OsAdminClient` interface; implement in `OsAdminClientImpl` using the OpenSearch `PUT _ilm/policy/{policyName}` REST endpoint via the low-level transport client; annotate `@Timed(histogram=true, value="os.admin.client.put.ilm.policy")`; the PUT is idempotent (safe to call on restart).
3. Add nullable field `private IlmPolicySettings ilmPolicySettings` to `MigrationData`.
4. Update `OsSchemaManager.onLeader()` to process `MigrationData` fields in this order per item: `ilmPolicySettings != null` → `putIlmPolicy()`; then `indexSettings != null` → idempotent template-then-index steps; then `clusterSettings != null` → `clusterSettings()`.

---

## Group 3 — OpenSearch Migrations

**Goal:** Register two `OsMigration` beans in `apps/event-ingest` that `OsSchemaManager` picks up automatically via `List<OsMigration>` injection.

1. Define `EventReplicaMigration` implementing `OsMigration` — `order()` returns `1`; `name()` returns `"001_cluster_replica_settings"`; `data()` returns a single `MigrationData` with `ClusterSettings` only (`searchMaxBuckets = 10000`, `searchCancelerAfter = 30 s`). This establishes baseline cluster settings. Replica count is set directly in the index template `IndexSettings.replicas` (see step 2), driven by the `opensearch.index.replicas` property (default `0`).
2. Define `EventStorageMigration` implementing `OsMigration` — `order()` returns `2`; `name()` returns `"002_events_template_ilm_index"`; `data()` returns a single `MigrationData` containing:
   - `IlmPolicySettings` for `events-ilm-policy` (rollover at 130 GB or 12 h; UltraWarm after rollover; delete after 4 days warm retention)
   - `IndexSettings` for `events-template` — `dynamic: false`, `date_detection: false`; keyword mappings for `eventId`, `schemaType`, `s3Key`; date mapping for `timestamp`; long mappings for `batchOffset`, `batchLength`; keyword mapping for `ruleResults`; template settings include `index.lifecycle.name: events-ilm-policy`; `replicas` injected from `${opensearch.index.replicas}`; initial index name `<events-{now/d}-000001>`
3. Create `EventOsMigrationConfig @Configuration` in `apps/event-ingest` declaring both as `@Bean` methods.

---

## Group 4 — Ingest Pipeline Wiring

**Goal:** Replace the Phase 4 `IngestPipelineService.process()` stub to call `OsDocumentClient.save()` directly after the S3 flush. No new class is introduced — the service layer owns this call. Fault-tolerance (retry, circuit breaker) lives in `OsDocumentClientImpl` inside `libs/opensearch-lib`, not here.

1. In `IngestPipelineService.process()`, after the S3 flush succeeds, build a `List<EventDocument>` from the processed batch — one per `EventRecord`, using `s3FlushResult.s3Key()`, `s3FlushResult.batchOffset(eventRecord)`, and `s3FlushResult.batchLength(eventRecord)`; `ruleResults` is `List.of()` at this phase.
2. Call `osDocumentClient.save(eventDocuments)` — the `BulkIngester` in `OsDocumentClientImpl` handles batching, flush interval, and error counting; no application-level retry or circuit breaker is needed here.
3. Inject `OsDocumentClient` into `IngestPipelineService` via constructor injection; confirm the existing `@Timed(histogram=true)` on `IngestPipelineService.process()` (required by `specs/Rules.md`) covers the full operation including the OS save call.

---

## Group 5 — Tests

**Goal:** Verify serialization correctness and the end-to-end write path against live infrastructure.

1. Unit test `RuleResultTest` — verify `@JsonValue` composite string: `new RuleResult("rule-abc", RuleStatus.SUCCESS)` serializes via the application `ObjectMapper` to the JSON string `"rule-abc_1"`; confirm `RuleStatus.UNKNOWN.getCode() == 0`, `SUCCESS.getCode() == 1`, `FAILURE.getCode() == 2`.
2. itest `EventStorageIT` — index 100 `EventDocument` records by calling `IngestPipelineService.process()` with a synthetic batch against Docker Compose OpenSearch; after `OsAdminClient.refresh(EventDocument.class)`:
   - Verify field mappings via `GET events_write/_mapping`: `eventId: keyword`, `schemaType: keyword`, `timestamp: date`, `s3Key: keyword`, `batchOffset: long`, `batchLength: long`, `ruleResults: keyword`
   - Verify `events_write` alias routes to the active `events-*` index (`GET _cat/aliases?v`)
   - Verify `events_read` alias resolves to the same index; documents indexed via `events_write` are retrievable via `GET events_read/_search`
   - Verify ILM policy registered: `GET _ilm/policy/events-ilm-policy` returns HTTP 200; response body includes hot-phase rollover at 130 GB / 12 h and warm-phase delete after 4 days
   - Verify ILM policy attached: `GET events_write/_settings` shows `index.lifecycle.name: events-ilm-policy`
   - Verify `ruleResults` round-trip: index a document with `ruleResults = [new RuleResult("rule-abc", SUCCESS)]`; retrieve via `OsDocumentClient.get()`; field value is `["rule-abc_1"]`
