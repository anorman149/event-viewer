# Phase 7 — OpenSearch Storage: Requirements

## Goal

Event metadata is indexed in OpenSearch via the Phase 6 schema manager infrastructure with production-grade ILM (hot SSD NVMe → UltraWarm → delete), read/write alias routing, and bulk ingestion via BulkIngester.

---

## Scope

### In Scope

- `RuleStatus` enum and `RuleResult` record in `apps/event-ingest`
- `EventDocument` record annotated `@OsIndex` in `apps/event-ingest`
- `IlmPolicySettings` value type and `OsAdminClient.putIlmPolicy()` in `libs/opensearch-lib`
- Nullable `ilmPolicySettings` field on `MigrationData`; `OsSchemaManager` ILM step support
- Two `OsMigration` beans in `apps/event-ingest`: cluster settings (order=1), ILM + template + index (order=2)
- `IngestPipelineService.process()` stub replaced to call `OsDocumentClient.save()` directly after S3 flush
- Observability: `@Timed`, `DistributionSummary`, `Counter` per spec

### Out of Scope

- Rule evaluation logic — Phase 8 populates `ruleResults`; Phase 7 always indexes an empty list
- Full boolean search — `OsDocumentClient.search()` stub remains (Phase 10)
- UltraWarm tier transition testing — requires a multi-node cluster; verified in staging only
- Custom snapshot repository — AWS-managed automated snapshots used (hourly, 14-day retention); no configuration needed

---

## Domain Types

### `RuleStatus` enum (`apps/event-ingest`)

```java
public enum RuleStatus {
    UNKNOWN(0),
    SUCCESS(1),
    FAILURE(2);

    private final int code;

    RuleStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
```

### `RuleResult` record (`apps/event-ingest`)

```java
public record RuleResult(String ruleId, RuleStatus status) {

    @JsonValue
    public String toComposite() {
        return ruleId + "_" + status.getCode();
    }
}
```

Jackson serializes each `RuleResult` as a flat keyword string via `@JsonValue` (e.g., `"rule-abc_1"` for `ruleId="rule-abc"`, `status=SUCCESS`). The OpenSearch mapping stores `ruleResults` as a `keyword` array. Deserialization is out of scope for this phase (write path only).

### `EventDocument` record (`apps/event-ingest`)

```java
@OsIndex(
    indexPattern = "<events-{now/d}-000001>",
    templateName = "events-template",
    alias = @Alias(write = "events_write", read = "events_read")
)
public record EventDocument(
    String          eventId,
    String          schemaType,
    Instant         timestamp,
    String          s3Key,
    long            batchOffset,
    long            batchLength,
    List<RuleResult> ruleResults    // always List.of() at Phase 7; populated by Phase 8
) {
    public EventDocument {
        ruleResults = ruleResults != null ? ruleResults : List.of();
    }
}
```

---

## Index Template

**Template name:** `events-template`  
**Covers pattern:** `events-*`

**Settings:**

```json
{
  "index": {
    "dynamic": false,
    "date_detection": false,
    "lifecycle": { "name": "events-ilm-policy" },
    "number_of_replicas": "${opensearch.index.replicas}"
  }
}
```

`opensearch.index.replicas` defaults to `0` in `application.yml` (single-node dev); set to `1` in prod.

**Explicit field mappings:**

| Field | OpenSearch type | Notes |
|---|---|---|
| `eventId` | `keyword` | UUID stored as string |
| `schemaType` | `keyword` | Schema type name |
| `timestamp` | `date` | ISO-8601 via Jackson |
| `s3Key` | `keyword` | Full Hive-partitioned S3 key |
| `batchOffset` | `long` | Byte offset within batch |
| `batchLength` | `long` | Byte length within batch |
| `ruleResults` | `keyword` | Array of `"{ruleId}_{statusCode}"` strings |

**Initial index:** `<events-{now/d}-000001>` (date math; `-000001` suffix increments on each rollover)  
**Aliases declared in template:** `events_write` (write alias), `events_read` (read alias)

---

## ILM Policy

**Policy name:** `events-ilm-policy`

| Phase | Trigger | Action |
|---|---|---|
| Hot (SSD NVMe) | Rollover at **130 GB** or **12 hours** | — |
| Warm (UltraWarm) | Immediately after hot rollover | Transition to UltraWarm |
| Warm retention | 4 days after warm transition | Auto-delete |

No cold tier. No custom snapshot repository (AWS-managed automated snapshots: hourly, 14-day retention on the AWS-managed bucket — comfortably outlasts the 4.5-day hot + warm lifecycle).

---

## libs/opensearch-lib Extensions

### `IlmPolicySettings` value type

```java
public class IlmPolicySettings {
    private String   policyName;
    private long     rolloverMaxSizeGb  = 130;
    private Duration rolloverMaxAge     = Duration.ofHours(12);
    private Duration warmRetention      = Duration.ofDays(4);
}
```

### `OsAdminClient` addition

```java
void putIlmPolicy(IlmPolicySettings settings) throws OsException;
```

Implemented in `OsAdminClientImpl` via the OpenSearch `PUT _ilm/policy/{policyName}` endpoint using the low-level transport client. Annotated `@Timed(histogram=true, value="os.admin.client.put.ilm.policy")`. The PUT is idempotent — calling it on restart overwrites the existing policy safely.

### `MigrationData` extension

Add nullable field:

```java
private IlmPolicySettings ilmPolicySettings;   // nullable
```

`OsSchemaManager.onLeader()` processes fields in this order per `MigrationData` item:

1. `ilmPolicySettings != null` → `adminClient.putIlmPolicy(settings)`
2. `indexSettings != null` → idempotent template-then-index steps (`templateExists` → `createTemplate`; `indexExists` → `createIndex`)
3. `clusterSettings != null` → `adminClient.clusterSettings(settings)`

---

## Migrations (`apps/event-ingest`)

### `EventReplicaMigration` (order=1)

```
name:  "001_cluster_replica_settings"
data:  [ MigrationData { clusterSettings: ClusterSettings { searchMaxBuckets=10000, searchCancelerAfter=30s } } ]
```

Establishes baseline cluster search settings. Replica count is set in the index template `IndexSettings.replicas` (driven by `opensearch.index.replicas`) and is therefore not a separate cluster settings concern.

### `EventStorageMigration` (order=2)

```
name:  "002_events_template_ilm_index"
data:  [ MigrationData {
           ilmPolicySettings: events-ilm-policy (130 GB / 12 h rollover; warm 4 days; delete),
           indexSettings:     events-template (mappings above, lifecycle.name=events-ilm-policy,
                                replicas=${opensearch.index.replicas}, initial index <events-{now/d}-000001>)
         } ]
```

ILM policy and index settings are bundled in a single `MigrationData` so they are applied together and tracked in one migration document.

Both migrations are declared as `@Bean` methods in `EventOsMigrationConfig @Configuration` in `apps/event-ingest`.

---

## Ingest Pipeline Wiring

`IngestPipelineService.process()` (the stub from Phase 4) is replaced to call `OsDocumentClient.save()` directly after the S3 flush. No new class is introduced.

After the S3 flush, build one `EventDocument` per processed `EventRecord`:

```java
new EventDocument(
    eventRecord.eventId(),
    eventRecord.schemaType(),
    eventRecord.timestamp(),
    s3FlushResult.s3Key(),
    s3FlushResult.batchOffset(eventRecord),
    s3FlushResult.batchLength(eventRecord),
    List.of()          // ruleResults empty at this phase
)
```

Then call `osDocumentClient.save(eventDocuments)`. The `BulkIngester` inside `OsDocumentClientImpl` owns batching, flush scheduling, and error counting — no application-level retry or circuit breaker is added on top. `OsDocumentClient` is injected into `IngestPipelineService` via constructor injection.

If the S3 flush fails, `osDocumentClient.save()` is not called for that batch.

---

## Observability

All metric names use dot notation per `specs/Rules.md`.

New metric introduced in this phase:

| Metric | Type | Source |
|---|---|---|
| `os.admin.client.put.ilm.policy` | `@Timed(histogram=true)` | `OsAdminClient.putIlmPolicy()` in `libs/opensearch-lib` |

The bulk indexing path is already covered by Phase 6 metrics on `OsDocumentClientImpl`: `os.document.client.save` (`@Timed`), `os.bulk.documents` (`DistributionSummary`), and `os.bulk.flush.failures` (`Counter`). `IngestPipelineService.process()` is already annotated `@Timed(histogram=true)` per `specs/Rules.md` and covers the full operation including the `save()` call.

---

## Context & Dependencies

- **Depends on Phase 5** (`libs/s3-lib`): `IngestPipelineService.process()` calls S3 flush first; `s3Key`, `batchOffset`, `batchLength` from the flush result populate `EventDocument`
- **Depends on Phase 6** (`libs/opensearch-lib`): `OsDocumentClient.save()`, `OsSchemaManager`, `@OsIndex`, `OsAdminClient`; this phase extends those interfaces with ILM support
- **Consumed by Phase 8** (Rules Engine): `EventDocument.ruleResults` is populated with actual rule evaluation results; no schema change required
- **Consumed by Phase 10** (Metadata Search): `events_read` alias queried via `OsDocumentClient.search()`

---

## Module Boundaries

| Artifact | New contents |
|---|---|
| `libs/opensearch-lib` | `IlmPolicySettings`; `OsAdminClient.putIlmPolicy()`; `OsAdminClientImpl` implementation; `MigrationData.ilmPolicySettings`; `OsSchemaManager` ILM step |
| `apps/event-ingest` | `RuleStatus`; `RuleResult`; `EventDocument`; `EventReplicaMigration`; `EventStorageMigration`; `EventOsMigrationConfig`; updated `IngestPipelineService` |
