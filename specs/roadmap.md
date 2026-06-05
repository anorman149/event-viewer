# Roadmap

Phases are ordered by dependency. Each phase is a self-contained, shippable increment of 2–7 tasks. Later phases may run in parallel once their dependencies are met.

---

## Phase 1 — Project Skeleton

**Goal:** A running, deployable shell with no business logic yet.

- [x] Multi-module Gradle project structure — `apps/gateway`, `apps/bff`, `apps/event-ingest`, `apps/event-read`, `apps/management`; `libs/event-api`, `libs/opensearch-lib`, `libs/s3-lib`, `libs/common`; `frontend/` as a sibling Vite project
- [x] Spring Boot application stub with health-check endpoint (`GET /actuator/health`)
- [x] React + TypeScript frontend scaffold (Vite, routing, placeholder home page)
- [x] Docker Compose for local dev (Kafka KRaft, PostgreSQL, OpenSearch, OpenSearch Dashboards, LocalStack S3)
- [x] GitHub Actions CI — build, lint, and unit-test on every push

---

## Phase 2 — Kafka Ingest

**Goal:** Events arrive at the REST endpoint and land durably in Kafka. Platform-wide build standards (security, serialization, threading, fault tolerance) are established here and inherited by every subsequent phase.

- [x] Kafka topic provisioning via Spring Kafka `@ConfigurationProperties` — each topic plus its DLT configured from `application.yml` with name, partitions, and replication factor; `KafkaAdmin` creates all topics on startup
- [x] REST ingest endpoint (`POST /event/v1/events`) with schema-less JSON body; publishes received events to `event-raw` topic
- [x] Jackson configuration standardized across all Spring Boot apps — `write-dates-as-timestamps: false`, `fail-on-unknown-properties: false`, `default-property-inclusion: non_null`
- [x] Virtual threads enabled on all Spring Boot MVC apps; `@Async` and `@Scheduled` executors configured on virtual thread pools; `ContextSnapshotTaskDecorator` in `libs/common` propagates Spring Security context, MDC, and Micrometer spans into every spawned thread
- [x] Spring OAuth2 Resource Server added to all internal apps (`event-ingest`, `event-read`, `management`, `bff`); each validates the JWT using a committed dev RSA public key; gateway validates and forwards `Authorization` header unchanged; dev RSA key pair generated for local use
- [x] Gateway Resilience4J: `CircuitBreaker` per downstream service route + adaptive `RateLimiter` per client keyed on JWT subject or API key

---

## Phase 3 — Leader Election

**Goal:** Distributed leadership coordination ensures that exactly one pod runs scheduled tasks at any time. All future phases that require a singleton scheduler (lag monitoring, rule-cache refresh, retention jobs) depend on this infrastructure.

- [x] Redis added to `docker-compose.yml` and `docker-compose-test.yml` (standalone, ephemeral — no named volume); Redis is always required; no always-leader fallback mode
- [x] `libs/leader` library — Redisson `RLock` with watchdog-managed TTL; `RedissonLeaderElectionService` submits the election loop to a single-thread executor at startup (bootstrapping returns immediately); exposes `isLeader()` and `getFencingToken()` as locally-cached reads (no Redis call)
- [x] Fencing token — `RAtomicLong` in Redis incremented on each lock acquisition; exposed via `getFencingToken()`; protected operations compare the token before committing writes to guard against zombie leaders
- [x] `LeaderListener` interface — beans implementing `LeaderListener` are auto-discovered via Spring `List<LeaderListener>` injection; notified with `onLeader()` / `onLeaderLoss()` on state transitions; fencing token accessible via `LeaderElectionService.getFencingToken()`; no Spring `ApplicationEventPublisher` used
- [x] Failure-mode protections — graceful shutdown explicitly releases the lock and notifies listeners (immediate handoff); JVM kill lets watchdog TTL expire (default 30 s); network partition detected via `isHeldByCurrentThread()` monitoring; all failure paths notify listeners
- [x] `LeaderAwareScheduler` — `runIfLeader(LeaderTask task) throws Exception`; executes task only when leader; propagates all exceptions; no-op on followers
- [x] `KafkaLagMonitor` in `apps/event-ingest` — `@Scheduled` bean (interval: 60 s); runs via `LeaderAwareScheduler`; queries Kafka `AdminClient` for consumer group lag; emits `kafka.consumer.lag` gauge per (topic, partition, consumer-group) tuple
- [x] Observability — dot-notation Micrometer metrics: `leader.election.acquisitions`, `leader.election.relinquishments`, `leader.election.connection.losses`, `leader.election.is.leader`, `leader.election.fencing.token`, `kafka.consumer.lag` per partition

---

## Phase 4 — Kafka Consumer

**Goal:** Events are consumed from 4 sharded topics with production-grade configuration: dynamic listeners from properties (no `@KafkaListener`), static membership for pod-restart resilience, cooperative sticky rebalancing, manual batch acknowledgment, SASL security, and a full retry → DLT pipeline. Parse failures are metered and skipped — DLT stays clean.

- [x] Topic topology — 4 topics (`event-raw-1` … `event-raw-4`), 80 partitions × 1 replica (prod); 2 partitions (local); DLT topics provisioned automatically at `{topic}.DLT`; all topics created via `KafkaAdmin` from `EventConsumerProperties`
- [x] Dynamic container creation — one `ConcurrentMessageListenerContainer` per topic, created programmatically from properties in `EventConsumerContainerFactory` (implements `SmartLifecycle`); no `@KafkaListener` annotations; concurrency configurable (default 20 prod, 1 local)
- [x] Consumer settings — `CooperativeStickyAssignor`, `group.instance.id = {MY_POD_NAME}-{topic}-{threadIndex}` for static membership, `AckMode.MANUAL_IMMEDIATE`, `BatchAcknowledgingMessageListener` (both main and DLT); concurrency computed at runtime via `computeConcurrency(partitionsPerTopic, INGEST_POD_COUNT_env)`; virtual thread executor per container; `session.timeout.ms=45000`, `max.poll.interval.ms=300000`, `max.poll.records=500`, `heartbeat.interval.ms=3000`, fetch settings tuned for 50 MB/request
- [x] SASL/SCRAM-SHA-512 — plaintext in dev; `security.protocol=SASL_SSL` in prod; `sasl.jaas.config` injected via `KAFKA_SASL_JAAS_CONFIG` env var; never committed
- [x] `EventBatchListener` — per-record parse: `JsonProcessingException` caught, metered (`kafka.consumer.parse.failures`), skipped; valid records forwarded to `IngestPipelineService.process()` stub; `acknowledge()` called once per batch regardless
- [x] `DefaultErrorHandler` — 3-attempt exponential backoff (1 s → 2 s → 4 s); `DeadLetterPublishingRecoverer` routes to `{topic}.DLT`; `JsonProcessingException` is non-retryable
- [x] `DltConsumerContainerFactory` — one DLT container per topic (concurrency = 1, group `event-ingest-dlt-group`); `DltBatchMessageListener` retries up to 100 times (5 s fixed); `dlt.recovered` / `dlt.exhausted` counters
- [x] Kafka lag metrics from Phase 3 `KafkaLagMonitor` registered for both `event-ingest-group` and `event-ingest-dlt-group`

---

## Phase 5 — S3 Storage

**Goal:** A clean, Spring-integrated S3 abstraction layer in `libs/s3-lib` provides context-aware get, create, and delete operations with Step Builder patterns and Autoconfiguration; events are stored as individually ZSTD-compressed records under Hive-partitioned keys.

- [x] `HiveKeyBuilder` in `libs/s3-lib` — constructs `events/year=YYYY/month=MM/day=DD/hour=HH/pod=<pod>/<uuid>.zst` from partition params; pod name injected from `MY_POD_NAME` env var
- [x] `S3Client` in `libs/s3-lib` — single abstraction over the AWS S3 SDK; context holds bucket, region, and prefix; exposes three operations: `get`, `create`, `delete`; each operation uses a Step Builder pattern to guide callers through required parameters (e.g., `s3Client.create().key(hiveKey).body(bytes).execute()`); no leaky SDK types in public return values
- [x] Per-event ZSTD compression — each event is individually compressed (ZSTD level-3) before being passed to `S3Client.create()`; the stored file is a flat sequence of independently-compressed blobs; per-event compressed byte offset and length are tracked so `S3Client.get()` can retrieve and decompress exactly one event via byte-range without reading the full file
- [x] Spring Autoconfiguration — `S3AutoConfiguration` in `libs/s3-lib` configures an `S3Client` bean from `S3Properties` (`s3.bucket`, `s3.region`, `s3.prefix`, `s3.endpoint-override` for LocalStack); wired via `AutoConfiguration.imports` so any app adding the dependency gets the bean automatically; connection pool size, request timeout, and retry strategy (SDK default + Resilience4J) configurable via `S3Properties`
- [x] `S3BucketInitializer` in `libs/s3-lib` — `SmartLifecycle` bean wired by `S3AutoConfiguration`; applies a 5-day object expiry lifecycle rule (`PutBucketLifecycleConfiguration`) to the events prefix on startup; idempotent; aligns S3 retention with OpenSearch metadata TTL
- [x] Security — IAM role / instance profile in prod; LocalStack dummy credentials (`AWS_ACCESS_KEY_ID=test`) in dev; credentials never committed; S3 bucket policy enforces server-side encryption at rest
- [x] `@Timed(histogram=true)` on `S3Client.create()`, `S3Client.get()`, and `S3Client.delete()`; `DistributionSummary` for bytes written and bytes read; `Counter` for S3 PUT failures, S3 GET failures, and total operations
- [x] Unit tests (mock S3 client) + itest against LocalStack: write ZSTD-compressed events, verify object exists, verify per-event byte offsets are correct, verify each event is independently decompressible via byte-range get

---

## Phase 6 — OpenSearch Schema Manager

**Goal:** A Liquibase-style schema manager library for OpenSearch provides ordered, idempotent, leader-gated migrations at startup; document classes declare their index, template, and alias metadata via annotation; all subsequent OpenSearch phases consume this library.

- [x] Two client interfaces in `libs/opensearch-lib` using the OpenSearch Java Client — `OsAdminClient` (createIndex, deleteIndex, indexExists, refreshIndex, createTemplate, templateExists, clusterSettings) and `OsDocumentClient` (save, get, deleteByQuery, search — search is a stub; full query logic added in Phase 10)
- [x] `@OsIndex` annotation — attributes: `indexPattern`, `templateName`, `writeAlias`, `readAlias`; applied directly to document record classes; `OsSchemaRegistry` classpath-scans on startup and caches `OsIndexMetadata` per annotated class; client methods call `registry.getMetadata(MyDoc.class)` to resolve alias names at runtime without hardcoding strings
- [x] `OsDocumentClient.save()` backed by `BulkIngester` from the OpenSearch Java Client — configurable flush threshold and interval; callers pass a list of documents and the registry supplies the correct write alias automatically
- [x] `OsMigration` abstraction — ordered, named, idempotent migration steps (each step checks `templateExists` / `indexExists` before acting); steps declared as Spring `@Bean` methods inside `@Configuration` classes; `apps/event-ingest` owns all migration configs for its indices
- [x] `OsSchemaManager` — collects all `OsMigration` beans at startup, sorts by declared order, executes sequentially; gated by `LeaderAwareScheduler` (Phase 3) so only the leader pod runs migrations; followers skip and rely on the leader having applied changes

---

## Phase 7 — OpenSearch Storage

**Goal:** Event metadata is indexed in OpenSearch via the Phase 6 schema manager infrastructure with production-grade ILM (hot SSD NVMe → UltraWarm → delete), read/write alias routing, and bulk ingestion via BulkIngester.

- [x] `EventDocument` record in `apps/event-ingest` annotated `@OsIndex(indexPattern="events-*", templateName="events-template", writeAlias="events_write", readAlias="events_read")` — fields: `eventId`, `schemaType`, `timestamp`, `s3Key`, `batchOffset`, `batchLength`, `ruleResults` (list of `{ruleId, status}` objects)
- [x] Index template `events-template` — covers pattern `events-*`; `dynamic: false`, `date_detection: false`; explicit keyword/date/integer mappings for all fixed fields; initial index created with date math name `<events-{now/d}-000001>` (suffix increments on each rollover); template declares `events_write` and `events_read` aliases
- [x] ILM policy — hot tier (SSD NVMe): rollover at 130 GB or 12 hours; transition to UltraWarm after rollover; UltraWarm retention: 4 days, then auto-delete; no cold tier; policy attached to the template via `index.lifecycle.name`
- [x] S3 snapshots — use the default AWS-managed automated snapshot policy (hourly snapshots, 14-day retention on the AWS-managed bucket); 14-day retention comfortably outlasts the 4.5-day hot + warm lifecycle; no custom snapshot repository needed
- [x] Replica count configurable — `opensearch.index.replicas: 0` for single-node dev; `1` for prod minimum; applied via an `OsMigration` cluster settings step
- [x] `EventDocumentIndexer` in `apps/event-ingest` — calls `OsDocumentClient.save()` (BulkIngester-backed); async flush; Resilience4J circuit breaker; retry with backoff on transient failures
- [x] `@Timed(histogram=true)` on bulk index; `DistributionSummary` for documents per bulk request; `Counter` for index failures and circuit breaker opens
- [x] itest against OpenSearch in Docker Compose: index events, verify field mappings, verify ILM policy applied, verify write alias routes to the active index, verify read alias resolves correctly

---

## Phase 8 — Metadata Search

**Goal:** Events are queryable with a composable, type-safe boolean query language; results are paginated using OpenSearch `search_after` (no PIT). The query language and pagination model live in `libs/event-api`; all OpenSearch translation and aggregation concerns live in `apps/event-read`. Abstraction layers are kept strictly separate.

### Search Query Language — `libs/event-api` (`search` package)

- [ ] **`FieldType` enum** — `KEYWORD`, `INTEGER`, `DATE`, `TEXT`, `BOOLEAN`; represents the OpenSearch field type backing each searchable field

- [ ] **`SearchField` enum** — one value per searchable event field; each value holds `fieldName: String` (the actual OpenSearch field name), `fieldType: FieldType`, and `allowedAggregation: AggregationType` (null when aggregation is not permitted on the field); values and their allowed aggregations:
  - `EVENT_ID` — `FieldType.KEYWORD`, `allowedAggregation = null`
  - `SCHEMA_TYPE` — `FieldType.KEYWORD`, `allowedAggregation = TERMS`
  - `TIMESTAMP` — `FieldType.DATE`, `allowedAggregation = DATE_HISTOGRAM`
  - `RULE_RESULT_STATUS` — `FieldType.KEYWORD`, `allowedAggregation = null`
  - (`S3_KEY` is an internal storage field and is intentionally excluded from `SearchField`)

- [ ] **`Expression` interface** — root marker interface for all query tree nodes

- [ ] **`ConditionExpr implements Expression`** — leaf query node with static factory methods that each accept a `SearchField` plus typed value(s) and return a fully initialised `ConditionExpr`:
  - `eq(SearchField, Object value)`
  - `in(SearchField, Collection<?> values)`
  - `between(SearchField, Object lower, Object upper)`
  - `exists(SearchField)`
  - `notExists(SearchField)`

- [ ] **`BooleanExpr implements Expression`** — composite query node; holds `must: List<Expression>`, `should: List<Expression>`, `mustNot: List<Expression>`; builder or static factory methods for each clause list; any list may be empty

- [ ] **`SortDirection` enum** — `ASC`, `DESC`

- [ ] **`SearchPage`** — pagination descriptor; fields: `page: int` (`@Min(0)`, default `0`), `size: int` (`@Min(1)`, `@Max(1000)`, default `20`); when the client omits `SearchPage` from the request entirely the system defaults to page 0 / size 20

- [ ] **`AggregationType` enum** — `TERMS`, `DATE_HISTOGRAM`, `VALUE_COUNT`

- [ ] **`AggregationRequest`** — describes one aggregation to compute alongside the query; fields: `name: String` (`@NotBlank`), `type: AggregationType` (`@NotNull`), `field: SearchField` (`@NotNull`, must have a non-null `allowedAggregation`), `interval: String` (required only for `DATE_HISTOGRAM`, e.g. `"1h"`); validation rejects any `field` whose `allowedAggregation` is null

- [ ] **`CursorPageable`** — encapsulates all cursor-based paging and sorting state; decouples the client from raw `search_after` mechanics; fields:
  - `page: @Valid SearchPage` — pagination bounds (defaults applied if omitted)
  - `sortField: @NotNull SearchField` — field to sort by; must be present for stable `search_after` ordering
  - `sortDirection: @NotNull SortDirection` — `ASC` or `DESC`
  - `searchAfter: List<Object>` — opaque cursor token produced by the previous `SearchResponse`; `null` on the first page; clients treat this as an opaque value and pass it back verbatim

- [ ] **`SearchRequest`** — fully validated search descriptor annotated with `@Validated`; fields:
  - `expression: Expression` — root query tree; `null` means match-all
  - `cursorPageable: @NotNull @Valid CursorPageable` — paging, sort, and cursor; contains `SearchPage` (defaulted if omitted)
  - `aggregations: @Valid List<AggregationRequest>` — empty list means no aggregations

- [ ] **`AggregationBucket`** — single bucket in an aggregation result; fields: `key: Object`, `docCount: long`, `subAggregations: Map<String, AggregationResult>`

- [ ] **`AggregationResult`** — output of one named aggregation; fields: `name: String`, `buckets: List<AggregationBucket>`

- [ ] **`SearchResponse<T>`** — generic search result; fields: `hits: List<T>`, `totalHits: long`, `nextPage: CursorPageable` (null when no further pages — `searchAfter` inside is populated from the last hit's sort values; client passes this object back as `cursorPageable` in the next `SearchRequest`), `aggregations: Map<String, AggregationResult>`

- [ ] Spring `@Validated` and Bean Validation constraints applied to all `SearchRequest` and nested objects; validation failures propagated to callers as `ConstraintViolationException`; `GlobalExceptionHandler` maps these to HTTP 400

### OpenSearch ORM additions — `libs/opensearch-lib`

- [ ] **`@FieldName` annotation** — `@Target(FIELD)`, `@Retention(RUNTIME)`; single attribute `value: String`; placed on fields inside `@OsIndex`-annotated document classes to declare the OpenSearch field name when it differs from the Java field name (e.g. `@FieldName("schema_type")` on a Java field named `schemaType`)

- [ ] **`FieldNameMapper`** — reflection cache for ORM field resolution:
  - Internal cache: `ConcurrentHashMap<Class<?>, Map<String, Field>>` — key is the OpenSearch field name (from `@FieldName.value` if present, otherwise the Java field name); value is the `java.lang.reflect.Field`
  - Populated once per class on first access by scanning all declared fields
  - Primary API: `getField(Class<?> docClass, String opensearchFieldName): Field` — returns the cached `Field` or throws `IllegalArgumentException` if the field is not found
  - `getValue(Object document, String opensearchFieldName): Object` — convenience method; retrieves and returns the field value via reflection

### Event-Read service — `apps/event-read`

- [ ] **`OsDocumentClient.search()` implementation** — fills in the Phase 6 stub; accepts an already-translated OpenSearch Java Client `SearchRequest`; executes against the read alias; returns the raw `SearchResponse` from the OpenSearch client; all query building and result mapping is the caller's responsibility

- [ ] **`SearchRequestTranslator`** — translates a `libs/event-api` `SearchRequest` into an OpenSearch Java Client `SearchRequest`:
  - Recursively walks the `Expression` tree: `BooleanExpr` → OpenSearch `bool` query with `must` / `should` / `must_not` clauses; `ConditionExpr` → `term` (eq), `terms` (in), `range` (between), `exists` (exists), `bool.must_not[exists]` (notExists)
  - Resolves `SearchField.fieldName` for all query leaf nodes
  - Reads sort field, sort direction, and `searchAfter` cursor from `searchRequest.cursorPageable`; appends `search_after` array only when non-null (omitted on first page)
  - Sets `sort` on `cursorPageable.sortField.fieldName` + `cursorPageable.sortDirection`; a secondary sort on `_id` guarantees stable ordering for `search_after`
  - Translates `AggregationRequest` list into OpenSearch aggregation DSL (`terms`, `date_histogram`, `value_count`)

- [ ] **`SearchResponseTranslator`** — translates raw OpenSearch `SearchResponse` → `SearchResponse<EventSearchResult>`:
  - Maps each hit to `EventSearchResult` via `ObjectMapper`
  - Extracts sort values from the last hit; if present, constructs a `CursorPageable` (copying `sortField`, `sortDirection`, and `SearchPage` from the request's `cursorPageable`, advancing `page` by 1, and setting `searchAfter` to the extracted sort values) → `nextPage`; `nextPage` is null when `hits` is empty
  - Extracts `aggregations` map → `Map<String, AggregationResult>` with bucket key, doc count, and nested sub-aggregations
  - Sets `totalHits` from `hits.total.value`

- [ ] **`EventSearchResult`** — external-safe projection of `EventDocument`; exposes `eventId`, `schemaType`, `timestamp`, `s3Key`; omits internal byte offsets

- [ ] **REST endpoint** — `POST /search/v1/events` in `apps/event-read`; accepts `@Validated @RequestBody SearchRequest`; returns `SearchResponse<EventSearchResult>`; POST is used because the query expression tree can be arbitrarily large; validation errors handled by `GlobalExceptionHandler` → HTTP 400

- [ ] Unit tests — expression building, `SearchRequestTranslator` output (verify OpenSearch query DSL shape), `FieldNameMapper` cache correctness and `@FieldName` override behaviour

- [ ] itest against OpenSearch in Docker Compose — index events via Phase 7 indexer; search with `eq`, `in`, `between`, `exists`, `notExists`, and nested `BooleanExpr` (AND / OR / NOT); verify `search_after` cursor advances correctly page-by-page; verify `TERMS` and `DATE_HISTOGRAM` aggregation buckets; verify `@FieldName`-annotated fields resolve correctly

---

## Phase 9 — Rules Engine

**Goal:** Configurable validation rules are defined in PostgreSQL, cached in the ingest service, and evaluated in parallel against incoming events; rule results are indexed in OpenSearch per event.

- [ ] `Schema` entity in PostgreSQL (`apps/management`) — `schema_id`, `name`, `version`, `description`, `created_at`; Flyway migration; JPA entity; REST CRUD (`/api/v1/schemas`)
- [ ] `Rule` entity in PostgreSQL — `rule_id`, `schema_id` (FK), `name`, `rule_type` (enum: `FIELD_EXISTS`, `FIELD_EQUALS`, `FIELD_REGEX`, `FIELD_RANGE`, `REQUIRED_FIELD_LIST`), `condition_expression` (JSON), `severity` (enum: `INFO` / `WARN` / `ERROR`), `enabled`; REST CRUD (`/api/v1/rules`, `/api/v1/schemas/{name}/rules`)
- [ ] Rule cache in `apps/event-ingest` — Caffeine cache keyed by schema name; TTL configurable (`rule-cache.ttl-seconds`); refreshed on `@Scheduled` interval (leader-aware via Phase 3); `rule_cache.hits` / `rule_cache.misses` counters
- [ ] `RuleEvaluationEngine` — groups the current Kafka consumer batch by `schemaType`; submits one virtual-thread task per schema group; each task evaluates all enabled rules for that schema against all events in the group; returns `List<RuleEvaluationResult>` per event
- [ ] `RuleEvaluationResult` — `eventId`, `ruleId`, `ruleResult` (enum: `PASS` / `FAIL` / `NOT_APPLICABLE` / `ERROR`), `evaluationTimeNs`
- [ ] Failed-rule events are NOT discarded — indexed in OpenSearch with `status = FAIL` in the `ruleResults` list; only system/parse errors route to dead-letter
- [ ] `@Timed(histogram=true)` on `RuleEvaluationEngine.evaluate()`; `Counter` for rules evaluated, rule failures per `rule_id`; `DistributionSummary` for events per batch group
- [ ] Unit tests for each rule type; itest: register schema + rule via management API, publish events, consume, assert `ruleResults` in OpenSearch index

---

## Phase 10 — Ingest Benchmarking

**Goal:** End-to-end throughput is baselined and the primary bottleneck on the path to 1M events/sec is identified and documented.

- [ ] Load-test harness in `apps/event-ingest/src/itest` — configurable event count (default 1M), payload size distribution, consumer concurrency, batch size
- [ ] Measure: events/sec from first Kafka produce to last S3 flush + OpenSearch index; S3 PUT throughput; OpenSearch bulk index throughput; Kafka consumer poll latency; rule evaluation overhead
- [ ] JVM tuning sweep — test with default JVM settings, then with tuned GC (`-XX:+UseZGC`), then with tuned virtual thread pool sizes; document delta
- [ ] Bottleneck identification — record which stage is the ceiling at each concurrency level
- [ ] Document results in `specs/2026-05-13-storage-consumers/benchmark-results.md` — hardware spec, JVM version, settings used, events/sec achieved per stage
- [ ] Target: establish baseline; remediate top bottleneck if below 100K events/sec; document path to 1M events/sec with horizontal scaling model

---

## Phase 11 — Payload Retrieval & API Contract

**Goal:** Raw event payloads are retrievable from S3; a typed API contract is published for frontend consumers.

- [ ] Raw payload retrieval: OpenSearch lookup → `s3_key` + byte range → fetch from S3 → ZSTD decompress → return event (`GET /search/v1/events/{id}/payload`)
- [ ] OpenAPI spec published and used to generate typed TypeScript client

---

## Phase 12 — Frontend Event Explorer

**Goal:** Users can find and inspect any event through the UI.

- [ ] Event list view — paginated, sortable table with boolean search/filter controls (AND / NOT / OR)
- [ ] Event detail panel — decoded raw payload (fetched from S3 via lookup), metadata, schema type, timestamps
- [ ] Type-safe API client layer from generated OpenAPI types
- [ ] Empty states, error boundaries, and loading skeletons
- [ ] Virtual scrolling for large result sets (no DOM thrash at 10k+ rows)

---

## Phase 13 — Real-Time Event Feed

**Goal:** Users see events arrive live without polling.

- [ ] Spring WebSocket gateway with STOMP broker
- [ ] Backend publishes inbound events to a STOMP topic post-Kafka consume (metadata only; no S3 round-trip on live feed)
- [ ] Live feed React component — auto-scrolling, pause/resume controls
- [ ] Event rate widget (events/sec, rolling 60-second window) sourced from OpenSearch aggregations
- [ ] Client-side stream filters (type, keyword) applied before render

---

## Phase 14 — Dashboards & Visualizations

**Goal:** Users can build and save custom visual layouts.

- [ ] Dashboard layout engine — draggable, resizable panel grid
- [ ] Time-series chart panel — event volume over configurable time window (OpenSearch date histogram aggregation)
- [ ] Event-type breakdown panel — pie / bar chart by schema type (OpenSearch terms aggregation)
- [ ] Dashboard persistence — save and load layouts in PostgreSQL
- [ ] Share dashboard by URL (read-only shareable link)

---

## Phase 15 — Alerting & Notifications

**Goal:** Users are notified when event patterns meet defined thresholds.

- [ ] Alert rule model in PostgreSQL — threshold (count, rate), time window, schema filter, boolean query expression
- [ ] Alert evaluation engine (Kafka Streams topology) — runs OpenSearch aggregation queries continuously
- [ ] Notification delivery — webhook and email channels
- [ ] Alert history and acknowledgement stored in PostgreSQL
- [ ] Alert management UI — create, edit, enable/disable, view history

---

## Phase 16 — Authentication & Access Control

**Goal:** Every action in the system is authenticated and authorized.

- [ ] Spring Security + JWT (access token + refresh token flow); user records in PostgreSQL
- [ ] User registration, login, and logout endpoints
- [ ] RBAC — roles: `Admin`, `Analyst`, `Viewer`; enforced on all endpoints
- [ ] API key issuance and validation for ingest clients (hashed, stored in PostgreSQL)
- [ ] Frontend auth flows — login page, token refresh, role-gated routes

---

## Phase 17 — Collaboration & Export

**Goal:** Insights are shareable and portable.

- [ ] Saved searches — store named boolean query expressions in PostgreSQL, recall from UI
- [ ] Export event results to CSV and JSON (stream metadata from OpenSearch; fetch payloads from S3 on demand)
- [ ] Shareable dashboard links with optional expiry (stored in PostgreSQL)
- [ ] Audit log UI — view who searched, exported, or modified alert rules

---

## Phase 18 — Data Retention & Archiving

**Goal:** Old data ages out automatically per policy; nothing is silently lost.

- [ ] Retention policy model in PostgreSQL — configurable TTL per schema type
- [ ] Scheduled purge job — delete expired OpenSearch metadata documents; S3 objects expire via bucket lifecycle policy
- [ ] GDPR per-record erasure: delete OpenSearch document + overwrite S3 batch byte range (or tombstone in index)
- [ ] Restore workflow — re-index a historical S3 batch back into OpenSearch on demand (key prefix → decompress → bulk index)
- [ ] Retention policy UI — view and edit TTL per schema type

---

## Phase 19 — Observability & Operations

**Goal:** The platform can monitor itself and support on-call engineers.

- [ ] Structured JSON logging (Logback) with correlation IDs across services, shipped to OpenSearch
- [ ] Micrometer metrics → Prometheus scrape endpoint on all services
- [ ] Grafana dashboard — ingest lag, S3 flush rate, OpenSearch indexing lag, query latency, Kafka consumer lag
- [ ] OpenTelemetry distributed tracing → Jaeger (trace full ingest path: REST → Kafka → S3 + OpenSearch)
- [ ] Runbook for common failure scenarios (Kafka lag, OpenSearch disk pressure, S3 flush backlog, DB locks)

---

## Phase 20 — Resilience & Disaster Recovery

**Goal:** The system survives failures and can be recovered to a known state.

- [ ] Kafka topic replication factor and consumer group offset management review
- [ ] PostgreSQL automated backup schedule and restore drill
- [ ] S3 bucket versioning and cross-region replication policy
- [ ] Chaos testing plan — kill Kafka broker, restart consumers, verify S3 batches complete and OpenSearch index stays consistent
- [ ] Documented RTO / RPO targets and recovery playbook

---

## Dependency Map

```
Phase 1 (Skeleton)
  └─ Phase 2 (Kafka Ingest: topic + REST endpoint)
       └─ Phase 3 (Leader Election: Redis lock, LeaderAwareScheduler, KafkaLagMonitor)
            └─ Phase 4 (Kafka Consumer: batch listener, SASL, manual ack, retry/DLT)
                 └─ Phase 5 (S3 Storage: S3Client abstraction, Autoconfiguration, Step Builders, ZSTD, Hive keys)
                 └─ Phase 6 (OpenSearch Schema Manager: OsAdminClient, OsDocumentClient, @OsIndex, OsSchemaManager) ← also needs Phase 3
                      └─ Phase 7 (OpenSearch Storage: EventDocument, ILM hot→UltraWarm, aliases, BulkIngester)
                           └─ Phase 8 (Metadata Search: boolean query language, search_after pagination, aggregations, @FieldName ORM, event-read REST)
                           └─ Phase 9 (Rules Engine: schema/rule CRUD, parallel eval, cache, OS rule results)
                                └─ Phase 10 (Ingest Benchmarking: end-to-end throughput baseline)
                      └─ Phase 8 also needs Phase 6 (OsDocumentClient.search() stub)
Phase 8 └─ Phase 11 (Payload Retrieval & API Contract)
              └─ Phase 12 (Frontend Event Explorer)
              └─ Phase 13 (Live Feed) ← also needs Phase 3
                   └─ Phase 14 (Dashboards)
                        └─ Phase 15 (Alerting)
Phase 16 (Auth) ← can start after Phase 1, applied across all phases
Phase 17 (Collaboration) ← needs Phase 14 + Phase 16
Phase 18 (Retention) ← needs Phase 5 + Phase 9
Phase 19 (Observability) ← can layer in at any phase
Phase 20 (DR) ← after Phase 19
```
