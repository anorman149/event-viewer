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

- [ ] `HiveKeyBuilder` in `libs/s3-lib` — constructs `events/year=YYYY/month=MM/day=DD/hour=HH/schema_type=<type>/pod=<pod>/<uuid>.zst` from partition params; pod name injected from `MY_POD_NAME` env var
- [ ] `S3Client` in `libs/s3-lib` — single abstraction over the AWS S3 SDK; context holds bucket, region, and prefix; exposes three operations: `get`, `create`, `delete`; each operation uses a Step Builder pattern to guide callers through required parameters (e.g., `s3Client.create().key(hiveKey).body(bytes).execute()`); no leaky SDK types in public return values
- [ ] Per-event ZSTD compression — each event is individually compressed (ZSTD level-3) before being passed to `S3Client.create()`; the stored file is a flat sequence of independently-compressed blobs; per-event compressed byte offset and length are tracked so `S3Client.get()` can retrieve and decompress exactly one event via byte-range without reading the full file
- [ ] Spring Autoconfiguration — `S3AutoConfiguration` in `libs/s3-lib` configures an `S3Client` bean from `S3Properties` (`s3.bucket`, `s3.region`, `s3.prefix`, `s3.endpoint-override` for LocalStack); wired via `AutoConfiguration.imports` so any app adding the dependency gets the bean automatically; connection pool size, request timeout, and retry strategy (SDK default + Resilience4J) configurable via `S3Properties`
- [ ] Security — IAM role / instance profile in prod; LocalStack dummy credentials (`AWS_ACCESS_KEY_ID=test`) in dev; credentials never committed; S3 bucket policy enforces server-side encryption at rest
- [ ] `@Timed(histogram=true)` on `S3Client.create()`, `S3Client.get()`, and `S3Client.delete()`; `DistributionSummary` for bytes written and bytes read; `Counter` for S3 PUT failures, S3 GET failures, and total operations
- [ ] Unit tests (mock S3 client) + itest against LocalStack: write ZSTD-compressed events, verify object exists, verify per-event byte offsets are correct, verify each event is independently decompressible via byte-range get

---

## Phase 6 — OpenSearch Schema Manager

**Goal:** A Liquibase-style schema manager library for OpenSearch provides ordered, idempotent, leader-gated migrations at startup; document classes declare their index, template, and alias metadata via annotation; all subsequent OpenSearch phases consume this library.

- [ ] Two client interfaces in `libs/opensearch-lib` using the OpenSearch Java Client — `OsAdminClient` (createIndex, deleteIndex, indexExists, refreshIndex, createTemplate, templateExists, clusterSettings) and `OsDocumentClient` (save, get, deleteByQuery, search — search is a stub; full query logic added in Phase 10)
- [ ] `@OsIndex` annotation — attributes: `indexPattern`, `templateName`, `writeAlias`, `readAlias`; applied directly to document record classes; `OsSchemaRegistry` classpath-scans on startup and caches `OsIndexMetadata` per annotated class; client methods call `registry.getMetadata(MyDoc.class)` to resolve alias names at runtime without hardcoding strings
- [ ] `OsDocumentClient.save()` backed by `BulkIngester` from the OpenSearch Java Client — configurable flush threshold and interval; callers pass a list of documents and the registry supplies the correct write alias automatically
- [ ] `OsMigration` abstraction — ordered, named, idempotent migration steps (each step checks `templateExists` / `indexExists` before acting); steps declared as Spring `@Bean` methods inside `@Configuration` classes; `apps/event-ingest` owns all migration configs for its indices
- [ ] `OsSchemaManager` — collects all `OsMigration` beans at startup, sorts by declared order, executes sequentially; gated by `LeaderAwareScheduler` (Phase 3) so only the leader pod runs migrations; followers skip and rely on the leader having applied changes
- [ ] itest — two app instances against a shared OpenSearch; verify only one applies migrations; restart both and verify idempotency (no errors re-running against an already-existing template / index)

---

## Phase 7 — OpenSearch Storage

**Goal:** Event metadata is indexed in OpenSearch via the Phase 6 schema manager infrastructure with production-grade ILM (hot SSD NVMe → UltraWarm → delete), read/write alias routing, and bulk ingestion via BulkIngester.

- [ ] `EventDocument` record in `apps/event-ingest` annotated `@OsIndex(indexPattern="events-*", templateName="events-template", writeAlias="events_write", readAlias="events_read")` — fields: `eventId`, `schemaType`, `timestamp`, `s3Key`, `batchOffset`, `batchLength`, `ruleResults` (list of `{ruleId, status}` objects)
- [ ] Index template `events-template` — covers pattern `events-*`; `dynamic: false`, `date_detection: false`; explicit keyword/date/integer mappings for all fixed fields; initial index created with date math name `<events-{now/d}-000001>` (suffix increments on each rollover); template declares `events_write` and `events_read` aliases
- [ ] ILM policy — hot tier (SSD NVMe): rollover at 130 GB or 12 hours; transition to UltraWarm after rollover; UltraWarm retention: 4 days, then auto-delete; no cold tier; policy attached to the template via `index.lifecycle.name`
- [ ] S3 snapshots — use the default AWS-managed automated snapshot policy (hourly snapshots, 14-day retention on the AWS-managed bucket); 14-day retention comfortably outlasts the 4.5-day hot + warm lifecycle; no custom snapshot repository needed
- [ ] Replica count configurable — `opensearch.index.replicas: 0` for single-node dev; `1` for prod minimum; applied via an `OsMigration` cluster settings step
- [ ] `EventDocumentIndexer` in `apps/event-ingest` — calls `OsDocumentClient.save()` (BulkIngester-backed); async flush; Resilience4J circuit breaker; retry with backoff on transient failures
- [ ] `@Timed(histogram=true)` on bulk index; `DistributionSummary` for documents per bulk request; `Counter` for index failures and circuit breaker opens
- [ ] itest against OpenSearch in Docker Compose: index events, verify field mappings, verify ILM policy applied, verify write alias routes to the active index, verify read alias resolves correctly

---

## Phase 8 — Rules Engine

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

## Phase 9 — Ingest Benchmarking

**Goal:** End-to-end throughput is baselined and the primary bottleneck on the path to 1M events/sec is identified and documented.

- [ ] Load-test harness in `apps/event-ingest/src/itest` — configurable event count (default 1M), payload size distribution, consumer concurrency, batch size
- [ ] Measure: events/sec from first Kafka produce to last S3 flush + OpenSearch index; S3 PUT throughput; OpenSearch bulk index throughput; Kafka consumer poll latency; rule evaluation overhead
- [ ] JVM tuning sweep — test with default JVM settings, then with tuned GC (`-XX:+UseZGC`), then with tuned virtual thread pool sizes; document delta
- [ ] Bottleneck identification — record which stage is the ceiling at each concurrency level
- [ ] Document results in `specs/2026-05-13-storage-consumers/benchmark-results.md` — hardware spec, JVM version, settings used, events/sec achieved per stage
- [ ] Target: establish baseline; remediate top bottleneck if below 100K events/sec; document path to 1M events/sec with horizontal scaling model

---

## Phase 10 — Metadata Search

**Goal:** Events are queryable with full boolean search returning paginated metadata.

- [ ] OpenSearch query service — full boolean search (AND / NOT / OR), keyword, time-range, and schema-type filters; implements the search stub defined in Phase 6 `OsDocumentClient`
- [ ] REST search endpoint (`GET /search/v1/events?q=...&type=...&from=...&to=...`) returning metadata documents
- [ ] Pagination and cursor-based result streaming for large result sets

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
                           └─ Phase 8 (Rules Engine: schema/rule CRUD, parallel eval, cache, OS rule results)
                                └─ Phase 9 (Ingest Benchmarking: end-to-end throughput baseline)
                                     └─ Phase 10 (Metadata Search: boolean query, pagination)
                                          └─ Phase 11 (Payload Retrieval & API Contract)
                                               └─ Phase 12 (Frontend Event Explorer)
                                               └─ Phase 13 (Live Feed) ← also needs Phase 3
                                                    └─ Phase 14 (Dashboards)
                                                         └─ Phase 15 (Alerting)
Phase 16 (Auth) ← can start after Phase 1, applied across all phases
Phase 17 (Collaboration) ← needs Phase 14 + Phase 16
Phase 18 (Retention) ← needs Phase 5 + Phase 8
Phase 19 (Observability) ← can layer in at any phase
Phase 20 (DR) ← after Phase 19
```
