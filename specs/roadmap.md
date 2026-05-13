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

- [ ] K8s leader election via Coordination API (Lease objects) using official Kubernetes Java client; isolated in new `libs/leader` library (keeps heavy `client-java` transitive deps out of `libs/common`)
- [ ] `LeaderElectionService` — acquires and renews a Lease; publishes `LeaderElectionEvent` (ACQUIRED / RELINQUISHED) to the Spring `ApplicationEventPublisher`; exposes `isLeader()` for synchronous checks
- [ ] `LeaderAwareScheduler` abstraction — wraps any `Runnable` to only execute on the current leader pod; no-op on followers
- [ ] Non-K8s fallback — `kubernetes.leader-election.enabled=false` causes `isLeader()` to always return `true` (single-node dev / CI mode)
- [ ] `KafkaLagMonitor` in `apps/event-ingest` — `@Scheduled` bean (interval: 60 s); runs via `LeaderAwareScheduler`; queries Kafka `AdminClient` for consumer group lag on all configured topics; emits `kafka.consumer.lag` gauge per (topic, partition, consumer-group) tuple
- [ ] K8s RBAC manifests in `infra/k8s/` — `ServiceAccount`, `Role` (leases: get/create/update), `RoleBinding`; `MY_POD_NAME` and `MY_POD_NAMESPACE` injected via downward API; `infra/` is the root folder for all infrastructure-as-code
- [ ] Local K8s deployment test — `event-ingest` deployed at 2 replicas; verify single leader, verify leadership transfer after pod kill
- [ ] Observability — `leader_election_acquisitions_total` counter, `leader_election_is_leader` gauge (1/0), `kafka_consumer_lag` gauge per partition

---

## Phase 4 — Kafka Consumer

**Goal:** Events are consumed from 4 sharded topics with production-grade configuration: dynamic listeners from properties (no `@KafkaListener`), static membership for pod-restart resilience, cooperative sticky rebalancing, manual batch acknowledgment, SASL security, and a full retry → DLT pipeline. Parse failures are metered and skipped — DLT stays clean.

- [ ] Topic topology — 4 topics (`event-raw-1` … `event-raw-4`), 80 partitions × 1 replica (prod); 2 partitions (local); DLT topics provisioned automatically at `{topic}.DLT`; all topics created via `KafkaAdmin` from `EventConsumerProperties`
- [ ] Dynamic container creation — one `ConcurrentMessageListenerContainer` per topic, created programmatically from properties in `EventConsumerContainerFactory` (implements `SmartLifecycle`); no `@KafkaListener` annotations; concurrency configurable (default 20 prod, 1 local)
- [ ] Consumer settings — `CooperativeStickyAssignor`, `group.instance.id = {MY_POD_NAME}-{topic}-{threadIndex}` for static membership, `AckMode.MANUAL_IMMEDIATE`, `BatchAcknowledgingMessageListener` (both main and DLT); concurrency computed at runtime via `computeConcurrency(partitionsPerTopic, INGEST_POD_COUNT_env)`; virtual thread executor per container; `session.timeout.ms=45000`, `max.poll.interval.ms=300000`, `max.poll.records=500`, `heartbeat.interval.ms=3000`, fetch settings tuned for 50 MB/request
- [ ] SASL/SCRAM-SHA-512 — plaintext in dev; `security.protocol=SASL_SSL` in prod; `sasl.jaas.config` injected via `KAFKA_SASL_JAAS_CONFIG` env var; never committed
- [ ] `EventBatchListener` — per-record parse: `JsonProcessingException` caught, metered (`kafka.consumer.parse.failures`), skipped; valid records forwarded to `IngestPipelineService.process()` stub; `acknowledge()` called once per batch regardless
- [ ] `DefaultErrorHandler` — 3-attempt exponential backoff (1 s → 2 s → 4 s); `DeadLetterPublishingRecoverer` routes to `{topic}.DLT`; `JsonProcessingException` is non-retryable
- [ ] `DltConsumerContainerFactory` — one DLT container per topic (concurrency = 1, group `event-ingest-dlt-group`); `DltBatchMessageListener` retries up to 100 times (5 s fixed); `dlt.recovered` / `dlt.exhausted` counters
- [ ] Kafka lag metrics from Phase 3 `KafkaLagMonitor` registered for both `event-ingest-group` and `event-ingest-dlt-group`

---

## Phase 5 — S3 Storage

**Goal:** Events are flushed as individually ZSTD-compressed records in Hive-partitioned batch files to S3 with full performance tuning, per-event byte-range tracking, and production-grade security.

- [ ] `HiveKeyBuilder` in `libs/s3-lib` — constructs `events/year=YYYY/month=MM/day=DD/hour=HH/schema_type=<type>/pod=<pod>/<uuid>.zst` from partition params; pod name injected from `MY_POD_NAME` env var
- [ ] Per-event ZSTD compression — each event is individually compressed (ZSTD level-3) and appended to the S3 object; the file is a flat sequence of independently-compressed blobs, not a single compressed stream; per-event compressed byte offset and length are recorded at write time so a GET Range can retrieve and decompress exactly one event without reading the rest of the file
- [ ] `S3BatchWriter` — writes one Kafka consumer batch directly to S3 with no additional buffering layer; batch size is whatever Kafka delivers (up to `max.poll.records` — if Kafka returns 20 events, 20 events are written immediately, no waiting); returns `S3BatchResult` (s3Key, List of per-event compressed byte offsets + lengths, total compressed/uncompressed sizes)
- [ ] `S3RangeReader` — accepts `List<S3EventKey>` (each entry: s3Key, byteOffset, byteLength); spawns one virtual thread per entry to execute parallel GET Range calls; each thread decompresses its individually-compressed event blob independently; no shared thread pool — virtual threads are created per invocation and are naturally bounded by the caller's list size
- [ ] `Event Read` REST API `loadEvents` parameter — boolean query parameter (default `false`); when `true`, pagination is hard-capped at 25 results per page and raw event payloads are fetched from S3 via `S3RangeReader` (capping to 25 bounds the maximum virtual thread count per request); when `false`, only OpenSearch metadata is returned with no S3 calls
- [ ] AWS S3 client config — connection pool size, request timeout, retry strategy (SDK default + Resilience4J); transfer manager for multipart upload on batches above a configurable size threshold
- [ ] Security — IAM role / instance profile in prod; LocalStack dummy credentials (`AWS_ACCESS_KEY_ID=test`) in dev; credentials never committed; S3 bucket policy enforces server-side encryption at rest
- [ ] `DistributionSummary` for batch event count, compressed bytes, uncompressed bytes; `@Timed(histogram=true)` on `S3BatchWriter.flush()` and `S3RangeReader.fetch()`; `Counter` for flush count, S3 PUT failures, S3 GET Range calls
- [ ] Unit tests (mock S3 client) + itest against LocalStack: flush a Kafka-batch-sized set of events (varying sizes), verify object exists, verify per-event byte offsets correct, verify each event independently decompressible by range

---

## Phase 6 — OpenSearch Storage

**Goal:** Event metadata is indexed in OpenSearch with a production-grade index configuration including ILM, S3 snapshot policy, replica tuning, and surrogate key mapping for rule results.

- [ ] `EventMetadataDocument` record in `libs/opensearch-lib` — fixed fields: `eventId`, `schemaType`, `schemaVersion`, `timestamp`, `ingestTs`, `s3Key`, `batchOffset`, `batchLength`, `ruleSurrogateKey` (int), `ruleResult` (enum: `PASS` / `FAIL` / `NOT_APPLICABLE` / `ERROR`)
- [ ] `BatchSummaryDocument` record — `s3Key`, `schemaType`, `schemaVersion`, `eventCount`, `firstEventTs`, `lastEventTs`, `compressedSizeBytes`, `uncompressedSizeBytes`, `pod`
- [ ] Index template `events-metadata-*` — `dynamic: false`, `date_detection: false`; explicit keyword/date/integer mappings for all fixed fields; `events-batches` index for batch summary documents
- [ ] Surrogate key mapping — `RuleSurrogateKeyRegistry` maintains a `ConcurrentHashMap<UUID, Integer>` (rule UUID → stable integer); integers written to OpenSearch as `rule_id_key`; registry persisted to PostgreSQL on first assignment; loaded at startup
- [ ] ILM policy — rollover at 50 GB or 7 days; warm phase (force merge to 1 segment) at 1 day; cold at 30 days; delete at configurable TTL (default 90 days)
- [ ] S3 snapshot repository — `event-viewer-os-snapshots` bucket; snapshot schedule (daily); restore workflow documented; LocalStack in dev
- [ ] Replica count configurable — `opensearch.index.replicas: 0` for single-node dev; `1` for prod minimum
- [ ] `EventMetadataIndexer` — bulk indexing with configurable batch size; async flush; Resilience4J circuit breaker around OpenSearch client; retry with backoff on transient failures
- [ ] `@Timed(histogram=true)` on `bulkIndex()`; `DistributionSummary` for documents per bulk request; `Counter` for index failures and circuit breaker opens
- [ ] itest against OpenSearch in Docker Compose: index 5,000 documents, verify field mappings, verify ILM policy applied, verify surrogate key stored correctly

---

## Phase 7 — Rules Engine

**Goal:** Configurable validation rules are defined in PostgreSQL, cached in the ingest service, and evaluated in parallel against incoming events; rule results are indexed in OpenSearch per event.

- [ ] `Schema` entity in PostgreSQL (`apps/management`) — `schema_id`, `name`, `version`, `description`, `created_at`; Flyway migration; JPA entity; REST CRUD (`/api/v1/schemas`)
- [ ] `Rule` entity in PostgreSQL — `rule_id`, `schema_id` (FK), `name`, `rule_type` (enum: `FIELD_EXISTS`, `FIELD_EQUALS`, `FIELD_REGEX`, `FIELD_RANGE`, `REQUIRED_FIELD_LIST`), `condition_expression` (JSON), `severity` (enum: `INFO` / `WARN` / `ERROR`), `enabled`; REST CRUD (`/api/v1/rules`, `/api/v1/schemas/{name}/rules`)
- [ ] `RuleSurrogateKeyRegistry` extended — assigns surrogate int to each `rule_id` UUID; persisted to PostgreSQL; loaded at `event-ingest` startup via management app REST call
- [ ] Rule cache in `apps/event-ingest` — Caffeine cache keyed by schema name; TTL configurable (`rule-cache.ttl-seconds`); refreshed on `@Scheduled` interval (leader-aware via Phase 3); `rule_cache.hits` / `rule_cache.misses` counters
- [ ] `RuleEvaluationEngine` — groups 5,000-event batch by `schemaType`; submits one virtual-thread task per schema group; each task evaluates all enabled rules for that schema against all events in the group; returns `List<RuleEvaluationResult>` per event
- [ ] `RuleEvaluationResult` — `eventId`, `ruleId`, `ruleSurrogateKey`, `ruleResult` (enum), `evaluationTimeNs`
- [ ] Failed-rule events are NOT discarded — indexed in OpenSearch with `ruleResult = FAIL`; only system/parse errors route to dead-letter
- [ ] `@Timed(histogram=true)` on `RuleEvaluationEngine.evaluate()`; `Counter` for rules evaluated, rule failures per `rule_id`; `DistributionSummary` for events per batch group
- [ ] Unit tests for each rule type; itest: register schema + rule via management API, publish events, consume, assert `ruleResult` in OpenSearch index

---

## Phase 8 — Ingest Benchmarking

**Goal:** End-to-end throughput is baselined and the primary bottleneck on the path to 1M events/sec is identified and documented.

- [ ] Load-test harness in `apps/event-ingest/src/itest` — configurable event count (default 1M), payload size distribution, consumer concurrency, batch size
- [ ] Measure: events/sec from first Kafka produce to last S3 flush + OpenSearch index; S3 PUT throughput; OpenSearch bulk index throughput; Kafka consumer poll latency; rule evaluation overhead
- [ ] JVM tuning sweep — test with default JVM settings, then with tuned GC (`-XX:+UseZGC`), then with tuned virtual thread pool sizes; document delta
- [ ] Bottleneck identification — record which stage is the ceiling at each concurrency level
- [ ] Document results in `specs/2026-05-13-storage-consumers/benchmark-results.md` — hardware spec, JVM version, settings used, events/sec achieved per stage
- [ ] Target: establish baseline; remediate top bottleneck if below 100K events/sec; document path to 1M events/sec with horizontal scaling model

---

## Phase 9 — Metadata Search

**Goal:** Events are queryable with full boolean search returning paginated metadata.

- [ ] OpenSearch query service — full boolean search (AND / NOT / OR), keyword, time-range, and schema-type filters
- [ ] REST search endpoint (`GET /search/v1/events?q=...&type=...&from=...&to=...`) returning metadata documents
- [ ] Pagination and cursor-based result streaming for large result sets

---

## Phase 10 — Payload Retrieval & API Contract

**Goal:** Raw event payloads are retrievable from S3; a typed API contract is published for frontend consumers.

- [ ] Raw payload retrieval: OpenSearch lookup → `s3_key` + byte range → fetch from S3 → ZSTD decompress → return event (`GET /search/v1/events/{id}/payload`)
- [ ] OpenAPI spec published and used to generate typed TypeScript client

---

## Phase 11 — Frontend Event Explorer

**Goal:** Users can find and inspect any event through the UI.

- [ ] Event list view — paginated, sortable table with boolean search/filter controls (AND / NOT / OR)
- [ ] Event detail panel — decoded raw payload (fetched from S3 via lookup), metadata, schema type, timestamps
- [ ] Type-safe API client layer from generated OpenAPI types
- [ ] Empty states, error boundaries, and loading skeletons
- [ ] Virtual scrolling for large result sets (no DOM thrash at 10k+ rows)

---

## Phase 12 — Real-Time Event Feed

**Goal:** Users see events arrive live without polling.

- [ ] Spring WebSocket gateway with STOMP broker
- [ ] Backend publishes inbound events to a STOMP topic post-Kafka consume (metadata only; no S3 round-trip on live feed)
- [ ] Live feed React component — auto-scrolling, pause/resume controls
- [ ] Event rate widget (events/sec, rolling 60-second window) sourced from OpenSearch aggregations
- [ ] Client-side stream filters (type, keyword) applied before render

---

## Phase 13 — Dashboards & Visualizations

**Goal:** Users can build and save custom visual layouts.

- [ ] Dashboard layout engine — draggable, resizable panel grid
- [ ] Time-series chart panel — event volume over configurable time window (OpenSearch date histogram aggregation)
- [ ] Event-type breakdown panel — pie / bar chart by schema type (OpenSearch terms aggregation)
- [ ] Dashboard persistence — save and load layouts in PostgreSQL
- [ ] Share dashboard by URL (read-only shareable link)

---

## Phase 14 — Alerting & Notifications

**Goal:** Users are notified when event patterns meet defined thresholds.

- [ ] Alert rule model in PostgreSQL — threshold (count, rate), time window, schema filter, boolean query expression
- [ ] Alert evaluation engine (Kafka Streams topology) — runs OpenSearch aggregation queries continuously
- [ ] Notification delivery — webhook and email channels
- [ ] Alert history and acknowledgement stored in PostgreSQL
- [ ] Alert management UI — create, edit, enable/disable, view history

---

## Phase 15 — Authentication & Access Control

**Goal:** Every action in the system is authenticated and authorized.

- [ ] Spring Security + JWT (access token + refresh token flow); user records in PostgreSQL
- [ ] User registration, login, and logout endpoints
- [ ] RBAC — roles: `Admin`, `Analyst`, `Viewer`; enforced on all endpoints
- [ ] API key issuance and validation for ingest clients (hashed, stored in PostgreSQL)
- [ ] Frontend auth flows — login page, token refresh, role-gated routes

---

## Phase 16 — Collaboration & Export

**Goal:** Insights are shareable and portable.

- [ ] Saved searches — store named boolean query expressions in PostgreSQL, recall from UI
- [ ] Export event results to CSV and JSON (stream metadata from OpenSearch; fetch payloads from S3 on demand)
- [ ] Shareable dashboard links with optional expiry (stored in PostgreSQL)
- [ ] Audit log UI — view who searched, exported, or modified alert rules

---

## Phase 17 — Data Retention & Archiving

**Goal:** Old data ages out automatically per policy; nothing is silently lost.

- [ ] Retention policy model in PostgreSQL — configurable TTL per schema type
- [ ] Scheduled purge job — delete expired OpenSearch metadata documents; S3 objects expire via bucket lifecycle policy
- [ ] GDPR per-record erasure: delete OpenSearch document + overwrite S3 batch byte range (or tombstone in index)
- [ ] Restore workflow — re-index a historical S3 batch back into OpenSearch on demand (key prefix → decompress → bulk index)
- [ ] Retention policy UI — view and edit TTL per schema type

---

## Phase 18 — Observability & Operations

**Goal:** The platform can monitor itself and support on-call engineers.

- [ ] Structured JSON logging (Logback) with correlation IDs across services, shipped to OpenSearch
- [ ] Micrometer metrics → Prometheus scrape endpoint on all services
- [ ] Grafana dashboard — ingest lag, S3 flush rate, OpenSearch indexing lag, query latency, Kafka consumer lag
- [ ] OpenTelemetry distributed tracing → Jaeger (trace full ingest path: REST → Kafka → S3 + OpenSearch)
- [ ] Runbook for common failure scenarios (Kafka lag, OpenSearch disk pressure, S3 flush backlog, DB locks)

---

## Phase 19 — Resilience & Disaster Recovery

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
       └─ Phase 3 (Leader Election: K8s lease, LeaderAwareScheduler, KafkaLagMonitor)
            └─ Phase 4 (Kafka Consumer: batch listener, SASL, manual ack, retry/DLT)
                 └─ Phase 5 (S3 Storage: ZSTD batch writer, Hive keys, byte-range GET)
                 └─ Phase 6 (OpenSearch Storage: metadata index, ILM, snapshots, surrogates)
                      └─ Phase 7 (Rules Engine: schema/rule CRUD, parallel eval, cache, OS rule results)
                           └─ Phase 8 (Ingest Benchmarking: end-to-end throughput baseline)
                                └─ Phase 9 (Metadata Search: boolean query, pagination)
                                     └─ Phase 10 (Payload Retrieval & API Contract)
                                          └─ Phase 11 (Frontend Event Explorer)
                                          └─ Phase 12 (Live Feed) ← also needs Phase 3
                                               └─ Phase 13 (Dashboards)
                                                    └─ Phase 14 (Alerting)
Phase 15 (Auth) ← can start after Phase 1, applied across all phases
Phase 16 (Collaboration) ← needs Phase 13 + Phase 15
Phase 17 (Retention) ← needs Phase 5 + Phase 7
Phase 18 (Observability) ← can layer in at any phase
Phase 19 (DR) ← after Phase 18
```
