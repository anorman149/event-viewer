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

- [ ] Kafka topic provisioning via Spring Kafka `@ConfigurationProperties` — each topic plus its DLT configured from `application.yml` with name, partitions, and replication factor; `KafkaAdmin` creates all topics on startup
- [ ] REST ingest endpoint (`POST /event/v1/events`) with schema-less JSON body; publishes received events to `event-raw` topic
- [ ] Jackson configuration standardized across all Spring Boot apps — `write-dates-as-timestamps: false`, `fail-on-unknown-properties: false`, `default-property-inclusion: non_null`
- [ ] Virtual threads enabled on all Spring Boot MVC apps; `@Async` and `@Scheduled` executors configured on virtual thread pools; `ContextSnapshotTaskDecorator` in `libs/common` propagates Spring Security context, MDC, and Micrometer spans into every spawned thread
- [ ] Spring OAuth2 Resource Server added to all internal apps (`event-ingest`, `event-read`, `management`, `bff`); each validates the JWT using a committed dev RSA public key; gateway validates and forwards `Authorization` header unchanged; dev RSA key pair generated for local use
- [ ] Gateway Resilience4J: `CircuitBreaker` per downstream service route + adaptive `RateLimiter` per client keyed on JWT subject or API key

---

## Phase 3 — Storage Consumers

**Goal:** Events consumed from Kafka are persisted to all three stores and throughput is baselined.

- [ ] Kafka consumer → S3 batch writer: accumulate 5,000 events, flush as ZSTD-compressed file with Hive-style key (`year=/month=/day=/hour=/schema_type=/pod=/<uuid>.zst`)
- [ ] Kafka consumer → OpenSearch metadata index: one document per event with `s3_key`, `batch_offset`, `batch_length`, indexed metadata fields
- [ ] Kafka consumer → PostgreSQL: write ingest audit record (batch key, event count, flush timestamp) — no raw event data
- [ ] Ingest throughput benchmark — establish baseline, target path to 1M events/sec

---

## Phase 4 — Schema Registry

**Goal:** Event types are defined in PostgreSQL, versioned, and enforced at ingest.

- [ ] Schema definition model in PostgreSQL (name, version, Avro schema, compatibility mode)
- [ ] Confluent Schema Registry integration for Avro serialization on the Kafka wire
- [ ] REST API to register and retrieve schema types (`/api/v1/schemas`)
- [ ] Schema validation applied at ingest — reject malformed events with 422; route failures to dead-letter topic
- [ ] Schema versioning with backward-compatibility check on registration
- [ ] Admin UI page: schema browser (list, view, register new type)

---

## Phase 5 — Metadata Search

**Goal:** Events are queryable with full boolean search returning paginated metadata.

- [ ] OpenSearch query service — full boolean search (AND / NOT / OR), keyword, time-range, and schema-type filters
- [ ] REST search endpoint (`GET /search/v1/events?q=...&type=...&from=...&to=...`) returning metadata documents
- [ ] Pagination and cursor-based result streaming for large result sets

---

## Phase 6 — Payload Retrieval & API Contract

**Goal:** Raw event payloads are retrievable from S3; a typed API contract is published for frontend consumers.

- [ ] Raw payload retrieval: OpenSearch lookup → `s3_key` + byte range → fetch from S3 → ZSTD decompress → return event (`GET /search/v1/events/{id}/payload`)
- [ ] OpenAPI spec published and used to generate typed TypeScript client

---

## Phase 7 — Frontend Event Explorer

**Goal:** Users can find and inspect any event through the UI.

- [ ] Event list view — paginated, sortable table with boolean search/filter controls (AND / NOT / OR)
- [ ] Event detail panel — decoded raw payload (fetched from S3 via lookup), metadata, schema type, timestamps
- [ ] Type-safe API client layer from generated OpenAPI types
- [ ] Empty states, error boundaries, and loading skeletons
- [ ] Virtual scrolling for large result sets (no DOM thrash at 10k+ rows)

---

## Phase 8 — Real-Time Event Feed

**Goal:** Users see events arrive live without polling.

- [ ] Spring WebSocket gateway with STOMP broker
- [ ] Backend publishes inbound events to a STOMP topic post-Kafka consume (metadata only; no S3 round-trip on live feed)
- [ ] Live feed React component — auto-scrolling, pause/resume controls
- [ ] Event rate widget (events/sec, rolling 60-second window) sourced from OpenSearch aggregations
- [ ] Client-side stream filters (type, keyword) applied before render

---

## Phase 9 — Dashboards & Visualizations

**Goal:** Users can build and save custom visual layouts.

- [ ] Dashboard layout engine — draggable, resizable panel grid
- [ ] Time-series chart panel — event volume over configurable time window (OpenSearch date histogram aggregation)
- [ ] Event-type breakdown panel — pie / bar chart by schema type (OpenSearch terms aggregation)
- [ ] Dashboard persistence — save and load layouts in PostgreSQL
- [ ] Share dashboard by URL (read-only shareable link)

---

## Phase 10 — Alerting & Notifications

**Goal:** Users are notified when event patterns meet defined thresholds.

- [ ] Alert rule model in PostgreSQL — threshold (count, rate), time window, schema filter, boolean query expression
- [ ] Alert evaluation engine (Kafka Streams topology) — runs OpenSearch aggregation queries continuously
- [ ] Notification delivery — webhook and email channels
- [ ] Alert history and acknowledgement stored in PostgreSQL
- [ ] Alert management UI — create, edit, enable/disable, view history

---

## Phase 11 — Authentication & Access Control

**Goal:** Every action in the system is authenticated and authorized.

- [ ] Spring Security + JWT (access token + refresh token flow); user records in PostgreSQL
- [ ] User registration, login, and logout endpoints
- [ ] RBAC — roles: `Admin`, `Analyst`, `Viewer`; enforced on all endpoints
- [ ] API key issuance and validation for ingest clients (hashed, stored in PostgreSQL)
- [ ] Frontend auth flows — login page, token refresh, role-gated routes

---

## Phase 12 — Collaboration & Export

**Goal:** Insights are shareable and portable.

- [ ] Saved searches — store named boolean query expressions in PostgreSQL, recall from UI
- [ ] Export event results to CSV and JSON (stream metadata from OpenSearch; fetch payloads from S3 on demand)
- [ ] Shareable dashboard links with optional expiry (stored in PostgreSQL)
- [ ] Audit log UI — view who searched, exported, or modified alert rules

---

## Phase 13 — Data Retention & Archiving

**Goal:** Old data ages out automatically per policy; nothing is silently lost.

- [ ] Retention policy model in PostgreSQL — configurable TTL per schema type
- [ ] Scheduled purge job — delete expired OpenSearch metadata documents; S3 objects expire via bucket lifecycle policy
- [ ] GDPR per-record erasure: delete OpenSearch document + overwrite S3 batch byte range (or tombstone in index)
- [ ] Restore workflow — re-index a historical S3 batch back into OpenSearch on demand (key prefix → decompress → bulk index)
- [ ] Retention policy UI — view and edit TTL per schema type

---

## Phase 14 — Observability & Operations

**Goal:** The platform can monitor itself and support on-call engineers.

- [ ] Structured JSON logging (Logback) with correlation IDs across services, shipped to OpenSearch
- [ ] Micrometer metrics → Prometheus scrape endpoint on all services
- [ ] Grafana dashboard — ingest lag, S3 flush rate, OpenSearch indexing lag, query latency, Kafka consumer lag
- [ ] OpenTelemetry distributed tracing → Jaeger (trace full ingest path: REST → Kafka → S3 + OpenSearch)
- [ ] Runbook for common failure scenarios (Kafka lag, OpenSearch disk pressure, S3 flush backlog, DB locks)

---

## Phase 15 — Resilience & Disaster Recovery

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
       └─ Phase 3 (Storage Consumers: S3 + OpenSearch + PostgreSQL audit)
            └─ Phase 4 (Schema Registry)
            └─ Phase 5 (Metadata Search)
                 └─ Phase 6 (Payload Retrieval & API Contract)
                      └─ Phase 7 (Explorer UI)
                      └─ Phase 8 (Live Feed)  ← also needs Phase 3
                           └─ Phase 9 (Dashboards — OpenSearch aggregations)
                                └─ Phase 10 (Alerting — OpenSearch + PostgreSQL rules)
Phase 11 (Auth) ← can start after Phase 1, applied across all phases
Phase 12 (Collaboration) ← needs Phase 9 + Phase 11
Phase 13 (Retention) ← needs Phase 3 + Phase 4
Phase 14 (Observability) ← can layer in at any phase
Phase 15 (DR) ← after Phase 14
```
