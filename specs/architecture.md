# Architecture

Event Viewer is a **monorepo** containing multiple independently deployable Spring Boot applications and shared Java libraries. This document is the authoritative reference for what lives where and why.

---

## Monorepo Layout

```
event-viewer/
  apps/
    gateway/          ← Spring Cloud Gateway (single external entry point)
    bff/              ← Backend for Frontend (UI aggregation + response shaping)
    event-ingest/     ← Kafka consumer + S3 batch writer + OpenSearch indexer
    event-read/       ← OpenSearch query + S3 payload retrieval
    management/       ← CRUD for schemas, dashboards, alert rules, users/RBAC
  libs/
    event-api/        ← shared models, DTOs, OpenAPI spec
    opensearch-lib/   ← OpenSearch abstraction layer
    s3-lib/           ← AWS S3 / LocalStack abstraction layer
    common/           ← logging config, exceptions, shared utilities
  frontend/           ← React + TypeScript (Vite, NOT a Gradle subproject)
  specs/              ← constitution and feature specs
  docker-compose.yml
  settings.gradle     ← declares all apps/* and libs/* as subprojects
  build.gradle        ← root: shared BOM, version catalog
```

---

## Applications

### `apps/gateway` — API Gateway

**Technology:** Spring Cloud Gateway (reactive)

Single entry point for all external traffic. Contains no business logic. Routes requests to downstream apps via configured predicates and applies cross-cutting concerns as filter chains.

| Responsibility | Implementation |
|---|---|
| Routing | `/event/v1/**` → event-ingest · `/search/v1/**` → event-read · `/api/v1/**` → management · `/bff/v1/**` → bff |
| Authentication | JWT validation filter (verify token signature + expiry; reject 401 before downstream); forwards `Authorization: Bearer` header unchanged to all downstream services so each can independently re-validate |
| Rate limiting | Adaptive token bucket per client keyed on JWT subject or API key; backed by Resilience4J `RateLimiter` (in-memory; Redis-backed in production) |
| Circuit breaker | Resilience4J `CircuitBreaker` per downstream service route; opens on consecutive failures; returns `503` to the caller while open to prevent cascade failures |
| Observability | Request/response logging filter (correlation ID injected into `X-Correlation-ID` header + MDC) |
| TLS termination | Handled at gateway; downstream services communicate over plain HTTP inside the cluster |

**Does NOT own:** any database, any event data, any business rules.

---

### `apps/event-ingest` — Event Ingest Service

**Technology:** Spring Boot 3.x, Spring Kafka, AWS SDK v2, OpenSearch Java client

Owns the full write path: accepts events, produces them to Kafka for durability, consumes them back off Kafka, accumulates into batches, and flushes to S3 and OpenSearch.

```
POST /event/v1/events
        │
        ▼
  Kafka Producer  ──►  Kafka Topic (event-raw)
                                │
                                ▼
                       Kafka Consumer (same app)
                          │            │
                          ▼            ▼
                   Batch Buffer     Schema Validation
                   (5,000 events)   (rejects → DLT)
                          │
                     Batch Flush
                      │        │
                      ▼        ▼
                     S3     OpenSearch
                  (ZSTD)   (metadata + s3_key)
```

**Why consumer and writer are in the same app:** The batch accumulator holds in-flight events in memory between consumer poll and S3 flush. Splitting consumer from writer would require shipping accumulated batches (up to 5MB) over the network, introducing transfer cost and a new failure mode at the exact hot path. The CQRS boundary is **ingest vs. read**, not consumer vs. writer.

**Scaling:** Each pod is assigned a subset of Kafka partitions. Each pod writes to its own S3 key prefix (`pod=<k8s-pod-name>/`) — no write coordination needed.

**Security:** Spring OAuth2 Resource Server; validates JWT signature and expiry on every inbound request using the platform RSA public key; rejects unauthenticated calls with `401` regardless of whether the request arrived via the gateway.

**Depends on libs:** `event-api`, `s3-lib`, `opensearch-lib`, `common`

---

### `apps/event-read` — Event Read Service

**Technology:** Spring Boot 3.x, AWS SDK v2, OpenSearch Java client

Owns the full read path: boolean search over OpenSearch metadata, and raw payload retrieval via S3 byte-range fetch.

| Endpoint | Behaviour |
|---|---|
| `GET /search/v1/events?q=...&type=...&from=...&to=...` | OpenSearch boolean query; returns paginated metadata documents |
| `GET /search/v1/events/{id}/payload` | OpenSearch lookup → `s3_key` + `batch_offset` + `batch_length` → S3 byte-range fetch → ZSTD decompress → return raw event |

**Traffic profile:** Read traffic is 1,000–10,000 RPS. Write traffic is 1M events/second inbound — but because events are batched to 5,000 before flushing, S3 and OpenSearch only see ~200 RPS of actual write I/O. Write event volume dwarfs read RPS; batching keeps write I/O manageable.

**Why read and write are separate apps:** Independent scaling (event-ingest scales on Kafka partition count; event-read scales on query concurrency), independent deployment, and clean separation of the OpenSearch indexing surface from the query surface.

**Security:** Spring OAuth2 Resource Server; validates JWT on every inbound request using the platform RSA public key; rejects unauthenticated calls with `401`.

**Depends on libs:** `event-api`, `s3-lib`, `opensearch-lib`, `common`

---

### `apps/management` — Management Service

**Technology:** Spring Boot 3.x, Spring Data JPA, PostgreSQL

Owns all CRUD management of system concepts. This is the only app that writes to PostgreSQL. Exposes a REST API consumed by the BFF (for the UI) and by the gateway (for admin API clients).

| Domain | Endpoints | Notes |
|---|---|---|
| Schema / event types | `GET/POST/PUT /api/v1/schemas` | Name, version, Avro schema, compatibility mode |
| Dashboard layouts | `GET/POST/PUT/DELETE /api/v1/dashboards` | Panel grid config, saved filter state |
| Alert rules | `GET/POST/PUT/DELETE /api/v1/alerts` | Threshold, time window, schema filter, notification channels |
| Users & RBAC | `GET/POST/PUT/DELETE /api/v1/users` | Accounts, role assignments |
| API keys | `POST/DELETE /api/v1/api-keys` | Hashed, scoped ingest keys |

**Security:** Spring OAuth2 Resource Server; validates JWT on every inbound request using the platform RSA public key; rejects unauthenticated calls with `401`.

**Depends on libs:** `event-api`, `common`

---

### `apps/bff` — Backend for Frontend

**Technology:** Spring Boot 3.x (WebFlux — reactive for fan-out aggregation)

The exclusive backend entry point for the React frontend. The browser never calls event-read, event-ingest, or management directly. The BFF aggregates internal service calls, shapes responses to match UI component contracts, and handles browser-specific concerns.

| Responsibility | Detail |
|---|---|
| Response aggregation | Combine OpenSearch search results (event-read) with schema metadata (management) in one frontend call |
| Response shaping | Map internal domain models to UI-optimized view models; the frontend is never exposed to raw backend DTOs |
| Session management | Handles browser cookie-based sessions and CSRF tokens (internal auth handoff with gateway JWT) |
| Fan-out | Parallel reactive calls to multiple services (WebFlux `Mono.zip` / `Flux.merge`) |

**Internal routing:** The BFF calls other services via internal cluster DNS (`http://event-read`, `http://management`, `http://event-ingest`) — it does **not** loop back through the gateway.

**Security:** Spring OAuth2 Resource Server; validates JWT on every inbound request. For outbound service-to-service calls the BFF forwards the JWT from the originating request in the downstream `Authorization` header.

**Depends on libs:** `event-api`, `common`

---

## Libraries

Libraries have no `main` class. They are imported as Gradle dependencies by apps that need them.

### `libs/event-api` — Shared Event Models & API Contract

- Java records for all shared domain types: `EventRecord`, `EventMetadata`, `BatchManifest`, `SchemaDefinition`
- OpenAPI 3.1 spec (Springdoc annotations); the generated spec is the contract consumed by the frontend TypeScript client
- Request/response DTOs shared across gateway and read/write services
- No I/O, no Spring beans, no infrastructure dependencies

### `libs/opensearch-lib` — OpenSearch Abstraction Layer

- Wraps the OpenSearch Java High-Level REST client
- Index lifecycle utilities (create, delete, alias swap for zero-downtime reindex)
- Query builders: boolean (AND / NOT / OR), time-range, keyword, terms aggregation, date histogram
- Bulk indexing helper with back-pressure and retry
- Document mappers: `EventDocument`, `BatchDocument`
- Configurable via Spring Boot `@ConfigurationProperties` (host, port, index prefix)

### `libs/s3-lib` — AWS S3 Abstraction Layer

- Wraps AWS SDK v2 S3 client
- LocalStack-compatible client factory: detects `AWS_ENDPOINT_OVERRIDE` env var and sets the endpoint URL accordingly — same code runs against LocalStack locally and real S3 in production
- **Key path builder:** constructs Hive-style S3 keys from event timestamp, schema type, and pod name
- **Batch writer:** serializes a list of `EventRecord` to NDJSON, compresses with ZSTD level 3, uploads to S3, returns `(s3_key, per-event offsets[])`
- **Batch reader:** byte-range GET from S3, ZSTD decompress, return single event by offset + length

### `libs/common` — Shared Utilities

- Logback JSON configuration (structured logging, correlation ID via MDC)
- `CorrelationIdFilter` — extracts or generates `X-Correlation-ID` and puts it in MDC
- Shared exception hierarchy (`EventViewerException`, `ValidationException`, `StorageException`)
- Spring Boot auto-configuration for the above (activated via `spring.factories` / `@AutoConfiguration`)

---

## Traffic Flow Diagram

```
  Browser (React UI)          API Clients (CLI, integrations)
         │                               │
         │  /bff/v1/**                   │  /event/v1/**
         │                               │  /search/v1/**
         │                               │  /api/v1/**
         └──────────────┬────────────────┘
                        ▼
               ┌──────────────┐
               │   gateway    │  Spring Cloud Gateway
               │              │  JWT auth · rate limit · routing
               └──────┬───────┘
                      │
        ┌─────────────┼──────────────┬───────────────┐
        │             │              │               │
        ▼             ▼              ▼               ▼
  ┌──────────┐ ┌───────────┐ ┌───────────┐ ┌────────────┐
  │   bff    │ │  event-   │ │  event-   │ │ management │
  │          │ │  ingest   │ │  read     │ │            │
  │ UI aggr. │ │           │ │           │ │ schemas    │
  │ response │ │ REST →    │ │ OpenSearch│ │ dashboards │
  │ shaping  │ │ Kafka →   │ │ query     │ │ alerts     │
  └────┬─────┘ │ S3 batch  │ │ → S3     │ │ users/RBAC │
       │       │ → OS idx  │ │   fetch  │ └─────┬──────┘
       │       └─────┬─────┘ └────┬─────┘       │
       │             │            │              │
       │  (internal) │            │              │
       └──calls──────┤            │              │
                     │            │              │
                     ▼            ▼              ▼
                   Kafka    OpenSearch        PostgreSQL
                     │      S3 (lookup)
                     ▼      ▼
                    S3 ◄────┘
               (raw batches,
                ZSTD, Hive key)
```

**Write volume:** 1M events/sec inbound → ~200 S3 + OpenSearch write ops/sec (5,000-event batches)
**Read volume:** 1,000–10,000 RPS across event-read + bff + management

---

## Platform Standards

These conventions apply to every `apps/*` Spring Boot application and must not be regressed in any subsequent phase.

### Internal Service Security Model

The gateway is **not** the sole auth boundary. Every internal service independently validates the JWT on every inbound request.

```
Client → Gateway (validate JWT + forward Authorization header)
                 → event-ingest  (re-validate JWT)
                 → event-read    (re-validate JWT)
                 → management    (re-validate JWT)
                 → bff           (re-validate JWT → forward to downstream)
```

Each service configures `spring-boot-starter-oauth2-resource-server` with the platform RSA public key:

```yaml
spring:
  security:
    oauth2:
      resourceserver:
        jwt:
          public-key-location: classpath:keys/platform-public.pem
```

The public key (`platform-public.pem`) is committed to `src/main/resources/keys/` in each app. The corresponding private key is never committed; it lives in `docker-compose.env` (gitignored) for local dev JWT generation and in a secrets manager in production.

**Why not trust the gateway:** If a gateway misconfiguration, a compromised internal pod, or a misconfigured load balancer routes traffic directly to an internal service, that service must still enforce auth. Defense in depth.

**Production complement:** K8s `NetworkPolicy` restricts which pods can reach which services at the network layer. Istio mTLS adds mutual certificate authentication between sidecars. JWT validation remains the primary application-layer control; mTLS is the network-layer second line of defense.

### Virtual Threads & Async Execution

All Spring Boot MVC apps enable virtual threads:

```yaml
spring:
  threads:
    virtual:
      enabled: true
```

`@Async` executors and `@Scheduled` task schedulers are configured to use virtual thread pools. No application code creates `new Thread(...)` directly — all task submission goes through Spring's executor abstractions so context propagation wiring applies uniformly.

For `apps/bff` (WebFlux): WebFlux already uses a non-blocking event loop; virtual threads do not apply to the reactive pipeline. Context propagation uses Reactor's `Context` API and `ContextPropagation.resetAll()`.

### Context Propagation

Every thread — web request, `@Async` task, or `@Scheduled` job — carries its full execution context:

| Context | Propagation mechanism |
|---|---|
| Spring Security principal | `DelegatingSecurityContextExecutor` wrapping the virtual thread executor |
| MDC (correlation ID, trace ID) | Logback `MDC`; copied via `ContextSnapshotTaskDecorator` on `@Async` executors |
| Micrometer Tracing (spans) | `ObservationThreadLocalAccessor` registered in `ContextRegistry`; propagates automatically via Micrometer's `ContextSnapshot` integration in Spring Boot 3.2+ |

A shared `ContextSnapshotTaskDecorator` bean in `libs/common` wraps all three. `@Async` executor beans in each app apply this decorator. Spring Boot's auto-configuration for Micrometer handles observation propagation; Security and MDC require the decorator.

### Jackson Configuration

All Spring Boot apps configure Jackson identically via `application.yml`:

```yaml
spring:
  jackson:
    serialization:
      write-dates-as-timestamps: false
    deserialization:
      fail-on-unknown-properties: false
    default-property-inclusion: non_null
```

`write-dates-as-timestamps: false` ensures all `Instant` / `LocalDate` fields serialize as ISO-8601 strings. `fail-on-unknown-properties: false` prevents deserialization failures when services receive responses with fields added in newer versions. `non_null` suppresses null fields in responses.

---

## Gradle Subproject Declarations

`settings.gradle` declares all subprojects under their directory prefix:

```groovy
rootProject.name = 'event-viewer'

include 'apps:gateway'
include 'apps:bff'
include 'apps:event-ingest'
include 'apps:event-read'
include 'apps:management'

include 'libs:event-api'
include 'libs:opensearch-lib'
include 'libs:s3-lib'
include 'libs:common'
```

App dependency example (`apps/event-read/build.gradle`):

```groovy
dependencies {
    implementation project(':libs:event-api')
    implementation project(':libs:opensearch-lib')
    implementation project(':libs:s3-lib')
    implementation project(':libs:common')
}
```
