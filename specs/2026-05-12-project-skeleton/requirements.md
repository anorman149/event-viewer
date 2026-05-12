# Requirements — Phase 1: Project Skeleton

## Scope

Stand up the structural shell of the project: Gradle multi-module backend, standalone Vite frontend, local dev infrastructure via Docker Compose, and a CI pipeline. No business logic, no event data, no auth.

---

## Decisions

### Module Structure

The monorepo uses an `apps/` + `libs/` directory split. All subprojects are declared in `settings.gradle` now so the module graph is locked in, even if most contain only a stub `build.gradle` and empty `src/` until their respective phases.

**Applications** (`apps/`) — each is a runnable Spring Boot app:

| Subproject | Spring Boot | Role |
|---|---|---|
| `apps:gateway` | Yes (Spring Cloud Gateway) | External entry point — routing, auth, rate limiting; executable in this phase |
| `apps:bff` | Yes (WebFlux, stub only) | Backend for Frontend — UI aggregation and response shaping (Phase 5+) |
| `apps:event-ingest` | Yes (stub only) | Kafka consumer + S3 writer + OpenSearch indexer (Phase 2) |
| `apps:event-read` | Yes (stub only) | OpenSearch query + S3 payload retrieval (Phase 4) |
| `apps:management` | Yes (MVC + JPA, stub only) | CRUD for schemas, dashboards, alert rules, users/RBAC (Phase 3+) |

**Libraries** (`libs/`) — no `main` class; imported as Gradle dependencies:

| Subproject | Role |
|---|---|
| `libs:event-api` | Shared domain models, DTOs, OpenAPI spec |
| `libs:opensearch-lib` | OpenSearch client abstraction (Phase 2+) |
| `libs:s3-lib` | AWS S3 / LocalStack abstraction, ZSTD writer/reader (Phase 2+) |
| `libs:common` | Logback JSON config, correlation ID filter, shared exceptions |

**Frontend** — `frontend/` at repo root, **not** a Gradle subproject (Vite/Node).

### Java Package Root

`org.eventviewer.<module>` — e.g., `org.eventviewer.api`, `org.eventviewer.ingest`. Matches the existing `group` in `build.gradle`.

### Toolchain Versions

| Tool | Version |
|---|---|
| Java | 25 (LTS) |
| Spring Boot | 3.4.x (latest patch) |
| Gradle | 8.x (Groovy DSL — existing `build.gradle` and `gradlew` kept) |
| Node | LTS (pinned in `frontend/.nvmrc`) |
| Vite | Latest stable at scaffold time |

### Docker Compose

| Service | Image | Local Port | Notes |
|---|---|---|---|
| `kafka` | `apache/kafka:3.8` | 9092 | KRaft mode — no Zookeeper; single broker for local dev |
| `postgres` | `postgres:16` | 5432 | Database: `eventviewer`, user: `eventviewer` |
| `opensearch` | `opensearchproject/opensearch:2` | 9200 | Single node, security disabled for local dev |
| `opensearch-dashboards` | `opensearchproject/opensearch-dashboards:2` | 5601 | Points to `opensearch:9200`; browser UI for index inspection |
| `localstack` | `localstack/localstack:3` | 4566 | AWS S3 emulation; `SERVICES=s3`; bucket `event-viewer-local` auto-created via LocalStack init script |

**Kafka KRaft:** The `apache/kafka:3.8` image runs in KRaft mode by default (combined broker + controller role). No Zookeeper container is needed. The `CLUSTER_ID` env var must be a valid base64-encoded UUID, generated once and committed to `docker-compose.env.example`.

All services use named Docker volumes so data survives `docker compose down`. Secrets (passwords, keys) live in `docker-compose.env` (gitignored); `docker-compose.env.example` is committed with safe placeholder values.

### Frontend Port

Vite dev server runs on `http://localhost:5173` (Vite default). The Spring Boot API runs on `http://localhost:8080`. No proxy config needed in this phase (no API calls from the frontend yet).

### What Is Explicitly Out of Scope

- No Kafka topic creation (Phase 2)
- No OpenSearch index mapping (Phase 2)
- No database schema / migrations (Phase 3)
- No authentication or security config (Phase 9)
- No business-logic endpoints beyond `/actuator/health`
- No coverage thresholds or static analysis gates (deferred)

---

## Context

This phase exists to eliminate "it works on my machine" drift early. By the end of Phase 1, any contributor can clone the repo, run `docker compose up -d`, run `./gradlew build`, and have a green, end-to-end verified local environment in under 10 minutes.

The CI smoke test (compile + tests + `GET /actuator/health` against a live Docker Compose stack) establishes the integration feedback loop that every subsequent phase will rely on.
