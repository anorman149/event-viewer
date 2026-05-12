# Tech Stack

## Guiding Principles

- **Proven at scale** — every choice must credibly support 1M events/second
- **Open-source first** — avoid proprietary lock-in at the data layer
- **Java-native backend** — all server-side services stay in the JVM ecosystem
- **Spec-driven** — APIs are contract-first (OpenAPI); schema types are versioned

---

## Backend

| Layer | Technology | Rationale |
|---|---|---|
| Language | Java 25 (LTS) | Latest LTS; virtual threads (Project Loom), records, pattern matching |
| Framework | Spring Boot 3.x | Production-grade HTTP, WebSocket, security, and actuator |
| Build | Gradle (Groovy DSL) | Multi-module support, incremental builds; Groovy DSL is fully supported and preferred here over Kotlin DSL |
| API style | REST + WebSocket (STOMP) | REST for query/admin; WebSocket for live event feeds |
| API contract | OpenAPI 3.1 (Springdoc) | Contract-first; generates typed clients for the frontend |

> **Gradle DSL note:** Gradle 8.x made Kotlin DSL the new default for `gradle init`, offering better IDE autocomplete and type safety. Groovy DSL remains fully supported and is not deprecated. This project stays on Groovy DSL.

## Event Streaming

| Component | Technology | Rationale |
|---|---|---|
| Message bus | Apache Kafka | Industry standard for high-throughput, durable event streaming |
| Schema registry | Confluent Schema Registry (self-hosted) | Avro schema versioning and compatibility enforcement |
| Stream processing | Kafka Streams | Lightweight, embedded; no separate cluster needed for early phases |

## Storage Architecture

Three stores with distinct, non-overlapping responsibilities:

### PostgreSQL — Entity & Concept Management

Owns all relational, structured, low-cardinality data. No event payload or time-series data lives here.

| Data | Examples |
|---|---|
| Schema / event type definitions | Name, version, Avro schema, compatibility mode |
| Users, roles, API keys | Auth entities |
| Alert rules, dashboard layouts | Application config |
| Retention policies | TTL per schema type |
| Audit log | Who did what, when |

### OpenSearch — Event Metadata & Search Index

Owns the searchable projection of every event. Supports full boolean queries (AND / NOT / OR), time-range filters, aggregations, and acts as the **lookup table for S3**.

| Field stored per event | Purpose |
|---|---|
| `event_id`, `schema_type`, `schema_version` | Identity |
| `timestamp`, `ingest_ts` | Time-range queries |
| Indexed metadata fields (from schema) | Boolean / keyword / full-text search |
| `s3_key` | Pointer to the raw batch file in S3 |
| `batch_offset`, `batch_length` (bytes) | Byte range of this event within the decompressed batch |

When a user needs the **full raw payload**, the API resolves it as: OpenSearch lookup → S3 key + byte range → fetch + decompress → return event.

### S3 — Raw Event Storage

Owns the immutable, compressed source of truth for every event payload. Never queried directly; always accessed via the OpenSearch lookup.

**Batching:** Events are accumulated and flushed in batches of **5,000 events** per file. This amortizes S3 PUT costs and produces files that compress well.

**Compression:** **Zstandard (ZSTD)** at level 3 (fast compression, ~3–5× better ratio than gzip at comparable speed). Level 3 is the default; tunable per deployment. The `.zst` extension is used on all batch files.

> ZSTD was chosen over Snappy (lower ratio) and gzip (slower decompression) for its balance of speed and compression density at high ingest rates.

**Key path — Hive-style partitioning:**

```
s3://<bucket>/events/
  year=YYYY/
    month=MM/
      day=DD/
        hour=HH/
          schema_type=<type>/
            pod=<k8s-pod-name>/
              <uuid>.zst
```

Example:
```
s3://event-viewer-prod/events/year=2026/month=05/day=12/hour=14/schema_type=order-created/pod=ingest-7d9f4b-xkp2q/a3f1bc82-4e7d.zst
```

**Why `pod=<k8s-pod-name>` in the path:**
Each pod writes exclusively to its own key prefix. No two pods ever write to the same S3 key, eliminating concurrent write conflicts without any locking or coordination. Pod name is injected via the `MY_POD_NAME` downward API env var.

**Hive compatibility:** The `year=/month=/day=/hour=/schema_type=` prefix structure is directly queryable by Athena, Spark, and other Hive-aware tools without a custom SerDe.

**OpenSearch batch index document** (written once per S3 flush):

```json
{
  "s3_key": "events/year=2026/month=05/.../pod=ingest-7d9f4b-xkp2q/a3f1bc82.zst",
  "schema_type": "order-created",
  "schema_version": "1.2",
  "event_count": 5000,
  "first_event_ts": "2026-05-12T14:30:00.000Z",
  "last_event_ts": "2026-05-12T14:30:04.871Z",
  "compressed_size_bytes": 524288,
  "uncompressed_size_bytes": 2621440,
  "pod": "ingest-7d9f4b-xkp2q"
}
```

Per-event documents in OpenSearch include `s3_key`, `batch_offset`, and `batch_length` for direct byte-range retrieval.

---

## Frontend

| Layer | Technology | Rationale |
|---|---|---|
| Language | TypeScript | Type safety across a complex, data-heavy UI |
| Framework | React 18+ | Component model ideal for live-updating dashboards |
| Build tool | Vite | Fast HMR, lean bundle output |
| State management | Zustand | Lightweight; avoids Redux boilerplate for this scale |
| Charting | Apache ECharts | High-performance rendering for time-series at volume |
| HTTP client | Axios + OpenAPI-generated types | Typed, contract-bound API calls |
| WebSocket | STOMP.js | Pairs with Spring's STOMP broker for live feeds |
| UI components | shadcn/ui (Tailwind CSS) | Accessible, unstyled primitives; full control over design |

## Infrastructure & Operations

| Concern | Technology |
|---|---|
| Containerization | Docker + Docker Compose (local dev) |
| Orchestration | Kubernetes (production target) |
| CI/CD | GitHub Actions |
| Metrics | Micrometer → Prometheus → Grafana |
| Distributed tracing | OpenTelemetry → Jaeger |
| Structured logging | Logback (JSON) → OpenSearch |
| Secrets management | Environment variables / Kubernetes Secrets |

## Security

| Concern | Approach |
|---|---|
| Authentication | Spring Security + JWT (access + refresh tokens) |
| Authorization | Role-Based Access Control (RBAC) |
| API ingest auth | API keys (hashed, scoped per client) |
| Transport | TLS everywhere (enforced in production) |
| Audit logging | Immutable audit trail in PostgreSQL |