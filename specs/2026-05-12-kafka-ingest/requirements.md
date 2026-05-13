# Requirements — Phase 2: Kafka Ingest

## Scope

Stand up the write entry point: a REST endpoint in `apps/event-ingest` that accepts events, validates the required envelope fields, and publishes each event as a JSON message to the `event-raw` Kafka topic. The gateway routes `/event/v1/**` to `event-ingest`.

This phase also establishes five platform-wide standards — `itest` source set, internal service security, Jackson configuration, virtual threads + context propagation, and gateway fault tolerance — that every subsequent phase inherits without revisiting.

No downstream persistence (S3, OpenSearch, PostgreSQL) is implemented here — that is Phase 3.

---

## Decisions

### `itest` Source Set Standard

All `apps/*` subprojects share a standardized `itest` source set declared in the root `build.gradle`.

| Source set | What goes here |
|---|---|
| `src/test/` | Unit tests — no Spring context, no running containers, no live services |
| `src/itest/` | Integration tests — anything requiring a live Spring Boot context, Docker Compose containers, or calls between microservices |

`itestImplementation` extends `testImplementation`; `itest` classpath includes `sourceSets.test.compileClasspath`. The `itest` Gradle task runs after `test`, is wired into `check`, and is the only task that triggers Docker Compose lifecycle. `test` always runs without external dependencies.

### Internal Service Security Model

Every internal service independently validates the JWT. The gateway is a routing and first-line validation layer, not the sole auth boundary.

**Why:** Without per-service auth, any pod that can reach an internal service port — whether due to misconfiguration, a compromised pod, or a missing network policy — has full access. Defense in depth requires each service to enforce auth itself.

**JWT flow:**
1. Client sends `Authorization: Bearer <jwt>` to the gateway
2. Gateway validates signature + expiry; rejects with `401` if invalid
3. Gateway forwards the `Authorization` header unchanged to the downstream service
4. Downstream service independently re-validates the same JWT

**Implementation:** All internal apps (`event-ingest`, `event-read`, `management`, `bff`) configure `spring-boot-starter-oauth2-resource-server` with:

```yaml
spring:
  security:
    oauth2:
      resourceserver:
        jwt:
          public-key-location: classpath:keys/local-public.pem
```

The `platform-public.pem` RSA-2048 public key is committed to `src/main/resources/keys/` in each app. The corresponding private key lives only in `docker-compose.env` (gitignored) for local JWT generation. Phase 11 replaces this with a real OIDC provider and JWKS endpoint.

**Why RSA over HMAC for dev:** With HMAC, any service holding the secret can forge tokens. With RSA, only the private key can sign; all services verify with the public key. This matches the production model (OIDC provider signs; services verify) from day one.

**Production complement:** K8s `NetworkPolicy` restricts pod-to-pod reachability. Istio mTLS adds mutual certificate authentication at the network layer. JWT validation remains the primary application-layer control.

**Dev JWT generation:** A dev utility script (`scripts/generate-dev-jwt.sh`) signs a test JWT using the dev private key for use in manual testing and `itest` setup. It is not committed to production deployments.

**Gateway security:** `apps/gateway` also configures `spring-boot-starter-oauth2-resource-server` to validate the incoming JWT before forwarding. This is separate from the downstream services' validation — the gateway rejects invalid tokens before any downstream call is made. Actuator endpoints are excluded from JWT requirements.

### Gateway Fault Tolerance (Resilience4J)

`apps/gateway` adds Resilience4J circuit breakers and rate limiters via `spring-cloud-starter-circuitbreaker-resilience4j`.

**Circuit breakers:** One `CircuitBreaker` instance per downstream service route (`event-ingest`, `event-read`, `management`, `bff`). Opens after a configurable failure-rate threshold; returns `503` while open; half-opens to probe recovery.

**Rate limiting:** Adaptive `RateLimiter` per client, keyed on JWT subject (for authenticated requests) or API key identifier. Rejects excess requests with `429`. Limits are configurable per route in `application.yml`.

Both are configured as Spring Cloud Gateway filters in `application.yml`:

```yaml
spring.cloud.gateway.routes:
  - id: event-ingest
    uri: http://event-ingest:8081
    predicates:
      - Path=/event/v1/**
    filters:
      - name: CircuitBreaker
        args:
          name: event-ingest
          fallbackUri: forward:/fallback/event-ingest
      - name: RequestRateLimiter
        args:
          rate-limiter: "#{@clientRateLimiter}"
          key-resolver: "#{@jwtSubjectKeyResolver}"
```

### Virtual Threads & Context Propagation

All Spring Boot MVC apps (`event-ingest`, `event-read`, `management`) enable virtual threads:

```yaml
spring:
  threads:
    virtual:
      enabled: true
```

`@Async` executors and `@Scheduled` task schedulers use virtual thread pools. A `ContextSnapshotTaskDecorator` bean in `libs/common` wraps all three context layers:

| Context | Mechanism |
|---|---|
| Spring Security principal | `DelegatingSecurityContextExecutor` |
| MDC (correlation ID, trace ID) | Logback MDC copied at task submission via `ContextSnapshotTaskDecorator` |
| Micrometer spans | `ObservationThreadLocalAccessor` registered in `ContextRegistry`; auto-propagated by Spring Boot 3.2+ |

For `apps/bff` (WebFlux): virtual threads do not apply to the reactive pipeline. Context propagation uses Reactor's `Context` API with `ContextPropagation.resetAll()` — no `ThreadLocal` strategies in reactive chains.

### Jackson Configuration

All Spring Boot apps set identical Jackson properties in `application.yml`:

```yaml
spring:
  jackson:
    serialization:
      write-dates-as-timestamps: false
    deserialization:
      fail-on-unknown-properties: false
    default-property-inclusion: non_null
```

These are set per-app in Phase 2 for `event-ingest` and `gateway`. Every subsequent phase adds the same block to each new app's `application.yml`. If duplication becomes excessive (beyond four apps), a `libs/common` Spring Boot auto-configuration class will centralize it.

### Ingest Payload Contract

The endpoint accepts a JSON envelope with the following fields:

| Field | Type | Required | Notes |
|---|---|---|---|
| `event_id` | UUID (string) | Yes | Client-generated; used as the Kafka message key |
| `schema_type` | string | Yes | Identifies the event type; no registry validation yet (Phase 4) |
| `timestamp` | ISO-8601 string | No | Event occurrence time; server fills with ingest instant if absent |
| `payload` | JSON object | No | Arbitrary free-form event data; passed through without inspection |

### Kafka Message Format

```json
{
  "event_id": "a3f1bc82-4e7d-4b2a-9c1d-123456789abc",
  "schema_type": "order-created",
  "timestamp": "2026-05-12T14:30:00Z",
  "ingest_ts": "2026-05-12T14:30:00.123Z",
  "payload": { "order_id": "ORD-999", "amount": 49.99 }
}
```

Key: `event_id` (string). Value: JSON string. Both use `StringSerializer`. Avro serialization is introduced in Phase 4.

### Kafka Topic Provisioning

Topics are declared in `application.yml` and created at startup by Spring Kafka's `KafkaAdmin` via `@ConfigurationProperties`. No `kafka-init` Docker Compose service.

```yaml
event-ingest:
  kafka:
    topics:
      - name: event-raw
        partitions: 3
        replication-factor: 1
        dead-letter:
          name: event-raw-dlt
          partitions: 1
          replication-factor: 1
```

Every topic in config produces two `NewTopic` beans (main + DLT). `spring.kafka.admin.fail-fast: false` allows retry if the broker is slow to start.

### HTTP Contract

| Scenario | Status | Body |
|---|---|---|
| Valid envelope published to Kafka | `202 Accepted` | `{ "event_id": "...", "ingest_ts": "..." }` |
| Missing required field | `400 Bad Request` | `{ "errors": [...] }` |
| Malformed JSON | `400 Bad Request` | Standard Spring error body |
| Kafka unavailable | `503 Service Unavailable` | `{ "error": "ingest unavailable" }` |
| Missing / invalid JWT | `401 Unauthorized` | Spring Security default |

### What Is Explicitly Out of Scope

- Kafka consumer (Phase 3)
- S3, OpenSearch, PostgreSQL writes (Phase 3)
- Avro + Schema Registry (Phase 4)
- Schema-level field validation (Phase 4)
- Real OIDC provider / JWKS endpoint (Phase 11)
- Full RBAC enforcement (Phase 11)
- API key issuance and management (Phase 11)

---

## Context

This phase establishes the write path entry point and the platform standards every subsequent phase inherits. The JWT forwarding + per-service Resource Server model means Phase 11 swaps in the real OIDC config in one place (`public-key-location` → `jwk-set-uri`) without restructuring any service's security configuration.
