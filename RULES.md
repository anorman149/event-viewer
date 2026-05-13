# Project Rules

Standards that apply to every file in this repository. Follow these without exception unless explicitly overridden in a spec.

---

## Testing

### Use Java objects for test data — never raw JSON strings

Build request and response objects using their Java types, then serialize/deserialize through the application's `ObjectMapper`. Do not construct JSON strings by hand.

**Why:** Hard-coded JSON strings break silently when field names or types change. Java objects break at compile time, and the `@JsonProperty` mapping is exercised the same way as the real code path.

**Right:**
```java
IngestRequest request = new IngestRequest(UUID.randomUUID(), "order-created", null, Map.of("key", "value"));
String body = objectMapper.writeValueAsString(request);
IngestResponse response = restTemplate.exchange(..., IngestResponse.class).getBody();
assertThat(response.eventId()).isEqualTo(request.eventId());
```

**Wrong:**
```java
String body = "{\"event_id\":\"" + UUID.randomUUID() + "\",\"schema_type\":\"order-created\"}";
```

### Controller tests belong in `src/itest/` — not in `src/test/`

Controller behavior must be tested against a real Spring Boot context with live infrastructure (Kafka, databases). Do not mock services or Kafka just to test an HTTP layer.

**Why:** Mocked controller tests give false confidence — they validate that mocks return what you told them to return, not that the system actually behaves correctly end-to-end.

- `src/test/` — pure unit tests: service logic, Kafka/persistence layer, property binding. No Spring context, no containers.
- `src/itest/` — integration tests: anything involving a controller, Kafka, a database, or any external system. Runs with Docker Compose managed by the root Gradle build.

### Every `apps/*` itest suite must have a single `BaseTest` abstract class

Every Spring Boot application's `src/itest/` package must contain one abstract `BaseTest` class that:

- Carries the `@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)` annotation
- Declares `@LocalServerPort int port`, `@Autowired TestRestTemplate restTemplate`, and `@Autowired ObjectMapper objectMapper`
- Provides shared helpers: `authHeaders()`, `baseUrl()`

All `*IT` test classes extend `BaseTest` and carry **no** `@SpringBootTest` annotation of their own.

```java
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class BaseTest {
    @LocalServerPort int port;
    @Autowired TestRestTemplate restTemplate;
    @Autowired ObjectMapper objectMapper;

    protected HttpHeaders authHeaders() { ... }
    protected String baseUrl() { return "http://localhost:" + port; }
}

class EventIngestIT extends BaseTest { ... }
class EventIngestValidationIT extends BaseTest { ... }
```

**Why:** Spring's context caching keys off the `@SpringBootTest` configuration. When all IT classes share the same `BaseTest`, the application context is started once and reused across the entire itest run — no redundant container or application startup per class.

### Integration tests use the application's `ObjectMapper`

In `@SpringBootTest` tests, inject `@Autowired ObjectMapper objectMapper` to get the application-configured instance (Jackson modules, naming strategies, inclusion rules all match production).

---

## Architecture

### Three-layer structure for every Spring Boot application

| Layer | Package | Responsibility |
|---|---|---|
| REST | `controller/` | Validate request, call service, return `ResponseEntity`. No business logic. No infrastructure calls. |
| Service | `service/` | Orchestrate business logic. Calls infrastructure layers. |
| Infrastructure | `kafka/`, `persistence/`, `search/` | Kafka publishing, database access, external API calls. No HTTP concerns. |

### Controllers always return `ResponseEntity<T>`

Every controller method must return `ResponseEntity<T>` with the response object wrapped inside. Do not use `@ResponseStatus` on methods — express the HTTP status via `ResponseEntity.status(...).body(...)`. This makes the status code explicit and visible at the call site.

**Right:**
```java
public ResponseEntity<IngestResponse> ingest(@Validated @RequestBody IngestRequest request) {
    return ResponseEntity.status(HttpStatus.ACCEPTED).body(eventIngestService.ingest(request));
}
```

**Wrong:**
```java
@ResponseStatus(HttpStatus.ACCEPTED)
public IngestResponse ingest(@Validated @RequestBody IngestRequest request) {
    return eventIngestService.ingest(request);
}
```

### GlobalExceptionHandler owns all exception handling

Every `apps/*` Spring Boot application must have a `GlobalExceptionHandler` annotated with `@RestControllerAdvice`. All `@ExceptionHandler` methods live there — never in controllers. Exception handlers return `ResponseEntity<ErrorResponse>` (not bare objects with `@ResponseStatus`). Use the shared `ErrorResponse` record from `libs/event-api` as the error contract.

### Security is always explicit — never rely on auto-configuration alone

Every `apps/*` Spring Boot application must have an explicit `SecurityConfig` class with a `SecurityFilterChain` bean that declares which paths are public and which require authentication.

---

## Observability

### Every service method must have `@Timed(histogram = true)`

Annotate every public method in `@Service` classes with Micrometer's `@Timed`. Always enable histograms for percentile-aware alerting.

```java
@Timed(value = "event.ingest.service.ingest",
       description = "Time to process an event through the ingest service layer",
       histogram = true)
public IngestResponse ingest(IngestRequest request) { ... }
```

Metric name convention: `{domain}.{app}.service.{method}` — all lowercase, dot-separated.

**Why `TimedAspect` is required:** `@Timed` on non-controller beans requires the `TimedAspect` AOP aspect to be active. `CommonAutoConfiguration` in `libs/common` registers this automatically when `MeterRegistry` is present.

### Every controller method must have `@Timed(histogram = true)`

Annotate every public method in `@RestController` classes with `@Timed`. Spring MVC already records `http.server.requests` at the framework level, but controller-level `@Timed` captures per-operation timing with a consistent naming scheme independent of the URL structure.

```java
@Timed(value = "event.ingest.controller.ingest",
       description = "Time to handle POST /event/v1/events",
       histogram = true)
public ResponseEntity<IngestResponse> ingest(...) { ... }
```

Metric name convention: `{domain}.{app}.controller.{operation}` — all lowercase, dot-separated.

### Infrastructure methods that do I/O must have `@Timed(histogram = true)`

Kafka publishers, database repositories, and search clients must also carry `@Timed` on their primary operation methods. Add a `DistributionSummary` for any meaningful payload size metric (e.g., serialized message bytes).

```java
@Timed(value = "kafka.event.publish", description = "...", histogram = true)
public void publish(...) {
    String json = objectMapper.writeValueAsString(message);
    messageSizeSummary.record(json.getBytes(StandardCharsets.UTF_8).length);
    kafkaTemplate.send(TOPIC, key, json);
}
```

`DistributionSummary` naming convention: `{system}.{resource}.{unit}` — e.g., `kafka.event.message.bytes`.

### Tracing propagation is enabled on all Spring Boot applications

Every `apps/*` `application.yml` must include the following management tracing block. W3C `traceparent` is produced for downstream compatibility (Jaeger, OpenTelemetry Collector, etc.); both W3C and B3 are consumed for cross-team interoperability.

```yaml
management:
  tracing:
    enabled: true
    sampling:
      probability: 1.0         # Lower in production; 1.0 safe for dev
    propagation:
      consume:
        - w3c
        - b3
      produce:
        - w3c
```

### Kafka tracing — trace context is automatically propagated as Kafka headers

When `spring.kafka.template.observation-enabled: true` is set, `KafkaTemplate.send()` automatically injects the active W3C `traceparent` header into every Kafka record when called within an active Micrometer observation. This connects the HTTP ingest trace to the downstream consumer trace without any manual header manipulation.

```yaml
spring:
  kafka:
    template:
      observation-enabled: true
    listener:
      observation-enabled: true   # add when consumers are implemented
```

### Validation error messages must identify the field and what was expected

`GlobalExceptionHandler.handleValidation()` must resolve the JSON property name (via `@JsonProperty` reflection on the target class) and include the rejected value when present. Error messages must clearly identify which field failed and why.

**Format:** `"'<json-property-name>': <constraint-message>"` — e.g., `"'event_id': must not be null"` or `"'event_id': must be a valid UUID (received: 'not-a-uuid')"`.

`handleUnreadableMessage()` handles JSON parse failures (missing body, wrong types) with a similarly descriptive message that includes the field name and invalid value.

### UUID fields use the `UUID` type with `@NotNull`; validation errors are handled in `handleUnreadableMessage`

Declare incoming UUID fields as `UUID` with `@NotNull`. When a caller sends an invalid UUID string, Jackson throws `HttpMessageNotReadableException` wrapping an `InvalidFormatException` — this bypasses Bean Validation. `GlobalExceptionHandler.handleUnreadableMessage()` detects this case via `InvalidFormatException.getTargetType() == UUID.class`, extracts the field name from `getPath()`, resolves the `@JsonProperty` name via reflection, and returns:

```
'event_id': must be a valid UUID (e.g. 550e8400-e29b-41d4-a716-446655440000) (received: 'not-a-uuid')
```

The `@ValidUUID` constraint and `UuidValidator` in `libs/event-api` remain available for validating UUID strings in path variables or query parameters where the field type must be `String`.

---

## Logging

### Every `application.yml` must declare the log pattern and default log level

All `apps/*` `application.yml` files must include:
```yaml
logging:
  level:
    root: error
  pattern:
    console: "%d{yyyy-MM-dd'T'HH:mm:ss.SSSXXX} %-5level %magenta([${spring.application.name},%X{traceId:-},%X{spanId:-}]) %cyan(%logger{36}) - %msg%n"
```

- **Default level `error`** — production default; only errors are logged. Lower levels are enabled via the local profile.
- **`%X{traceId:-}` / `%X{spanId:-}`** — Micrometer Tracing (Brave bridge) writes these MDC keys on every request. They appear as `-` when no active trace exists.
- **`${spring.application.name}`** — makes each app's logs immediately identifiable in aggregated log streams.

### Local development uses `application-local.yml`

Every `apps/*` app must have an `application-local.yml` alongside `application.yml`:
```yaml
logging:
  level:
    root: info
    org.eventviewer: debug
```

Activate with `--spring.profiles.active=local` in your IDE run configuration or with `SPRING_PROFILES_ACTIVE=local` environment variable. Never commit this profile active in `application.yml`.

---

## Build

### Docker Compose lifecycle is managed at the root Gradle level

The `com.avast.gradle.docker-compose` plugin is applied once at the root `build.gradle`. All `itest` tasks in `apps/*` subprojects automatically get Docker Compose started before they run and torn down after. Never apply the plugin again in a subproject `build.gradle`.
