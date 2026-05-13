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

---

## Build

### Docker Compose lifecycle is managed at the root Gradle level

The `com.avast.gradle.docker-compose` plugin is applied once at the root `build.gradle`. All `itest` tasks in `apps/*` subprojects automatically get Docker Compose started before they run and torn down after. Never apply the plugin again in a subproject `build.gradle`.
