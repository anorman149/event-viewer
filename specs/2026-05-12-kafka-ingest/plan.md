# Plan — Phase 2: Kafka Ingest

Each task group is a discrete, reviewable unit of work. Complete them in order; each group is a dependency for the next.

---

## Group 1 — Build Infrastructure & Platform Standards

Establish the `itest` source set, Jackson config, virtual threads, and context propagation as standards across all `apps/*` subprojects. These are set once here and inherited by every subsequent phase.

1. In the root `build.gradle`, add a `configure` block targeting all `:apps:*` subprojects. Declare the `itest` source set, wire classpath from `test`, and register the `itest` Gradle task:
   ```groovy
   configure(subprojects.findAll { it.path.startsWith(':apps:') }) {
       sourceSets {
           itest {
               java.srcDir 'src/itest/java'
               resources.srcDir 'src/itest/resources'
               compileClasspath += sourceSets.test.compileClasspath
               runtimeClasspath += sourceSets.test.runtimeClasspath
           }
       }
       configurations {
           itestImplementation.extendsFrom testImplementation
           itestRuntimeOnly.extendsFrom testRuntimeOnly
       }
       tasks.register('itest', Test) {
           description = 'Runs integration tests against live Docker Compose services.'
           group = 'verification'
           testClassesDirs = sourceSets.itest.output.classesDirs
           classpath = sourceSets.itest.runtimeClasspath
           shouldRunAfter test
       }
       check.dependsOn itest
   }
   ```
2. Create `src/itest/java/` and `src/itest/resources/` directories with a `.gitkeep` in each `apps/*` subproject that has no live tests yet: `gateway`, `bff`, `event-read`, `management`
3. Add a `ContextSnapshotTaskDecorator.java` to `libs/common` in `org.eventviewer.common.context` — wraps `Runnable` with `ContextSnapshot.captureAll()` so any `@Async` executor using this decorator automatically propagates the Spring Security context, MDC, and Micrometer `ObservationRegistry` into the spawned thread:
   ```java
   public class ContextSnapshotTaskDecorator implements TaskDecorator {
       @Override
       public Runnable decorate(Runnable runnable) {
           ContextSnapshot snapshot = ContextSnapshotFactory.builder().build().captureAll();
           return () -> {
               try (ContextSnapshot.Scope scope = snapshot.setThreadLocals()) {
                   runnable.run();
               }
           };
       }
   }
   ```
4. Register `ContextSnapshotTaskDecorator` as a Spring Boot auto-configuration bean in `libs/common` so every app that imports `common` gets it automatically; expose it as a `TaskDecorator` bean named `contextSnapshotTaskDecorator`

---

## Group 2 — Kafka Topic Config, event-ingest App & Security

Wire Kafka topic provisioning, the REST ingest endpoint, Jackson, virtual threads, and OAuth2 Resource Server into `apps/event-ingest`. Add Resilience4J and Resource Server to `apps/gateway`.

1. **Dev RSA key pair:** Generate a 2048-bit RSA key pair for local development:
   ```bash
   openssl genrsa -out dev-private.pem 2048
   openssl rsa -in dev-private.pem -pubout -out platform-public.pem
   ```
   - Commit `platform-public.pem` to `src/main/resources/keys/` in **every** `apps/*` subproject
   - Add `dev-private.pem` to `docker-compose.env` (gitignored); add placeholder to `docker-compose.env.example`
   - Commit a `scripts/generate-dev-jwt.sh` script that signs a test JWT using `dev-private.pem` (use `openssl` or a minimal Java CLI); document usage in a comment at the top of the script

2. **`apps/event-ingest` dependencies** — add to `build.gradle`:
   - `spring-boot-starter-web`, `spring-boot-starter-actuator`, `spring-kafka`
   - `spring-boot-starter-oauth2-resource-server`
   - `libs:event-api`, `libs:common`

3. **`apps/event-ingest/src/main/resources/application.yml`:**
   ```yaml
   server.port: 8081
   spring:
     application.name: event-ingest
     threads:
       virtual:
         enabled: true
     jackson:
       serialization:
         write-dates-as-timestamps: false
       deserialization:
         fail-on-unknown-properties: false
       default-property-inclusion: non_null
     security:
       oauth2:
         resourceserver:
           jwt:
             public-key-location: classpath:keys/platform-public.pem
     kafka:
       bootstrap-servers: localhost:9092
       producer:
         key-serializer: org.apache.kafka.common.serialization.StringSerializer
         value-serializer: org.apache.kafka.common.serialization.StringSerializer
         acks: all
         retries: 3
       admin:
         fail-fast: false
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

4. **`KafkaTopicProperties.java`** in `org.eventviewer.ingest.config` — `@ConfigurationProperties(prefix = "event-ingest.kafka")` holding `List<TopicDefinition>`; each `TopicDefinition` has `name`, `partitions`, `replicationFactor`, and a nested `DeadLetter` record

5. **`KafkaTopicConfig.java`** in `org.eventviewer.ingest.config` — reads `KafkaTopicProperties`, declares `NewTopic` beans for every main topic and its DLT via `KafkaAdmin.NewTopics`

6. **`AsyncConfig.java`** in `org.eventviewer.ingest.config` — declares a `ThreadPoolTaskExecutor` bean with `setVirtualThreads(true)` and `setTaskDecorator(contextSnapshotTaskDecorator)`; annotate the config class with `@EnableAsync`

7. **`IngestRequest.java`** and **`IngestResponse.java`** records in `libs/event-api` (`org.eventviewer.api.ingest`):
   ```java
   public record IngestRequest(
       @NotNull UUID eventId,
       @NotBlank String schemaType,
       Instant timestamp,
       Object payload
   ) {}

   public record IngestResponse(UUID eventId, Instant ingestTs) {}
   ```

8. **`EventIngestController.java`** in `org.eventviewer.ingest.controller`:
   - `POST /event/v1/events` — `@Validated` body; fill `timestamp` from `Instant.now()` if absent; serialize envelope + `ingest_ts` to JSON; send via `KafkaTemplate` with key `eventId.toString()`; return `202 Accepted` with `IngestResponse`
   - `MethodArgumentNotValidException` → `400` with `{ "errors": [...] }`
   - `KafkaException` → `503` with `{ "error": "ingest unavailable" }`

9. **`EventIngestApplication.java`** main class; verify `./gradlew :apps:event-ingest:bootRun` starts on 8081 with both topics created

10. **`apps/gateway` — Resource Server + Jackson + Resilience4J:**
    - Add to `build.gradle`: `spring-boot-starter-oauth2-resource-server`, `spring-cloud-starter-circuitbreaker-resilience4j`
    - Add to `application.yml`: same Jackson block, same `spring.security.oauth2.resourceserver.jwt.public-key-location`
    - Create `GatewaySecurityConfig.java` — `@Bean SecurityWebFilterChain` that requires JWT on all routes except `/actuator/**`
    - Add `CircuitBreaker` and `RequestRateLimiter` filters to each gateway route in `application.yml`
    - Create `JwtSubjectKeyResolver.java` implementing `KeyResolver` — extracts the JWT `sub` claim as the rate-limit key; falls back to remote IP if no JWT present

---

## Group 3 — Integration Tests in `itest` Source Set

Wire Docker Compose around the `itest` task and write all live tests for event-ingest.

1. Apply `com.avast.gradle:gradle-docker-compose-plugin:0.17.x` in the root `build.gradle`; configure it in `apps/event-ingest/build.gradle` to wrap the `itest` task:
   ```groovy
   dockerCompose {
       useComposeFiles = ['../../docker-compose.yml']
       isRequiredBy(tasks.named('itest'))
       waitForTcpPorts = true
   }
   ```

2. Add a test JWT helper in `src/itest/java/org/eventviewer/ingest/support/TestJwtFactory.java` — reads the dev private key from `docker-compose.env` (or a test resource path) and generates signed JWTs for use in `@BeforeEach` or `@BeforeAll` setup; keeps token generation in one place across all integration test classes

3. Write **`EventIngestIT.java`** in `src/itest/java/org/eventviewer/ingest`:
   - `@SpringBootTest(webEnvironment = RANDOM_PORT)`
   - All requests include `Authorization: Bearer <testJwt>` from `TestJwtFactory`
   - **Happy path:** valid envelope → `202` → poll `event-raw` within 5s → assert key = `event_id`, value contains `schema_type` and `ingest_ts`
   - **Timestamp default:** POST without `timestamp` → `ingestTs` in response is non-null and within 1 second of test execution
   - **Unauthenticated:** POST without `Authorization` header → `401`

4. Write **`EventIngestValidationIT.java`** in `src/itest/`:
   - Missing `event_id` → `400`
   - Missing `schema_type` → `400`
   - Non-UUID `event_id` → `400`
   - Empty body `{}` → `400`

5. Write **`KafkaTopicProvisioningIT.java`** in `src/itest/`: after Spring context starts, use `AdminClient.listTopics()` → assert `event-raw` exists with 3 partitions and `event-raw-dlt` exists with 1 partition

6. Verify `./gradlew :apps:event-ingest:itest` — Docker Compose comes up, topics are created by Spring Kafka, all `itest` tests pass, Docker Compose tears down; total runtime under 3 minutes
