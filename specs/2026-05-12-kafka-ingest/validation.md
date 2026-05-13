# Validation — Phase 2: Kafka Ingest

Phase 2 is complete and mergeable when every check below passes without manual intervention, except where a manual step is explicitly noted.

---

## 1. Gradle Build (Unit Tests — no Docker required)

```bash
./gradlew :apps:event-ingest:test
```

- All unit tests in `src/test/` pass with no running containers
- `EventIngestController` tested with a mocked `KafkaTemplate` via `@WebMvcTest`:
  - Valid request → `202` response with `event_id` and `ingest_ts`
  - Missing `event_id` → `400` with error body
  - Missing `schema_type` → `400` with error body
  - `KafkaTemplate` throws → `503`
- `KafkaTopicProperties` binding tested with `@ConfigurationPropertiesTest` — verifies both main topic and DLT fields deserialize correctly from YAML

---

## 2. Integration Tests via Docker Compose (itest source set)

```bash
./gradlew :apps:event-ingest:itest
```

The `gradle-docker-compose-plugin` manages the full lifecycle automatically around the `itest` task only:

1. `docker compose up -d` — brings up `kafka`, `postgres`, `opensearch`, `opensearch-dashboards`, `localstack`
2. Waits for `kafka` to reach healthy status
3. Spring Boot test context starts → `KafkaAdmin` creates `event-raw` and `event-raw-dlt` topics
4. `itest` tests run against the live broker on `localhost:9092`
5. `docker compose down` — tears down all containers regardless of test outcome

**Integration tests that must pass (`src/itest/`):**

| Test class | Test case | Assertion |
|---|---|---|
| `EventIngestIT` | Happy path | `POST /event/v1/events` with valid envelope + valid JWT → `202 Accepted` → message on `event-raw` with key `event_id`, value containing `schema_type` and `ingest_ts` (polled within 5s) |
| `EventIngestIT` | Timestamp default | POST without `timestamp` + valid JWT → `ingestTs` in response is non-null and within 1 second of test execution |
| `EventIngestIT` | Unauthenticated | POST without `Authorization` header → `401` |
| `EventIngestValidationIT` | Missing `event_id` | POST with valid JWT, missing `event_id` → `400` |
| `EventIngestValidationIT` | Missing `schema_type` | POST with valid JWT, missing `schema_type` → `400` |
| `EventIngestValidationIT` | Non-UUID `event_id` | POST with valid JWT, `"event_id": "not-a-uuid"` → `400` |
| `EventIngestValidationIT` | Empty body | POST with valid JWT, `{}` → `400` |
| `KafkaTopicProvisioningIT` | Topics created | After context start, `AdminClient.listTopics()` includes `event-raw` (3 partitions) and `event-raw-dlt` (1 partition) |

Total `itest` runtime must not exceed 3 minutes including Docker Compose startup.

---

## 3. Manual Smoke Test

Perform this checklist once before opening the PR. Start from a clean state.

```bash
docker compose up -d
```

**a. Topics exist**

```bash
docker compose exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
# Must include: event-raw
# Must include: event-raw-dlt
```

**b. Start services**

```bash
./gradlew :apps:gateway:bootRun &
./gradlew :apps:event-ingest:bootRun &
```

Wait for both to log `Started ... in ... seconds`.

**c. Health checks**

```bash
curl -s http://localhost:8080/actuator/health | jq .status
# Expected: "UP"

curl -s http://localhost:8081/actuator/health | jq .status
# Expected: "UP"
```

**d. Generate a dev JWT**

```bash
# Requires local-private.pem from docker-compose.env
./scripts/generate-dev-jwt.sh
# Outputs a signed JWT — copy it as DEV_JWT for the next steps
```

**e. Post a valid event via the gateway**

```bash
curl -s -X POST http://localhost:8080/event/v1/events \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $DEV_JWT" \
  -d '{
    "event_id": "a3f1bc82-4e7d-4b2a-9c1d-123456789abc",
    "schema_type": "order-created",
    "payload": { "order_id": "ORD-1", "amount": 49.99 }
  }' | jq .
# Expected: { "event_id": "a3f1bc82-...", "ingest_ts": "..." }
# HTTP status: 202
```

**f. Confirm unauthenticated request is rejected**

```bash
curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8080/event/v1/events \
  -H "Content-Type: application/json" \
  -d '{ "event_id": "a3f1bc82-4e7d-4b2a-9c1d-123456789abc", "schema_type": "order-created" }'
# Expected: 401
```

**g. Confirm message landed on the topic**

```bash
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic event-raw \
  --from-beginning \
  --max-messages 1
# Expected: JSON with event_id, schema_type, ingest_ts, payload
```

**h. Confirm DLT topic is empty (nothing errored)**

```bash
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic event-raw-dlt \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 3000
# Expected: no messages (timeout is normal here)
```

**i. Validation rejection**

```bash
curl -s -w "\nHTTP %{http_code}" -X POST http://localhost:8080/event/v1/events \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $DEV_JWT" \
  -d '{ "payload": { "foo": "bar" } }'
# Expected: HTTP 400
```

**j. Teardown**

```bash
docker compose down
```

---

## 4. CI Pipeline

A pull request against `main` must show all GitHub Actions jobs green:

| Job | Pass Condition |
|---|---|
| `backend-unit` | `./gradlew test` exits 0 across all subprojects; no Docker required |
| `backend-itest` | `./gradlew :apps:event-ingest:itest` exits 0; Docker Compose starts in the Actions runner, all `itest` tests pass, Docker Compose tears down |
| `frontend` | `npm ci && npm run build` exits 0 (unchanged from Phase 1) |

`backend-itest` must run on `ubuntu-latest` (Docker available by default on GitHub-hosted runners). It must depend on `backend-unit` passing first.

---

## 5. Build Convention Verification

Confirm the `itest` standard is correctly applied to all `apps/*` subprojects:

```bash
./gradlew projects
# All five apps listed

for app in gateway bff event-ingest event-read management; do
  ls apps/$app/src/itest/java
  # Must exist (at minimum a .gitkeep or test class)
done
```

`./gradlew :apps:gateway:test` must pass with the Spring Security permit-all config in place (existing `GatewayApplicationTests` context-load test must still succeed).

---

## Definition of Done

All five checks above pass on a clean clone of the branch. `event-raw` and `event-raw-dlt` are provisioned by Spring Kafka on startup with the correct partition counts. `POST /event/v1/events` through the gateway returns `202` and the message is verifiably on the topic. The `itest` source set is present and empty in all `apps/*` subprojects not yet using it. The PR description links to this validation document.
