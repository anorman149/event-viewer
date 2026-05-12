# Validation — Phase 1: Project Skeleton

Phase 1 is complete and mergeable when every check below passes without manual intervention.

---

## 1. Gradle Build

```bash
./gradlew build
```

- All nine subprojects listed in `./gradlew projects`: `apps:gateway`, `apps:bff`, `apps:event-ingest`, `apps:event-read`, `apps:management`, `libs:event-api`, `libs:opensearch-lib`, `libs:s3-lib`, `libs:common`
- Compiles without errors across all subprojects
- `apps:gateway` context-load test (`@SpringBootTest`) passes
- No subproject is missing from `./gradlew projects` output

---

## 2. Frontend Build

```bash
cd frontend && npm ci && npm run build
```

- Produces a `frontend/dist/` directory with no TypeScript or Vite errors
- `npm run dev` starts the Vite dev server on `http://localhost:5173` and serves the placeholder home page

---

## 3. Docker Compose — All Services Healthy

```bash
docker compose up -d
docker compose ps
```

All five containers must reach `healthy` status within 90 seconds of `up`:

| Container | Health Check |
|---|---|
| `kafka` | `kafka-broker-api-versions.sh --bootstrap-server localhost:9092` exits 0 |
| `postgres` | `pg_isready -U eventviewer` returns 0 |
| `opensearch` | `GET http://localhost:9200/_cluster/health` → `status` is `green` or `yellow` |
| `opensearch-dashboards` | `GET http://localhost:5601/api/status` → HTTP 200 |
| `localstack` | `GET http://localhost:4566/_localstack/health` → `s3` service shows `running` |

Additionally, the LocalStack init script must have run: `awslocal --endpoint-url=http://localhost:4566 s3 ls` must list the `event-viewer-local` bucket.

---

## 4. Backend Health Endpoint

With Docker Compose running and the Spring Boot app started locally:

```bash
curl -s http://localhost:8080/actuator/health | jq .status
# Expected: "UP"
```

Response must be HTTP 200 with body `{"status":"UP"}`. No other actuator endpoints need to be exposed at this stage.

---

## 5. CI Pipeline

A pull request against `main` must show all three GitHub Actions jobs green:

| Job | Pass Condition |
|---|---|
| `backend` | `./gradlew build` exits 0 |
| `frontend` | `npm ci && npm run build` exits 0 |
| `smoke-test` | Docker Compose starts healthy → `./gradlew :apps:gateway:bootRun` starts → `GET http://localhost:8080/actuator/health` returns HTTP 200 → both torn down cleanly |

No job may be skipped. The smoke-test job must depend on both `backend` and `frontend` jobs passing first.

---

## 6. Repository Structure

The final repo layout must match the agreed structure:

```
event-viewer/
  .github/
    workflows/
      ci.yml
  apps/
    gateway/
      build.gradle
      src/main/java/org/eventviewer/gateway/GatewayApplication.java
      src/main/resources/application.yml
      src/test/java/org/eventviewer/gateway/GatewayApplicationTests.java
    bff/
      build.gradle
      src/                    ← stub only in this phase
    event-ingest/
      build.gradle
      src/                    ← stub only in this phase
    event-read/
      build.gradle
      src/                    ← stub only in this phase
    management/
      build.gradle
      src/                    ← stub only in this phase
  libs/
    event-api/
      build.gradle
      src/
    opensearch-lib/
      build.gradle
      src/
    s3-lib/
      build.gradle
      src/
    common/
      build.gradle
      src/
  frontend/
    .nvmrc
    package.json
    vite.config.ts
    src/
    dist/                     ← after build (gitignored)
  specs/
  docker-compose.yml
  docker-compose.env.example
  build.gradle
  settings.gradle
  gradlew
  gradlew.bat
```

`docker-compose.env` is gitignored. `frontend/dist/` and `frontend/node_modules/` are gitignored.

---

## Definition of Done

All six checks above pass on a clean clone of the branch. No hardcoded secrets in committed files. The PR description links to this validation document.
