# Plan — Phase 1: Project Skeleton

Each task group is a discrete, reviewable unit of work. Complete them in order; each group is a dependency for the next.

---

## Group 1 — Gradle Multi-Module Structure

Set up the root build and all subproject scaffolds under the `apps/` + `libs/` split. No application source code yet — just the build graph.

1. Update `settings.gradle` to declare all nine subprojects:
   ```groovy
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
2. Create each subproject directory with a minimal `build.gradle` (Java plugin, group `org.eventviewer.<subproject-name>`)
3. Add `apps/gateway/build.gradle` with Spring Cloud Gateway dependency (`spring-cloud-starter-gateway`) — this is the only fully wired app in this phase; all others are stubs
4. Add a root-level `build.gradle` with a `subprojects {}` block applying shared config: Spring Boot BOM, JUnit 5, Java 25 toolchain
5. Verify `./gradlew projects` lists all nine subprojects and `./gradlew build` compiles without errors across all of them

---

## Group 2 — Gateway Application Stub

Wire a minimal, runnable Spring Cloud Gateway application in `apps/gateway`. All other apps remain stubs; all libs remain empty.

1. Add Spring Boot plugin, `spring-cloud-starter-gateway`, and `spring-boot-starter-actuator` to `apps/gateway/build.gradle`
2. Create `GatewayApplication.java` main class in `org.eventviewer.gateway`
3. Configure `apps/gateway/src/main/resources/application.yml`: server port `8080`, actuator health endpoint exposed, app name `event-viewer-gateway`
4. Write one smoke-test `GatewayApplicationTests.java` that loads the Spring context (`@SpringBootTest`)
5. Verify `./gradlew :apps:gateway:bootRun` starts and `GET http://localhost:8080/actuator/health` returns `{"status":"UP"}`

---

## Group 3 — Frontend Scaffold

Bootstrap the React + TypeScript frontend as a standalone Vite project in `frontend/` at the repo root (not a Gradle subproject).

1. Run `npm create vite@latest frontend -- --template react-ts` to generate the scaffold
2. Install baseline dependencies: `react-router-dom`, `axios`, `zustand`
3. Replace generated boilerplate with a shell layout: `App.tsx` with a `<Router>`, one `/` route rendering a placeholder `HomePage`
4. Add `frontend/.nvmrc` pinned to the Node LTS version in use
5. Verify `npm run dev` in `frontend/` starts on `http://localhost:5173` and `npm run build` produces a clean `dist/`

---

## Group 4 — Docker Compose (Local Dev)

Define the full local infrastructure stack. All services must reach a healthy state with a single command.

1. Create `docker-compose.yml` at the repo root with services: `kafka` (KRaft), `postgres`, `opensearch`, `opensearch-dashboards`, `localstack`
2. Configure Kafka with KRaft env vars (`KAFKA_NODE_ID`, `KAFKA_PROCESS_ROLES=broker,controller`, `CLUSTER_ID`); no Zookeeper
3. Add a LocalStack init script (`/etc/localstack/init/ready.d/01-create-bucket.sh`) that runs `awslocal s3 mb s3://event-viewer-local` on container start
4. Configure each service with a fixed local port, named volume, and a `healthcheck` (see requirements for port assignments)
5. Add `docker-compose.env` (gitignored) for secrets; provide `docker-compose.env.example` with safe placeholder values including a pre-generated `CLUSTER_ID`
6. Verify `docker compose up -d` brings all five containers to `healthy` status within 90 seconds

---

## Group 5 — GitHub Actions CI

Gate every push and pull request. The pipeline must pass before a branch can merge.

1. Create `.github/workflows/ci.yml` with two jobs: `backend` and `frontend`
2. `backend` job: checkout → Java 25 setup → `./gradlew build` (compiles + runs tests)
3. `frontend` job: checkout → Node LTS setup → `npm ci` → `npm run build` (in `frontend/`)
4. `smoke-test` job (depends on both): start Docker Compose services → wait for healthy → hit `GET /actuator/health` → assert HTTP 200 → tear down
5. Verify the workflow passes on a test branch before marking Phase 1 complete
