# Phase 3 — Leader Election: Requirements

## Scope

Build distributed leadership coordination so that exactly one pod at a time runs singleton scheduled tasks. This is a foundational infrastructure phase — every subsequent phase that needs a single-node scheduler (Kafka lag monitoring, rule-cache refresh, retention jobs) depends on this.

---

## Problem

When `event-ingest` runs at multiple replicas in Kubernetes, scheduled tasks like Kafka lag monitoring would fire on every pod simultaneously. This causes duplicate metrics, duplicate side-effects, and wasted work. Coordinated leadership ensures exactly one pod is the "active" scheduler at any moment, with automatic failover when that pod dies.

---

## What Is Being Built

### `libs/leader` (new library)

A dedicated Gradle module for leader election. Leader election is K8s-specific infrastructure with its own dependency graph (`io.kubernetes:client-java`). It must not pollute `libs/common`, which is a zero-overhead dependency pulled into every module.

| Component | Description |
|---|---|
| `LeaderElectionProperties` | `@ConfigurationProperties(prefix = "kubernetes.leader-election")` — `enabled`, `leaseName`, `namespace`, `leaseDurationSeconds` (default 15), `renewDeadlineSeconds` (default 10), `retryPeriodSeconds` (default 2) |
| `LeaderElectionService` | Acquires and renews a K8s Coordination API `Lease` object; publishes `LeaderElectedEvent` and `LeaderRelinquishedEvent` to Spring `ApplicationEventPublisher`; exposes `boolean isLeader()` |
| `LeaderAwareScheduler` | Interface: `void runIfLeader(Runnable task)`; no-op on followers; implementation delegates to `LeaderElectionService.isLeader()` |
| `LeaderElectionAutoConfiguration` | `@ConditionalOnProperty(name = "kubernetes.leader-election.enabled", havingValue = "true")` — K8s beans registered only when K8s mode is active; otherwise the standalone fallback is registered |
| Non-K8s fallback | When `kubernetes.leader-election.enabled=false`, `isLeader()` always returns `true`; `LeaderAwareScheduler.runIfLeader()` always executes the task. This is the default for local Docker Compose dev and CI. |

### `apps/event-ingest` additions

| Component | Description |
|---|---|
| `KafkaLagMonitor` | `@Scheduled` bean; interval configurable via `kafka.lag-monitor.interval-ms` (default 60 s — once per minute); wrapped in `LeaderAwareScheduler.runIfLeader()`; queries `AdminClient.listConsumerGroupOffsets()` + `listOffsets()` for each configured consumer group; emits `kafka.consumer.lag` gauge per (topic, partition, consumer-group) |
| `KafkaLagMonitorProperties` | `@ConfigurationProperties(prefix = "kafka.lag-monitor")` — `enabled`, `intervalMs`, `consumerGroupIds` (list) |

### `infra/k8s/` directory (new, at repo root)

A root `infra/` folder groups all infrastructure-as-code. K8s manifests live under `infra/k8s/`. Future additions (Terraform, Helm charts, Kustomize overlays) also land in `infra/`.

| File | Description |
|---|---|
| `infra/k8s/leader-election-rbac.yaml` | `ServiceAccount`, `Role` (verbs: get/create/update on `leases`), `RoleBinding` scoped to the deployment namespace |
| `infra/k8s/event-ingest-deployment.yaml` | Stub K8s `Deployment` for `apps/event-ingest`; includes `MY_POD_NAME` (downward API `fieldRef: fieldPath: metadata.name`) and `MY_POD_NAMESPACE` (downward API `fieldRef: fieldPath: metadata.namespace`) env vars |
| `infra/k8s/README.md` | Local K8s deployment steps |

---

## Key Decisions

### Dedicated `libs/leader` Library

Leader election pulls in `io.kubernetes:client-java` which has significant transitive dependencies (OkHttp, Bouncy Castle, etc.). Isolating it in `libs/leader` means only apps that explicitly declare a dependency on it carry that weight. Apps that never need leader election (gateway, bff, event-read, management) stay unaffected.

### Kubernetes Java Client

Use the official `io.kubernetes:client-java` library (not Fabric8). The Coordination API `LeaderElector` utility handles Lease create/update/renew lifecycle. Pod identity comes from `MY_POD_NAME` env var (K8s downward API) — never from hostname parsing.

### Lease Name

Configurable via `kubernetes.leader-election.lease-name` (default: `event-ingest-leader`). Must be unique per deployment within a namespace. Using a configurable name allows multiple independent `event-ingest` deployments in the same namespace without conflict.

### Timing Parameters

`leaseDurationSeconds` must be greater than `renewDeadlineSeconds`. `renewDeadlineSeconds` must be greater than `retryPeriodSeconds`. Defaults (15 / 10 / 2) follow the Kubernetes controller-manager convention. The lag monitor interval (60 s) is intentionally much longer than `leaseDurationSeconds` (15 s) to avoid any chance of a follower executing during a brief leadership gap.

### Non-K8s Fallback

`kubernetes.leader-election.enabled=false` is the default. In this mode the `LeaderElectionService` is not instantiated and `isLeader()` returns `true`. This means Docker Compose dev, unit tests, and CI all work with zero K8s dependency. The K8s path is exercised only during the local K8s deployment test.

### No Spring Integration / Spring Cloud Kubernetes

The official Kubernetes Java client is used directly to keep the dependency surface small and avoid Spring Cloud Kubernetes version coupling. `LeaderElectionService` wraps only what is needed.

### RBAC Minimalism

The ServiceAccount is granted only `leases` resource access (`get`, `create`, `update`). No other K8s API access is requested. The role is namespaced (not cluster-scoped).

### `infra/` Root Folder

All infrastructure-as-code lives under a single root `infra/` folder. This keeps K8s manifests, and any future Terraform or Helm work, in one discoverable location rather than scattered at the repo root.

---

## Out of Scope

- Kafka consumer implementation (Phase 4)
- S3 or OpenSearch writes (Phases 5–6)
- Full SASL Kafka configuration (Phase 4)
- Any PostgreSQL changes — no schema migrations in this phase
- Frontend changes

---

## Non-Functional Requirements

| Concern | Requirement |
|---|---|
| Availability | Leadership must transfer within `leaseDurationSeconds` of a pod crash (15 s default) |
| Observability | Three meters required: `leader_election_acquisitions_total` counter, `leader_election_relinquishments_total` counter, `leader_election_is_leader` gauge (1.0 on leader, 0.0 on follower) |
| Security | No K8s credentials committed; ServiceAccount token auto-mounted by K8s; `LeaderElectionProperties` must not expose sensitive fields in `/actuator/env` |
| Testability | All components testable with `kubernetes.leader-election.enabled=false`; K8s-path tested via local K8s deployment only |
| Performance | Leader election overhead must be < 1 ms per `isLeader()` call (cached boolean, not a K8s API call on every check) |

---

## Dependencies

- Phase 1 ✅ — project skeleton; `settings.gradle` must be updated to `include ':libs:leader'`
- Phase 2 ✅ — `apps/event-ingest` exists; Kafka admin client available (Spring Kafka)
- `io.kubernetes:client-java` — new third-party dependency added to `libs/leader/build.gradle` only
- `apps/event-ingest/build.gradle` — must add `implementation project(':libs:leader')`
