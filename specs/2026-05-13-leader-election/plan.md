# Phase 3 — Leader Election: Implementation Plan

---

## Group 1 — New Library Module & Kubernetes Client Dependency (`libs/leader`)

1. Add `include ':libs:leader'` to root `settings.gradle`; create `libs/leader/build.gradle` with `java-library` plugin, `spring-boot-starter` dependency, and `io.kubernetes:client-java` (pinned version); confirm no transitive conflict with Spring Boot BOM via `./gradlew :libs:leader:dependencies`
2. Add `LeaderElectionProperties` — `@ConfigurationProperties(prefix = "kubernetes.leader-election")`; fields: `enabled` (boolean, default false), `leaseName` (String, default `event-ingest-leader`), `namespace` (String, default `default`), `leaseDurationSeconds` (int, default 15), `renewDeadlineSeconds` (int, default 10), `retryPeriodSeconds` (int, default 2); validated with `@Validated` (`@Min` on timing fields, cross-field check: `renewDeadline < leaseDuration` and `retryPeriod < renewDeadline`)
3. Register `LeaderElectionProperties` and `LeaderElectionAutoConfiguration` in `libs/leader/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` so any app depending on `libs/leader` picks them up automatically
4. Unit test: `LeaderElectionPropertiesTest` — binding from YAML, cross-field validation rejects invalid timing combos

---

## Group 2 — `LeaderElectionService` (`libs/leader`)

5. `LeaderElectedEvent` and `LeaderRelinquishedEvent` — plain Spring `ApplicationEvent` subclasses carrying `podName` and `leaseName`
6. `LeaderElectionService` interface — `boolean isLeader()`, `String currentLeader()` (returns holder identity or empty string)
7. `KubernetesLeaderElectionService` — implementation; uses `io.kubernetes.client.extended.leaderelection.LeaderElector`; runs election loop on a dedicated virtual thread (daemon); updates `volatile boolean leader` flag on ACQUIRE / RELINQUISH callbacks; publishes events to `ApplicationEventPublisher`; implements `DisposableBean` to release lease on shutdown
8. `StandaloneLeaderElectionService` — fallback implementation when `kubernetes.leader-election.enabled=false`; `isLeader()` always returns `true`; publishes `LeaderElectedEvent` on `@PostConstruct`
9. `LeaderElectionAutoConfiguration` — `@ConditionalOnProperty(name="kubernetes.leader-election.enabled", havingValue="true")` registers `KubernetesLeaderElectionService`; else registers `StandaloneLeaderElectionService`; both exposed as `LeaderElectionService` bean
10. Unit tests:
    - `KubernetesLeaderElectionServiceTest` — mock `LeaderElector`; verify `isLeader()` transitions, events published on acquire/relinquish, graceful shutdown releases lease
    - `StandaloneLeaderElectionServiceTest` — verify `isLeader()` always true, `LeaderElectedEvent` published on startup

---

## Group 3 — `LeaderAwareScheduler` (`libs/leader`)

11. `LeaderAwareScheduler` interface — single method `void runIfLeader(Runnable task)`
12. `LeaderAwareSchedulerImpl` — injects `LeaderElectionService`; `runIfLeader` calls `isLeader()` and, if true, executes task; if false, increments `leader_aware_scheduler.skipped_total` counter and returns immediately
13. `@Timed(histogram=true, name="leader_aware_scheduler.execution")` on the execution path
14. Register `LeaderAwareSchedulerImpl` as a bean in `LeaderElectionAutoConfiguration` (both K8s and standalone branches)
15. Unit tests: task runs when leader, task skipped + counter incremented when follower, exception in task does not crash scheduler or propagate to caller

---

## Group 4 — `KafkaLagMonitor` (`apps/event-ingest`)

16. Add `implementation project(':libs:leader')` to `apps/event-ingest/build.gradle`
17. `KafkaLagMonitorProperties` — `@ConfigurationProperties(prefix = "kafka.lag-monitor")`; fields: `enabled` (boolean), `intervalMs` (long, default 60000 — once per minute), `consumerGroupIds` (List<String>)
18. `KafkaLagMonitor` — Spring `@Service`; constructor-injects `AdminClient`, `LeaderAwareScheduler`, `MeterRegistry`, `KafkaLagMonitorProperties`
19. `@Scheduled` method (virtual thread executor from Phase 2); wraps body in `leaderAwareScheduler.runIfLeader(...)`:
    - calls `AdminClient.listConsumerGroupOffsets(groupId)` for each configured group
    - calls `AdminClient.listOffsets(topicPartitions)` with `OffsetSpec.latest()` for the same partitions
    - computes lag per (group, topic, partition) = latest offset − committed offset
    - upserts `kafka.consumer.lag` `Gauge` per `(group, topic, partition)` tag tuple via `Gauge.builder(...).register(meterRegistry)` (upsert pattern — second call updates value, does not register a second gauge)
20. `kafka.consumer.lag` gauge retains its last value when the pod is a follower (monitor skips; gauge is not removed — acceptable and documented)
21. `@Timed(histogram=true, name="kafka.lag_monitor.check")` on the full execution
22. Unit tests: `KafkaLagMonitorTest` — mock `AdminClient`; verify gauges registered per (group, topic, partition); verify no `AdminClient` calls when `isLeader()` returns false; verify upsert behavior on second invocation
23. Add to `apps/event-ingest/application.yml`:
    ```yaml
    kafka:
      lag-monitor:
        enabled: true
        interval-ms: 60000
        consumer-group-ids:
          - event-ingest-group
    ```

---

## Group 5 — Observability Meters (`libs/leader` + `apps/event-ingest`)

24. In `KubernetesLeaderElectionService` and `StandaloneLeaderElectionService`:
    - `leader_election_acquisitions_total` — `Counter`; incremented on each ACQUIRE callback (standalone: incremented once on startup)
    - `leader_election_relinquishments_total` — `Counter`; incremented on each RELINQUISH callback
    - `leader_election_is_leader` — `Gauge` backed by `AtomicInteger` (1 when leader, 0 when follower); registered on construction
25. Verify all meters appear under `GET /actuator/prometheus` in the `event-ingest` integration test
26. Confirm `apps/event-ingest/application.yml` has the `management.tracing` block from Phase 2; add `kafka.template.observation-enabled: true` if not already present

---

## Group 6 — K8s Manifests (`infra/k8s/`)

27. Create `infra/` at repo root; create `infra/k8s/` subdirectory
28. `infra/k8s/leader-election-rbac.yaml`:
    - `ServiceAccount` named `event-ingest`
    - `Role` with rules: `apiGroups: ["coordination.k8s.io"]`, `resources: ["leases"]`, `verbs: ["get", "create", "update"]`
    - `RoleBinding` binding the role to the service account
    - Namespace parameterizable (comment indicating kustomize overlay or `envsubst` for CI)
29. `infra/k8s/event-ingest-deployment.yaml`:
    - `spec.replicas: 2` (minimum for testing leader election)
    - `serviceAccountName: event-ingest`
    - `env` entries for `MY_POD_NAME` (downward API `metadata.name`) and `MY_POD_NAMESPACE` (`metadata.namespace`)
    - `env` entries for `KUBERNETES_LEADER_ELECTION_ENABLED=true`, `KUBERNETES_LEADER_ELECTION_LEASE_NAME=event-ingest-leader`
    - `SPRING_PROFILES_ACTIVE=local` for local K8s dev
    - Resource requests/limits placeholder (comment: fill in after benchmarking phase)
    - Liveness probe: `GET /actuator/health/liveness`, readiness probe: `GET /actuator/health/readiness`
30. `infra/k8s/README.md` — local K8s deployment steps: build image, push to local registry, `kubectl apply -f infra/k8s/`, commands to verify leader election, commands to test failover

---

## Group 7 — Integration Tests (`apps/event-ingest/src/itest`)

31. Confirm `BaseTest` exists from Phase 2 itest suite
32. `LeaderElectionIT` — start application with `kubernetes.leader-election.enabled=false` (standalone mode); `GET /actuator/prometheus` response body contains `leader_election_is_leader` with value `1.0`; `leader_election_acquisitions_total` counter = 1
33. `KafkaLagMonitorIT` — start application in standalone mode; wait one monitor interval (60 s; consider setting `kafka.lag-monitor.interval-ms=500` via `@TestPropertySource` for the itest to avoid a 60 s wait); verify `kafka.consumer.lag` gauge appears in `/actuator/prometheus` for the configured consumer group
34. Manual local K8s test (not automated CI) — steps documented in `infra/k8s/README.md`: deploy 2 replicas, `kubectl logs` confirm single leader, `kubectl delete pod <leader>`, verify second pod logs ACQUIRED within `leaseDurationSeconds`

---

## Group 8 — CI & Build Wiring

35. Confirm `jvmArgs '-Dnet.bytebuddy.experimental=true'` in root `build.gradle` covers the new `:libs:leader` subproject (verify `allprojects` or `subprojects` scope)
36. `libs/leader` has unit tests only (no itest); confirm it is excluded from `check.dependsOn itest` wiring or that the absence of an `itest` source set does not break the task
37. Confirm `./gradlew build` passes end-to-end: all modules compile, all unit tests pass, `event-ingest` itest passes
