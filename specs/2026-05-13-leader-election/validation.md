# Phase 3 — Leader Election: Validation

A phase is complete and mergeable when every criterion below passes. Work through the groups in order — later groups depend on earlier ones.

---

## Group 1 — New Library Module & Properties

| # | Check | How to verify |
|---|---|---|
| 1.1 | `libs/leader` module declared in `settings.gradle` | `./gradlew :libs:leader:build` — no "project not found" error |
| 1.2 | `libs/leader` compiles with `client-java` added | `./gradlew :libs:leader:build` — no compilation errors |
| 1.3 | `LeaderElectionProperties` binds correctly from YAML | `LeaderElectionPropertiesTest` passes; all fields bound from test YAML fixture |
| 1.4 | Cross-field validation rejects `renewDeadline >= leaseDuration` | `LeaderElectionPropertiesTest` includes a case with invalid timing — expects `ConstraintViolationException` |
| 1.5 | No dependency version conflicts | `./gradlew :libs:leader:dependencies --configuration runtimeClasspath` — inspect for `client-java` transitive conflicts; resolve any shown as `*` |
| 1.6 | `libs/common` build is unaffected | `./gradlew :libs:common:build` still passes with no new dependencies |

---

## Group 2 — `LeaderElectionService`

| # | Check | How to verify |
|---|---|---|
| 2.1 | `KubernetesLeaderElectionServiceTest` passes | `./gradlew :libs:leader:test` — all tests green |
| 2.2 | `StandaloneLeaderElectionServiceTest` passes | Same command; verify `isLeader()` returns `true` and `LeaderElectedEvent` is published on startup |
| 2.3 | `LeaderElectionAutoConfiguration` wires `Standalone` when `enabled=false` | Integration test with `enabled=false` — assert `LeaderElectionService` bean present and `isLeader()` returns `true` |
| 2.4 | `LeaderElectionAutoConfiguration` wires `Kubernetes` when `enabled=true` | Integration test with `enabled=true` and mocked `LeaderElector` — assert `KubernetesLeaderElectionService` is the registered bean |
| 2.5 | Graceful shutdown releases lease | `KubernetesLeaderElectionServiceTest` — call `destroy()`, verify mock `LeaderElector` receives release signal |

---

## Group 3 — `LeaderAwareScheduler`

| # | Check | How to verify |
|---|---|---|
| 3.1 | Task runs exactly once when `isLeader()` is `true` | `LeaderAwareSchedulerTest` — verify runnable called exactly once |
| 3.2 | Task is skipped and counter incremented when `isLeader()` is `false` | `LeaderAwareSchedulerTest` — verify runnable never called; `leader_aware_scheduler.skipped_total` counter = 1 |
| 3.3 | Exception in task does not crash the scheduler or propagate to caller | `LeaderAwareSchedulerTest` — task throws `RuntimeException`; assert no exception escapes `runIfLeader()`; assert counter not incremented (task ran, it just failed) |
| 3.4 | `@Timed` metric registered | Verify `leader_aware_scheduler.execution` appears in a `SimpleMeterRegistry` in the test |

---

## Group 4 — `KafkaLagMonitor`

| # | Check | How to verify |
|---|---|---|
| 4.1 | `KafkaLagMonitorTest` passes | `./gradlew :apps:event-ingest:test` — all tests green |
| 4.2 | No `AdminClient` calls when `isLeader()` is `false` | Mock `AdminClient` in test; set leader = false; trigger scheduled method manually; verify zero interactions on mock |
| 4.3 | One gauge registered per (group, topic, partition) tuple | Set leader = true; mock `AdminClient` to return 2 partitions for 1 topic; assert 2 gauges in `MeterRegistry` |
| 4.4 | Gauge upsert — second invocation updates value, does not register a new gauge | Call monitor twice with different lag values; assert `meterRegistry.find("kafka.consumer.lag")` returns exactly 2 gauges (not 4) |
| 4.5 | Default interval is 60 s | `KafkaLagMonitorProperties` default binding test asserts `intervalMs == 60000` |
| 4.6 | Application starts without error with `kafka.lag-monitor.enabled=true` | `./gradlew :apps:event-ingest:itest` — `KafkaLagMonitorIT` starts context (interval overridden to 500 ms via `@TestPropertySource`) and asserts gauge exists in `/actuator/prometheus` |

---

## Group 5 — Observability Meters

| # | Check | How to verify |
|---|---|---|
| 5.1 | `leader_election_is_leader` gauge present in Prometheus output | `LeaderElectionIT` (standalone mode) — `GET /actuator/prometheus` body contains `leader_election_is_leader` with value `1.0` |
| 5.2 | `leader_election_acquisitions_total` counter = 1 after startup in standalone mode | Same IT — Prometheus output contains `leader_election_acquisitions_total 1.0` |
| 5.3 | `kafka.consumer.lag` gauge present after one monitor interval | `KafkaLagMonitorIT` — gauge appears in Prometheus output after first scheduled firing |
| 5.4 | All metric names follow platform naming convention | Manual review: snake_case; no spaces; no camelCase |

---

## Group 6 — K8s Manifests

| # | Check | How to verify |
|---|---|---|
| 6.1 | RBAC manifest is valid YAML | `kubectl apply --dry-run=client -f infra/k8s/leader-election-rbac.yaml` — no errors |
| 6.2 | Deployment manifest is valid YAML | `kubectl apply --dry-run=client -f infra/k8s/event-ingest-deployment.yaml` — no errors |
| 6.3 | `MY_POD_NAME` env var is downward API reference (not hardcoded) | Manual review — must use `fieldRef.fieldPath: metadata.name` |
| 6.4 | `serviceAccountName` references the account defined in RBAC manifest | Manual review — names must match exactly |
| 6.5 | No sensitive values committed | Manual review — no passwords, tokens, or keys in `infra/` |

---

## Group 7 — Local K8s Deployment Test (Manual)

Performed once before merge; not required in CI.

| # | Check | Steps |
|---|---|---|
| 7.1 | Single leader acquired | `kubectl apply -f infra/k8s/`; wait 30 s; `kubectl logs -l app=event-ingest --all-containers` — exactly one pod logs `"acquired leadership"` |
| 7.2 | Follower state logged | Second pod logs confirm follower state (no acquired leadership message) |
| 7.3 | Leader metrics on leader pod | `kubectl port-forward <leader-pod> 8080:8080`; `curl localhost:8080/actuator/prometheus` — `leader_election_is_leader 1.0` |
| 7.4 | Follower metrics on follower pod | Port-forward follower pod — `leader_election_is_leader 0.0` |
| 7.5 | Leadership transfers after pod kill | `kubectl delete pod <leader-pod>`; within `leaseDurationSeconds` (15 s) the remaining pod logs `"acquired leadership"`; `leader_election_acquisitions_total` counter increments on the new leader |
| 7.6 | Lag monitor runs only on leader | After leadership transfer, `kafka.consumer.lag` gauge updates only occur on the new leader pod |

---

## Group 8 — CI & Build

| # | Check | How to verify |
|---|---|---|
| 8.1 | `./gradlew test` passes on all modules including `libs/leader` | CI `backend-unit` job green |
| 8.2 | `./gradlew :apps:event-ingest:itest` passes | CI `backend-itest` job green |
| 8.3 | No ByteBuddy errors on Java 25 for `libs/leader` | `jvmArgs '-Dnet.bytebuddy.experimental=true'` confirmed in root `build.gradle` for all subprojects |
| 8.4 | No new compiler warnings introduced | `./gradlew build --warning-mode all` — review output for new warnings |

---

## Merge Criteria Summary

- [ ] All unit tests in `libs/leader` pass
- [ ] All unit tests in `apps/event-ingest` pass
- [ ] `LeaderElectionIT` and `KafkaLagMonitorIT` in `apps/event-ingest/src/itest` pass
- [ ] K8s manifests pass `kubectl --dry-run` validation
- [ ] Local K8s deployment test (Groups 7.1–7.6) completed and all checks pass
- [ ] `leader_election_is_leader`, `leader_election_acquisitions_total`, `leader_election_relinquishments_total`, and `kafka.consumer.lag` visible in `/actuator/prometheus`
- [ ] `libs/common` build is unaffected (no new dependencies added to it)
- [ ] `./gradlew build` passes end-to-end with no errors
- [ ] No secrets or credentials committed
