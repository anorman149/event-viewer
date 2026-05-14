# Phase 3 — Leader Election: Validation

A phase is complete and mergeable when every criterion below passes. Work through the groups in order — later groups depend on earlier ones.

---

## Group 1 — Redis Infrastructure

| # | Check | How to verify |
|---|---|---|
| 1.1 | Redis service present in `docker-compose.yml` with healthcheck | `docker compose config` — `redis` service appears; `healthcheck.test` contains `redis-cli ping` |
| 1.2 | Redis service present in `docker-compose-test.yml` | Same check on test compose file |
| 1.3 | Redis starts and responds to PING | `docker compose up redis -d && docker compose exec redis redis-cli ping` → `PONG` |
| 1.4 | `spring.data.redis.*` properties present in `application.yml` | Manual review — `host`, `port`, `password` (env-var substituted) present |
| 1.5 | No Redis data volume in either compose file | Manual review — leader election state must be ephemeral; no named volume for Redis |

---

## Group 2 — `libs/leader` Module & Properties

| # | Check | How to verify |
|---|---|---|
| 2.1 | `libs/leader` module declared in `settings.gradle` | `./gradlew :libs:leader:build` — no "project not found" error |
| 2.2 | `libs/leader` compiles with Redisson dependency | `./gradlew :libs:leader:build` — no compilation errors |
| 2.3 | `RedisLeaderElectionPropertiesTest` passes | `./gradlew :libs:leader:test` — all binding and validation cases green |
| 2.4 | Cross-field validation rejects `lockWatchdogTimeoutMs <= retryIntervalMs` | Test case with invalid values expects `ConstraintViolationException` |
| 2.5 | No dependency version conflicts | `./gradlew :libs:leader:dependencies --configuration runtimeClasspath` — no `*` conflicts for Redisson or Netty |
| 2.6 | `libs/common` build is unaffected | `./gradlew :libs:common:build` still passes with no new dependencies |
| 2.7 | `LeaderTask` is a `@FunctionalInterface` with `throws Exception` | Code review — annotation present; method signature matches `void execute() throws Exception` |
| 2.8 | `LeaderListener` interface has both `onLeader(long)` and `onLeaderLoss(long)` | Code review — both methods present with the correct `long fencingToken` parameter |

---

## Group 3 — `RedissonLeaderElectionService`

| # | Check | How to verify |
|---|---|---|
| 3.1 | `isLeader()` and `getFencingToken()` make no Redis call | Code review — both methods read local `volatile` fields only; no `RedissonClient` or `RLock` reference inside |
| 3.2 | `tryLock` returns `false` → no listener notified | Unit test: mock `tryLock` → `false`; assert `onLeader()` never called on test listener; assert `isLeader()` = `false`; assert `getFencingToken()` = -1L |
| 3.3 | `tryLock` returns `true` → acquisition recorded | Unit test: `onLeader(token)` called on all injected listeners; `isLeader()` = `true`; `getFencingToken()` = mocked `incrementAndGet()` value |
| 3.4 | `isHeldByCurrentThread()` → false (not shutting down) → loss recorded | Unit test: `onLeaderLoss(token)` called; `isLeader()` = `false`; `getFencingToken()` = -1L; connection-losses counter = 1 |
| 3.5 | `destroy()` when leader → lock released; listeners notified | Unit test: verify `lock.unlock()` called; `onLeaderLoss(token)` called on all listeners; relinquishments counter = 1 |
| 3.6 | `destroy()` when not leader → no unlock; no listeners notified | Unit test: `lock.unlock()` never called; no listener call recorded |
| 3.7 | `startElectionLoop()` returns immediately (does not block bootstrapping) | Unit test: call `startElectionLoop()` with `tryLock` blocking indefinitely; assert method returns within 50 ms |
| 3.8 | Election loop retries after failed acquisition | Unit test: `tryLock` → `false` three times → `true`; assert `onLeader()` eventually called |
| 3.9 | Fencing token monotonically increases across acquisitions | Unit test: simulate acquisition, loss, re-acquisition; assert second `onLeader()` token > first token |
| 3.10 | All unit tests in `RedissonLeaderElectionServiceTest` pass | `./gradlew :libs:leader:test` — green |

---

## Group 4 — `LeaderAwareScheduler`

| # | Check | How to verify |
|---|---|---|
| 4.1 | Task runs when leader; exception propagates to caller | `LeaderAwareSchedulerTest` — `LeaderTask` throws `IOException`; assert `IOException` propagates from `runIfLeader()` |
| 4.2 | `RuntimeException` from task propagates | Same pattern with `IllegalStateException` |
| 4.3 | Task result is returned with no interference | Task completes normally when leader; assert no exception escapes |
| 4.4 | `leader.aware.scheduler.skipped` counter = 1 when not leader | Not-leader case: counter = 1; task `execute()` never called |
| 4.5 | `leader.aware.scheduler.execution` timer registered | Timer appears in `SimpleMeterRegistry` after a leader execution |

---

## Group 5 — `KafkaLagMonitor`

Validity is determined by correct behavior (AdminClient calls, lag computation), not by metric assertions — see Rules.md.

| # | Check | How to verify |
|---|---|---|
| 5.1 | No `AdminClient` calls when not leader | `KafkaLagMonitorTest` — mock `AdminClient`; `isLeader()` = `false`; trigger scheduled method; zero interactions on mock |
| 5.2 | `listConsumerGroupOffsets()` called with correct group ID when leader | `isLeader()` = `true`; assert `adminClient.listConsumerGroupOffsets("event-ingest-group")` called |
| 5.3 | `listOffsets()` called with the partitions returned by `listConsumerGroupOffsets()` | Assert partitions in `listOffsets()` call match those returned from group offset listing |
| 5.4 | Lag computation is correct | Given latestOffset=100, committedOffset=90; assert gauge is registered with value 10 for that (group, topic, partition) |
| 5.5 | Default `intervalMs` is 60 s | Properties default binding test asserts `intervalMs == 60000` |
| 5.6 | All `KafkaLagMonitorTest` cases pass | `./gradlew :apps:event-ingest:test` — green |

---

## Group 6 — Integration Tests

Tests assert deterministic behavior through service methods and mock interactions — not through `/actuator/prometheus` or metric endpoint responses (see Rules.md).

| # | Check | How to verify |
|---|---|---|
| 6.1 | `LeaderElectionIT` — service acquires leadership | `leaderElectionService.isLeader()` returns `true` within startup timeout |
| 6.2 | `LeaderElectionIT` — fencing token is positive | `leaderElectionService.getFencingToken() > 0` |
| 6.3 | `LeaderElectionIT` — listener notified on startup | Test `LeaderListener` bean's `onLeader()` called exactly once; `lastFencingToken()` matches `getFencingToken()` |
| 6.4 | `KafkaLagMonitorIT` — `AdminClient` was called | `adminClientSpy.listConsumerGroupOffsetsInvocationCount() >= 1` after overridden 500 ms interval fires |
| 6.5 | `LeaderElectionConnectionIT` — re-acquisition after simulated failure | After first `tryLock` throws `RedisConnectionException`, retry succeeds; `isLeader()` becomes `true` within 3 × `retryIntervalMs`; `getFencingToken() > 0` |
| 6.6 | All three IT classes pass | `./gradlew :apps:event-ingest:itest` — green |

---

## Group 7 — CI & Build

| # | Check | How to verify |
|---|---|---|
| 7.1 | `./gradlew test` passes all modules including `libs/leader` | CI `backend-unit` job green |
| 7.2 | `./gradlew :apps:event-ingest:itest` passes | CI `backend-itest` job green (Redis available in CI environment) |
| 7.3 | No ByteBuddy errors on Java 25 for `libs/leader` | `jvmArgs '-Dnet.bytebuddy.experimental=true'` confirmed in root `build.gradle` for all subprojects |
| 7.4 | No new compiler warnings | `./gradlew build --warning-mode all` — no new warnings |

---

## Merge Criteria Summary

- [ ] All unit tests in `libs/leader` pass
- [ ] All unit tests in `apps/event-ingest` pass
- [ ] `LeaderElectionIT`, `KafkaLagMonitorIT`, `LeaderElectionConnectionIT` all pass
- [ ] Redis service present and healthy in both `docker-compose.yml` and `docker-compose-test.yml`
- [ ] `leaderElectionService.isLeader()` returns `true` and `getFencingToken() > 0` confirmed in `LeaderElectionIT`
- [ ] `TestLeaderListener.onLeader()` call confirmed in `LeaderElectionIT`
- [ ] `libs/common` build is unaffected (no new dependencies)
- [ ] `./gradlew build` passes end-to-end with no errors
- [ ] No secrets or credentials committed
- [ ] No test assertions use `/actuator/prometheus`, `/actuator/metrics`, or Prometheus text output (see Rules.md)
