# Phase 3 — Leader Election: Requirements

## Scope

Build distributed leadership coordination so that exactly one pod at a time runs singleton scheduled tasks. This is a foundational infrastructure phase — every subsequent phase that needs a single-node scheduler (Kafka lag monitoring, rule-cache refresh, retention jobs) depends on this.

---

## Problem

When `event-ingest` runs at multiple replicas, scheduled tasks like Kafka lag monitoring would fire on every pod simultaneously. This causes duplicate metrics, duplicate side-effects, and wasted work. Coordinated leadership ensures exactly one pod is the "active" scheduler at any moment, with automatic failover when that pod dies or loses connectivity.

---

## What Is Being Built

### Redis (new infrastructure dependency)

Redis is added to `docker-compose.yml` (local dev) and `docker-compose-test.yml` (integration tests). Standalone mode only. Redis is always required — there is no "always-leader" fallback mode.

### `libs/leader` (new library)

A dedicated Gradle module for leader election. Leader election uses Redisson (Redis-backed distributed lock with Watchdog), which brings Netty and other transitive dependencies. It must not pollute `libs/common`.

| Component | Description |
|---|---|
| `RedisLeaderElectionProperties` | `@ConfigurationProperties(prefix = "leader-election")` — `lockName` (default `leader:event-ingest`), `retryIntervalMs` (default 2000), `lockWatchdogTimeoutMs` (default 30000) |
| `LeaderListener` | Interface: `void onLeader(long fencingToken)`, `void onLeaderLoss(long fencingToken)`; any Spring bean implementing this is auto-discovered and notified on leadership transitions |
| `LeaderTask` | `@FunctionalInterface` with `void execute() throws Exception`; passed to `LeaderAwareScheduler.runIfLeader()` |
| `RedissonLeaderElectionService` | Acquires and holds a Redisson `RLock` in watchdog mode; increments an `RAtomicLong` fencing counter on each acquisition; runs an election loop via a single-thread executor; notifies all `LeaderListener` beans on state changes; exposes `boolean isLeader()` and `long getFencingToken()` |
| `LeaderAwareScheduler` | Interface: `void runIfLeader(LeaderTask task) throws Exception`; no-op on followers; propagates exceptions from the task to the caller |
| `LeaderElectionAutoConfiguration` | Registers `RedissonLeaderElectionService` and `LeaderAwareSchedulerImpl` as Spring beans; `RedissonClient` auto-configured via `redisson-spring-boot-starter` from `spring.data.redis.*` properties |

### `apps/event-ingest` additions

| Component | Description |
|---|---|
| `KafkaLagMonitor` | `@Scheduled` bean; interval configurable via `kafka.lag-monitor.interval-ms` (default 60 s); wrapped in `LeaderAwareScheduler.runIfLeader()`; queries `AdminClient.listConsumerGroupOffsets()` + `listOffsets()` for each configured consumer group; emits `kafka.consumer.lag` gauge per (topic, partition, consumer-group) |
| `KafkaLagMonitorProperties` | `@ConfigurationProperties(prefix = "kafka.lag-monitor")` — `enabled`, `intervalMs`, `consumerGroupIds` (list) |

---

## Key Decisions

### Redisson + RLock Watchdog

Redisson's `RLock` in watchdog mode (`tryLock(0, -1, SECONDS)`) is used for leader election. The watchdog automatically renews the lock TTL in the background as long as the JVM is alive. When the JVM is killed or the connection is lost and not restored within `lockWatchdogTimeout`, the watchdog thread stops and the lock expires naturally — freeing leadership for another pod without any explicit release.

Default watchdog timeout: 30 seconds. Redisson renews the lock every `lockWatchdogTimeout / 3` seconds (10 s by default). Configurable via `leader-election.lock-watchdog-timeout-ms`.

### Fencing Token via RAtomicLong

Each time the lock is acquired, a Redisson `RAtomicLong` at `{lockName}:fence` is incremented and the returned value becomes the **fencing token** for that leadership tenure. The token monotonically increases across restarts and failovers because it lives in Redis, not in-memory.

- `LeaderElectionService.getFencingToken()` returns the current token when leader, `-1L` when not
- Callers performing protected operations (schema migrations, scheduled writes) should obtain the token before acting and verify their copy matches the current token before committing
- This protects against zombie leaders: a pod that paused long enough for its lock to expire and a new leader to be elected cannot perform destructive writes with a stale token

### Election Loop on Executor Thread

A single-thread executor submits the election loop immediately on `@PostConstruct`. The bootstrapping thread returns at once — the election loop runs independently. The loop:

```
executor.submit(() -> {
    while (!Thread.currentThread().isInterrupted() && !shuttingDown) {
        try {
            boolean acquired = lock.tryLock(0, -1, SECONDS);
            if (acquired) {
                long token = fencingCounter.incrementAndGet();
                currentFencingToken = token;
                isLeaderFlag = true;
                listeners.forEach(l -> l.onLeader(token));

                while (lock.isHeldByCurrentThread() && !interrupted && !shuttingDown) {
                    Thread.sleep(retryIntervalMs / 2);
                }

                if (!shuttingDown) {
                    // Connection drop or TTL expiry
                    isLeaderFlag = false;
                    currentFencingToken = -1L;
                    listeners.forEach(l -> l.onLeaderLoss(token));
                }
            } else {
                Thread.sleep(retryIntervalMs);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
});
```

### LeaderListener — Custom Event Notification (Not Spring ApplicationEvents)

Leadership state changes are broadcast to all beans implementing `LeaderListener`. Spring auto-discovers these by constructor-injecting `List<LeaderListener>` into `RedissonLeaderElectionService` — no custom annotation or classpath scanner is needed; Spring's native collection injection handles discovery.

`LeaderListener.onLeader(long fencingToken)` — called on the election thread; implementations should return quickly. Long-running initialization (e.g., starting a scheduled task) should be submitted to an executor inside `onLeader()`.

`LeaderListener.onLeaderLoss(long fencingToken)` — called on the same thread; use the token to confirm the tenure being relinquished matches any token the listener stored on `onLeader()`.

### Exception Propagation in `LeaderAwareScheduler`

`runIfLeader(LeaderTask task)` propagates all exceptions (checked and unchecked) from the task directly to the caller. Callers are responsible for handling failures. The scheduler never swallows exceptions.

```java
@FunctionalInterface
public interface LeaderTask {
    void execute() throws Exception;
}

void runIfLeader(LeaderTask task) throws Exception;
```

### Failure Modes and Protections

| Scenario | Protection |
|---|---|
| Graceful pod shutdown | `DisposableBean.destroy()` calls `lock.unlock()`, resets `isLeaderFlag`, notifies all listeners via `onLeaderLoss()`, then shuts down the executor — immediate handoff |
| JVM killed (`kill -9`) | Watchdog thread dies; lock TTL expires within `lockWatchdogTimeout` (default 30 s); another pod acquires, increments fencing token |
| Network partition / connection drop | Watchdog cannot renew; lock TTL expires; `isHeldByCurrentThread()` returns false; listeners notified; reconnection re-enters the acquisition loop |
| Redis restart | Lock and fencing counter are lost; all pods attempt re-acquisition; first to succeed becomes leader; fencing counter resets to 0 (acceptable — counter is only an ordering guarantee, not a global truth) |
| Zombie leader (GC pause / long stop-the-world) | Watchdog stops during pause; lock expires; new leader acquires and increments token; when zombie resumes, `getFencingToken()` is stale — protected operations compare tokens and reject stale writes |

### `isLeader()` is Locally Cached

`isLeader()` reads a `volatile boolean` — it never makes a Redis call. The watchdog and election loop maintain this flag. This guarantees sub-millisecond performance for all callers.

### Metric Names Use Dot Notation

All Micrometer metric names use `.` as the separator. The Prometheus registry converts dots to underscores automatically. Metrics defined in this phase:

| Micrometer name | Type | Description |
|---|---|---|
| `leader.election.acquisitions` | Counter | Incremented on each successful lock acquisition |
| `leader.election.relinquishments` | Counter | Incremented on each lock release (graceful or forced) |
| `leader.election.connection.losses` | Counter | Incremented when lock is lost due to connection drop (not graceful shutdown) |
| `leader.election.is.leader` | Gauge | 1.0 when leader, 0.0 when follower |
| `leader.election.fencing.token` | Gauge | Current fencing token value; -1.0 when not leader |
| `leader.aware.scheduler.skipped` | Counter | Incremented when `runIfLeader` is a no-op (not leader) |
| `leader.aware.scheduler.execution` | Timer | Execution time of tasks that ran as leader |
| `kafka.lag.monitor.check` | Timer | Full execution time of each `KafkaLagMonitor` scheduled run |
| `kafka.consumer.lag` | Gauge | Lag per (group, topic, partition) tag tuple |

### No K8s Dependency

No Kubernetes Java client, no RBAC manifests, no Lease objects. Redis is the only external coordination dependency.

### `spring.data.redis.*` for Connection Config

Redisson is configured via `spring.data.redis.host` / `port` / `password`. The `redisson-spring-boot-starter` reads these and builds the `RedissonClient` bean automatically. Redis password is injected via `REDIS_PASSWORD` env var and never committed.

---

## Out of Scope

- Kafka consumer implementation (Phase 4)
- S3 or OpenSearch writes (Phases 5–6)
- Full SASL Kafka configuration (Phase 4)
- Any PostgreSQL changes — no schema migrations in this phase
- Frontend changes
- Redis Sentinel or Cluster configuration (standalone only in Phase 3)

---

## Non-Functional Requirements

| Concern | Requirement |
|---|---|
| Availability | Leadership transfers within `lockWatchdogTimeout` of a pod crash (default 30 s); within `retryIntervalMs` of a graceful shutdown (default 2 s) |
| Observability | Nine meters defined above; all use dot notation |
| Security | No Redis credentials committed; password injected via `REDIS_PASSWORD` env var; `RedisLeaderElectionProperties` must not expose sensitive fields in `/actuator/env` |
| Testability | All components unit-testable with mocked `RedissonClient` and `RLock`; integration tests use Redis from `docker-compose-test.yml`; tests never use `/actuator/prometheus` or metric assertions to verify functional correctness — see Platform Standards in `specs/architecture.md` |
| Performance | `isLeader()` and `getFencingToken()` must be < 1 ms (read cached local state, no Redis call) |

---

## Dependencies

- Phase 1 ✅ — project skeleton; `settings.gradle` must be updated to `include ':libs:leader'`
- Phase 2 ✅ — `apps/event-ingest` exists; Kafka admin client available (Spring Kafka)
- `org.redisson:redisson-spring-boot-starter` — new third-party dependency added to `libs/leader/build.gradle` only
- `apps/event-ingest/build.gradle` — must add `implementation project(':libs:leader')`
- Redis running in `docker-compose.yml` and `docker-compose-test.yml`
