# Phase 3 — Leader Election: Implementation Plan

---

## Group 1 — Redis Infrastructure

1. Add Redis service to `docker-compose.yml`:
   ```yaml
   redis:
     image: redis:7-alpine
     ports:
       - "6379:6379"
     command: redis-server --save "" --appendonly no
     healthcheck:
       test: ["CMD", "redis-cli", "ping"]
       interval: 5s
       timeout: 3s
       retries: 10
   ```
   No named volume — leader election state is ephemeral and must not survive Redis restarts.

2. Add identical Redis service to `docker-compose-test.yml` (same image, ephemeral, same healthcheck).

3. Add Redis connection properties to `apps/event-ingest/src/main/resources/application.yml`:
   ```yaml
   spring:
     data:
       redis:
         host: ${REDIS_HOST:localhost}
         port: ${REDIS_PORT:6379}
         password: ${REDIS_PASSWORD:}
   ```

---

## Group 2 — `libs/leader` Module, Dependencies & Properties

4. Add `include ':libs:leader'` to root `settings.gradle`; create `libs/leader/build.gradle` with `java-library` plugin, `spring-boot-starter`, and `org.redisson:redisson-spring-boot-starter` (pinned version); confirm no transitive conflict via `./gradlew :libs:leader:dependencies --configuration runtimeClasspath`.

5. `RedisLeaderElectionProperties` — `@ConfigurationProperties(prefix = "leader-election")`; fields:
   - `lockName` (String, default `leader:event-ingest`) — also used as prefix for fencing counter key (`{lockName}:fence`)
   - `retryIntervalMs` (long, default 2000) — `@Min(500)` validation
   - `lockWatchdogTimeoutMs` (long, default 30000) — `@Min(5000)` validation; must be > `retryIntervalMs` (`@AssertTrue` cross-field)
   Annotate with `@Validated`.

6. `LeaderTask` — `@FunctionalInterface` in `libs/leader`; single method `void execute() throws Exception`. This is the functional type accepted by `LeaderAwareScheduler`.

7. `LeaderListener` — interface in `libs/leader`:
   ```java
   public interface LeaderListener {
       void onLeader(long fencingToken);
       void onLeaderLoss(long fencingToken);
   }
   ```
   Any Spring bean implementing this interface is automatically discovered via `List<LeaderListener>` constructor injection into `RedissonLeaderElectionService`.

8. `LeaderElectionService` interface — `boolean isLeader()`, `long getFencingToken()` (returns current token when leader, `-1L` when not).

9. Register `LeaderElectionAutoConfiguration` in `libs/leader/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`.

10. Unit test: `RedisLeaderElectionPropertiesTest` — binding from YAML fixture; `@Min` validation rejects `retryIntervalMs < 500`; cross-field check rejects `lockWatchdogTimeoutMs <= retryIntervalMs`.

---

## Group 3 — `RedissonLeaderElectionService` (`libs/leader`)

11. `RedissonLeaderElectionService` — implements `LeaderElectionService`, `DisposableBean`:
    - Constructor-injects `RedissonClient`, `RedisLeaderElectionProperties`, `List<LeaderListener>`, `MeterRegistry`
    - Registers all meters in constructor: `leader.election.acquisitions` counter, `leader.election.relinquishments` counter, `leader.election.connection.losses` counter, `leader.election.is.leader` gauge (backed by `AtomicInteger`), `leader.election.fencing.token` gauge (backed by `AtomicLong`, value -1 initially)
    - Builds `RLock lock = redissonClient.getLock(properties.getLockName())`
    - Builds `RAtomicLong fencingCounter = redissonClient.getAtomicLong(properties.getLockName() + ":fence")`
    - `volatile boolean isLeaderFlag` and `volatile long currentFencingToken = -1L` — read by `isLeader()` and `getFencingToken()` with no Redis call

12. `@PostConstruct startElectionLoop()` — submits the loop to a single-thread executor. Returns immediately; bootstrapping is not blocked:
    ```java
    private final ExecutorService electionExecutor =
        Executors.newSingleThreadExecutor(Thread.ofVirtual().name("leader-election-loop").factory());

    @PostConstruct
    void startElectionLoop() {
        electionExecutor.submit(this::runElectionLoop);
    }
    ```

13. `runElectionLoop()` implementation:
    ```java
    private void runElectionLoop() {
        while (!Thread.currentThread().isInterrupted() && !shuttingDown) {
            try {
                boolean acquired = lock.tryLock(0, -1, SECONDS); // no wait; watchdog manages TTL
                if (acquired) {
                    long token = fencingCounter.incrementAndGet();
                    currentFencingToken = token;
                    isLeaderFlag = true;
                    isLeaderGauge.set(1);
                    fencingTokenGauge.set(token);
                    acquisitionsCounter.increment();
                    listeners.forEach(l -> l.onLeader(token));

                    // Monitor lock retention
                    while (lock.isHeldByCurrentThread()
                            && !Thread.currentThread().isInterrupted()
                            && !shuttingDown) {
                        Thread.sleep(properties.getRetryIntervalMs() / 2);
                    }

                    if (!shuttingDown) {
                        // Connection drop or TTL expiry — not a clean shutdown
                        long lostToken = currentFencingToken;
                        isLeaderFlag = false;
                        currentFencingToken = -1L;
                        isLeaderGauge.set(0);
                        fencingTokenGauge.set(-1L);
                        relinquishmentsCounter.increment();
                        connectionLossesCounter.increment();
                        listeners.forEach(l -> l.onLeaderLoss(lostToken));
                    }
                } else {
                    Thread.sleep(properties.getRetryIntervalMs());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
    ```

14. `destroy()` — graceful shutdown:
    ```java
    @Override
    public void destroy() throws Exception {
        shuttingDown = true;
        electionExecutor.shutdownNow(); // interrupts the loop thread
        if (lock.isHeldByCurrentThread()) {
            long token = currentFencingToken;
            isLeaderFlag = false;
            currentFencingToken = -1L;
            isLeaderGauge.set(0);
            fencingTokenGauge.set(-1L);
            relinquishmentsCounter.increment();
            lock.unlock();
            listeners.forEach(l -> l.onLeaderLoss(token));
        }
        electionExecutor.awaitTermination(5, SECONDS);
    }
    ```

15. `LeaderElectionAutoConfiguration` — registers `RedissonLeaderElectionService` as `LeaderElectionService` bean and `LeaderAwareSchedulerImpl` as `LeaderAwareScheduler` bean.

16. Unit tests (`RedissonLeaderElectionServiceTest`) — mock `RedissonClient`, `RLock`, `RAtomicLong`, `MeterRegistry`, and use a test `LeaderListener` that records calls:
    - `tryLock` returns `false` → `isLeader()` is `false`; no listener notified; `getFencingToken()` = -1L
    - `tryLock` returns `true` → `isLeader()` is `true`; `onLeader(token)` called on all listeners; `getFencingToken()` returns mocked `incrementAndGet()` value; acquisitions counter = 1
    - `isHeldByCurrentThread()` returns `true` then `false` (while not shutting down) → `onLeaderLoss(token)` called; `isLeader()` is `false`; connection-losses counter = 1; relinquishments counter = 1
    - `destroy()` when leader → `lock.unlock()` called; `onLeaderLoss(token)` called on all listeners; relinquishments counter = 1
    - `destroy()` when not leader → `lock.unlock()` never called; no listener notified
    - `tryLock` fails three times then succeeds → `onLeader()` eventually called; acquisitions counter = 1

---

## Group 4 — `LeaderAwareScheduler` (`libs/leader`)

17. `LeaderAwareScheduler` interface — `void runIfLeader(LeaderTask task) throws Exception`.

18. `LeaderAwareSchedulerImpl` — injects `LeaderElectionService`; calls `isLeader()`:
    - If `true`: executes `task.execute()`; any exception propagates directly to the caller
    - If `false`: increments `leader.aware.scheduler.skipped` counter; returns without executing
    - Wraps the execution path with `@Timed(histogram=true, name="leader.aware.scheduler.execution")`

19. Register `LeaderAwareSchedulerImpl` in `LeaderElectionAutoConfiguration`.

20. Unit tests:
    - Task runs and returns when leader; no exception is suppressed
    - Task throws `IOException`; assert it propagates from `runIfLeader()`
    - Task throws `RuntimeException`; assert it propagates
    - `leader.aware.scheduler.skipped` counter = 1 when not leader; task never called
    - `leader.aware.scheduler.execution` timer registered in `SimpleMeterRegistry` after a leader execution

---

## Group 5 — `KafkaLagMonitor` (`apps/event-ingest`)

21. Add `implementation project(':libs:leader')` to `apps/event-ingest/build.gradle`.

22. `KafkaLagMonitorProperties` — `@ConfigurationProperties(prefix = "kafka.lag-monitor")`; fields: `enabled` (boolean), `intervalMs` (long, default 60000), `consumerGroupIds` (List<String>).

23. `KafkaLagMonitor` — Spring `@Service`; constructor-injects `AdminClient`, `LeaderAwareScheduler`, `MeterRegistry`, `KafkaLagMonitorProperties`.

24. `@Scheduled` method (virtual thread executor from Phase 2); body wrapped in `leaderAwareScheduler.runIfLeader(...)`:
    - `AdminClient.listConsumerGroupOffsets(groupId)` for each configured group
    - `AdminClient.listOffsets(topicPartitions, OffsetSpec.latest())`
    - lag per (group, topic, partition) = latestOffset − committedOffset
    - Upsert `kafka.consumer.lag` `Gauge` per tag tuple — Micrometer deduplicates by tag set

25. `@Timed(histogram=true, name="kafka.lag.monitor.check")` on the full scheduled method.

26. Unit tests (`KafkaLagMonitorTest`) — the primary assertions are behavioral, not metric-based (see Rules.md):
    - Mock `AdminClient`; `isLeader()` returns `false`; trigger scheduled method; assert zero `AdminClient` interactions
    - `isLeader()` returns `true`; mock returns 2 partitions for 1 group/topic; assert `AdminClient.listConsumerGroupOffsets()` called with the correct group ID; assert `AdminClient.listOffsets()` called with the correct partitions
    - Verify lag computation correctness: given latestOffset=100 and committedOffset=90, assert lag=10 is passed to the gauge builder
    - Default `intervalMs` binding test asserts `intervalMs == 60000`

27. Add to `apps/event-ingest/src/main/resources/application.yml`:
    ```yaml
    kafka:
      lag-monitor:
        enabled: true
        interval-ms: 60000
        consumer-group-ids:
          - event-ingest-group

    leader-election:
      lock-name: leader:event-ingest
      retry-interval-ms: 2000
      lock-watchdog-timeout-ms: 30000
    ```

---

## Group 6 — Integration Tests (`apps/event-ingest/src/itest`)

28. `LeaderElectionIT` — registers a `TestLeaderListener` bean (`@TestConfiguration`) that records `onLeader()` calls; starts application with Redis running; asserts:
    - `leaderElectionService.isLeader()` returns `true`
    - `leaderElectionService.getFencingToken()` returns a value `> 0`
    - `testLeaderListener.onLeaderCallCount()` equals 1
    - `testLeaderListener.lastFencingToken()` matches `leaderElectionService.getFencingToken()`

29. `KafkaLagMonitorIT` — overrides `kafka.lag-monitor.interval-ms=500` via `@TestPropertySource`; waits 600 ms for one firing; asserts:
    - `adminClientSpy.listConsumerGroupOffsetsInvocationCount()` ≥ 1 (spy or captured argument)
    - No exception thrown during the run

30. `LeaderElectionConnectionIT` — uses a `@TestConfiguration` to replace `RLock` with a spy that throws `RedisConnectionException` on the first `tryLock()` call, then delegates to a real mock that returns `true`; asserts:
    - Within 3 × `retryIntervalMs`, `leaderElectionService.isLeader()` becomes `true`
    - `leaderElectionService.getFencingToken()` > 0

---

## Group 7 — CI & Build Wiring

31. Confirm `jvmArgs '-Dnet.bytebuddy.experimental=true'` in root `build.gradle` covers the new `:libs:leader` subproject (verify `allprojects` or `subprojects` scope).

32. `libs/leader` has unit tests only; confirm absence of an `itest` source set does not break the `check.dependsOn itest` task wiring (add `if (sourceSets.findByName('itest'))` guard or equivalent).

33. Confirm `./gradlew build` passes end-to-end: all modules compile, all unit tests pass, `event-ingest` itest passes with Redis available.
