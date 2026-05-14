package org.eventviewer.leader;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class RedissonLeaderElectionService implements LeaderElectionService, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(RedissonLeaderElectionService.class);

    private final RedissonClient redissonClient;
    private final RedisLeaderElectionProperties properties;
    private final List<LeaderListener> listeners;
    private final Counter acquisitionsCounter;
    private final Counter relinquishmentsCounter;
    private final Counter connectionLossesCounter;
    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    private final AtomicLong fencingToken = new AtomicLong(-1L);
    private final ExecutorService electionExecutor;
    private volatile boolean shuttingDown = false;
    private volatile RLock lock;
    private volatile RAtomicLong fencingCounter;

    public RedissonLeaderElectionService(
            RedissonClient redissonClient,
            RedisLeaderElectionProperties properties,
            List<LeaderListener> listeners,
            MeterRegistry meterRegistry) {
        this.redissonClient = redissonClient;
        this.properties = properties;
        this.listeners = listeners;
        this.electionExecutor = Executors.newSingleThreadExecutor(
                Thread.ofVirtual().name("leader-election-loop").factory());

        acquisitionsCounter = Counter.builder("leader.election.acquisitions")
                .description("Number of times leadership was acquired")
                .register(meterRegistry);
        relinquishmentsCounter = Counter.builder("leader.election.relinquishments")
                .description("Number of times leadership was relinquished")
                .register(meterRegistry);
        connectionLossesCounter = Counter.builder("leader.election.connection.losses")
                .description("Number of times leadership was lost due to a connection failure")
                .register(meterRegistry);
        Gauge.builder("leader.election.is.leader", isLeader, v -> v.get() ? 1.0 : 0.0)
                .description("1.0 when this instance is the leader, 0.0 otherwise")
                .register(meterRegistry);
        Gauge.builder("leader.election.fencing.token", fencingToken, AtomicLong::doubleValue)
                .description("Current fencing token; -1 when not leader")
                .register(meterRegistry);
    }

    @PostConstruct
    void startElectionLoop() {
        electionExecutor.submit(this::runElectionLoop);
    }

    private void runElectionLoop() {
        while (!Thread.currentThread().isInterrupted() && !shuttingDown) {
            try {
                lock = redissonClient.getLock(properties.getLockName());
                fencingCounter = redissonClient.getAtomicLong(properties.getLockName() + ":fence");

                boolean acquired = lock.tryLock(0, -1, TimeUnit.SECONDS);
                if (acquired) {
                    long token = fencingCounter.incrementAndGet();
                    fencingToken.set(token);
                    isLeader.set(true);
                    acquisitionsCounter.increment();
                    log.info("Acquired leadership (fencing token={})", token);
                    listeners.forEach(LeaderListener::onLeader);

                    while (lock.isHeldByCurrentThread()
                            && !Thread.currentThread().isInterrupted()
                            && !shuttingDown) {
                        Thread.sleep(properties.getRetryIntervalMs() / 2);
                    }

                    long lostToken = fencingToken.get();
                    boolean hadLock = lock.isHeldByCurrentThread();
                    isLeader.set(false);
                    fencingToken.set(-1L);
                    relinquishmentsCounter.increment();
                    if (!hadLock && !shuttingDown) {
                        log.warn("Lost leadership due to connection failure (fencing token={})", lostToken);
                        connectionLossesCounter.increment();
                    } else {
                        log.info("Relinquished leadership (fencing token={})", lostToken);
                    }
                    if (hadLock) {
                        lock.unlock();
                    }
                    listeners.forEach(LeaderListener::onLeaderLoss);
                } else if (!shuttingDown) {
                    Thread.sleep(properties.getRetryIntervalMs());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                cleanupAfterInterrupt();
            } catch (Exception e) {
                log.warn("Leader election error, retrying in {}ms: {}", properties.getRetryIntervalMs(), e.getMessage());
                try {
                    Thread.sleep(properties.getRetryIntervalMs());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    cleanupAfterInterrupt();
                }
            }
        }
    }

    private void cleanupAfterInterrupt() {
        if (isLeader.compareAndSet(true, false)) {
            fencingToken.set(-1L);
            relinquishmentsCounter.increment();
            if (lock != null && lock.isHeldByCurrentThread()) {
                try {
                    lock.unlock();
                } catch (Exception e) {
                    log.warn("Failed to unlock during interrupt cleanup: {}", e.getMessage());
                }
            }
            listeners.forEach(LeaderListener::onLeaderLoss);
        }
    }

    @Override
    public boolean isLeader() {
        return isLeader.get();
    }

    @Override
    public long getFencingToken() {
        return fencingToken.get();
    }

    @Override
    public void destroy() throws InterruptedException {
        shuttingDown = true;
        electionExecutor.shutdownNow();
        boolean terminated = electionExecutor.awaitTermination(5, TimeUnit.SECONDS);
        if (!terminated && isLeader.compareAndSet(true, false)) {
            log.warn("Election loop did not terminate in 5s; forcing lock release");
            long token = fencingToken.getAndSet(-1L);
            relinquishmentsCounter.increment();
            try {
                if (lock != null) lock.forceUnlock();
            } catch (Exception e) {
                log.warn("Force unlock failed during destroy: {}", e.getMessage());
            }
            listeners.forEach(LeaderListener::onLeaderLoss);
        }
    }
}
