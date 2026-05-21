package org.eventviewer.leader;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RedissonLeaderElectionServiceTest {

    @Mock RedissonClient redissonClient;
    @Mock RLock lock;
    @Mock RAtomicLong fencingCounter;

    SimpleMeterRegistry registry;
    RecordingLeaderListener listener;
    RedisLeaderElectionProperties properties;
    RedissonLeaderElectionService service;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        listener = new RecordingLeaderListener();
        properties = new RedisLeaderElectionProperties();
        properties.setRetryIntervalMs(50L);
        properties.setLockWatchdogTimeoutMs(30000L);

        lenient().when(redissonClient.getLock(properties.getLockName())).thenReturn(lock);
        lenient().when(redissonClient.getAtomicLong(properties.getLockName() + ":fence")).thenReturn(fencingCounter);
        lenient().when(fencingCounter.incrementAndGet()).thenReturn(1L);

        service = new RedissonLeaderElectionService(
                redissonClient, properties, List.of(listener), registry);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        service.destroy();
    }

    @Test
    void isLeaderFalseBeforeAcquisition() throws Exception {
        when(lock.tryLock(0, -1, TimeUnit.SECONDS)).thenReturn(false);
        service.startElectionLoop();
        Thread.sleep(200);

        assertThat(service.isLeader()).isFalse();
        assertThat(service.getFencingToken()).isEqualTo(-1L);
    }

    @Test
    void isLeaderTrueAfterAcquisition() throws Exception {
        when(lock.tryLock(0, -1, TimeUnit.SECONDS)).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);
        service.startElectionLoop();

        await().atMost(5, TimeUnit.SECONDS).until(service::isLeader);

        assertThat(service.isLeader()).isTrue();
        assertThat(service.getFencingToken()).isEqualTo(1L);
        assertThat(listener.onLeaderCallCount()).isEqualTo(1);
        assertThat(registry.counter("leader.election.acquisitions").count()).isEqualTo(1.0);
    }

    @Test
    void onLeaderLossCalledWhenLockLostUnexpectedly() throws Exception {
        when(lock.tryLock(0, -1, TimeUnit.SECONDS)).thenReturn(true).thenReturn(false);
        when(lock.isHeldByCurrentThread())
                .thenReturn(true)
                .thenReturn(false);
        service.startElectionLoop();

        await().atMost(5, TimeUnit.SECONDS).until(() -> listener.onLeaderLossCallCount() > 0);

        assertThat(service.isLeader()).isFalse();
        assertThat(service.getFencingToken()).isEqualTo(-1L);
        assertThat(listener.onLeaderLossCallCount()).isGreaterThanOrEqualTo(1);
        assertThat(registry.counter("leader.election.relinquishments").count()).isGreaterThanOrEqualTo(1.0);
        assertThat(registry.counter("leader.election.connection.losses").count()).isGreaterThanOrEqualTo(1.0);
    }

    @Test
    void destroyUnlocksWhenLeader() throws Exception {
        when(lock.tryLock(0, -1, TimeUnit.SECONDS)).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);
        service.startElectionLoop();

        await().atMost(5, TimeUnit.SECONDS).until(service::isLeader);

        service.destroy();

        assertThat(service.isLeader()).isFalse();
        assertThat(listener.onLeaderLossCallCount()).isGreaterThanOrEqualTo(1);
        assertThat(registry.counter("leader.election.relinquishments").count()).isGreaterThanOrEqualTo(1.0);
    }

    @Test
    void destroyDoesNotUnlockWhenNotLeader() throws Exception {
        when(lock.tryLock(0, -1, TimeUnit.SECONDS)).thenReturn(false);
        service.startElectionLoop();
        Thread.sleep(200);

        service.destroy();

        verify(lock, never()).unlock();
        verify(lock, never()).forceUnlock();
        assertThat(listener.onLeaderLossCallCount()).isZero();
    }

    @Test
    void retriesAfterFailedAcquisition() throws Exception {
        when(lock.tryLock(0, -1, TimeUnit.SECONDS))
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);
        service.startElectionLoop();

        await().atMost(5, TimeUnit.SECONDS).until(service::isLeader);

        assertThat(listener.onLeaderCallCount()).isEqualTo(1);
    }

    @Test
    void fencingTokenIncreasesOnReacquisition() throws Exception {
        when(fencingCounter.incrementAndGet()).thenReturn(1L).thenReturn(2L);
        when(lock.tryLock(0, -1, TimeUnit.SECONDS)).thenReturn(true);
        when(lock.isHeldByCurrentThread())
                .thenReturn(true)
                .thenReturn(false)
                .thenReturn(true);
        service.startElectionLoop();

        await().atMost(5, TimeUnit.SECONDS).until(() -> listener.onLeaderCallCount() >= 2);

        assertThat(service.getFencingToken()).isEqualTo(2L);
    }

    @Test
    void loopHandlesConnectionExceptionWithRetry() throws Exception {
        when(lock.tryLock(0, -1, TimeUnit.SECONDS))
                .thenThrow(new RuntimeException("Redis connection refused"))
                .thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);
        service.startElectionLoop();

        await().atMost(5, TimeUnit.SECONDS).until(service::isLeader);

        assertThat(listener.onLeaderCallCount()).isEqualTo(1);
    }

    @Test
    void lockObtainedOnElectionThreadNotInPostConstruct() throws Exception {
        when(lock.tryLock(anyLong(), anyLong(), any())).thenAnswer(inv -> {
            Thread.sleep(60_000);
            return false;
        });

        long start = System.currentTimeMillis();
        service.startElectionLoop();
        long elapsed = System.currentTimeMillis() - start;

        // getLock/getAtomicLong must NOT have been called yet — they happen on the election thread
        assertThat(elapsed).isLessThan(500);
    }

    @Test
    void allMetersRegisteredOnConstruction() {
        assertThat(registry.find("leader.election.acquisitions").counter()).isNotNull();
        assertThat(registry.find("leader.election.relinquishments").counter()).isNotNull();
        assertThat(registry.find("leader.election.connection.losses").counter()).isNotNull();
        assertThat(registry.find("leader.election.is.leader").gauge()).isNotNull();
        assertThat(registry.find("leader.election.fencing.token").gauge()).isNotNull();
    }

    static class RecordingLeaderListener implements LeaderListener {
        private final AtomicInteger onLeaderCalls = new AtomicInteger(0);
        private final AtomicInteger onLeaderLossCalls = new AtomicInteger(0);

        @Override
        public void onElected() { onLeaderCalls.incrementAndGet(); }

        @Override
        public void onRevoked() { onLeaderLossCalls.incrementAndGet(); }

        int onLeaderCallCount() { return onLeaderCalls.get(); }
        int onLeaderLossCallCount() { return onLeaderLossCalls.get(); }
    }
}
