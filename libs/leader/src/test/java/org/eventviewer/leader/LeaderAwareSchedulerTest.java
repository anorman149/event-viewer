package org.eventviewer.leader;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LeaderAwareSchedulerTest {

    @Mock LeaderElectionService leaderElectionService;

    SimpleMeterRegistry registry;
    LeaderAwareScheduler scheduler;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        scheduler = new LeaderAwareSchedulerImpl(leaderElectionService, registry);
    }

    @Test
    void taskRunsWhenLeader() throws Exception {
        when(leaderElectionService.isLeader()).thenReturn(true);
        int[] callCount = {0};

        scheduler.runIfLeader(() -> callCount[0]++);

        assertThat(callCount[0]).isEqualTo(1);
    }

    @Test
    void taskNotCalledWhenNotLeader() throws Exception {
        when(leaderElectionService.isLeader()).thenReturn(false);
        int[] callCount = {0};

        scheduler.runIfLeader(() -> callCount[0]++);

        assertThat(callCount[0]).isEqualTo(0);
        assertThat(registry.counter("leader.aware.scheduler.skipped").count()).isEqualTo(1.0);
    }

    @Test
    void checkedExceptionPropagates() {
        when(leaderElectionService.isLeader()).thenReturn(true);

        assertThatThrownBy(() -> scheduler.runIfLeader(() -> { throw new IOException("disk error"); }))
                .isInstanceOf(IOException.class)
                .hasMessage("disk error");
    }

    @Test
    void uncheckedExceptionPropagates() {
        when(leaderElectionService.isLeader()).thenReturn(true);

        assertThatThrownBy(() -> scheduler.runIfLeader(() -> { throw new IllegalStateException("boom"); }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("boom");
    }

    @Test
    void executionTimerRecordedOnLeaderRun() throws Exception {
        when(leaderElectionService.isLeader()).thenReturn(true);

        scheduler.runIfLeader(() -> {});

        assertThat(registry.find("leader.aware.scheduler.execution").timer()).isNotNull();
        assertThat(registry.find("leader.aware.scheduler.execution").timer().count()).isEqualTo(1);
    }

    @Test
    void executionTimerRecordedEvenOnException() {
        when(leaderElectionService.isLeader()).thenReturn(true);

        try {
            scheduler.runIfLeader(() -> { throw new RuntimeException(); });
        } catch (Exception ignored) {}

        assertThat(registry.find("leader.aware.scheduler.execution").timer().count()).isEqualTo(1);
    }

    @Test
    void skippedCounterNotIncrementedOnLeaderRun() throws Exception {
        when(leaderElectionService.isLeader()).thenReturn(true);

        scheduler.runIfLeader(() -> {});

        assertThat(registry.counter("leader.aware.scheduler.skipped").count()).isZero();
    }
}
