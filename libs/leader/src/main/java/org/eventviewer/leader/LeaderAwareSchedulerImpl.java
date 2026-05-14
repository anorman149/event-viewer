package org.eventviewer.leader;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.concurrent.TimeUnit;

public class LeaderAwareSchedulerImpl implements LeaderAwareScheduler {

    private final LeaderElectionService leaderElectionService;
    private final Counter skippedCounter;
    private final Timer executionTimer;

    public LeaderAwareSchedulerImpl(LeaderElectionService leaderElectionService, MeterRegistry meterRegistry) {
        this.leaderElectionService = leaderElectionService;
        this.skippedCounter = Counter.builder("leader.aware.scheduler.skipped")
                .description("Number of tasks skipped because this instance is not the leader")
                .register(meterRegistry);
        this.executionTimer = Timer.builder("leader.aware.scheduler.execution")
                .description("Execution time of tasks run as leader")
                .publishPercentileHistogram(true)
                .register(meterRegistry);
    }

    @Override
    public void runIfLeader(LeaderTask task) throws Exception {
        if (!leaderElectionService.isLeader()) {
            skippedCounter.increment();
            return;
        }
        long start = System.nanoTime();
        try {
            task.execute();
        } finally {
            executionTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }
}
