package org.eventviewer.ingest.support;

import org.eventviewer.leader.LeaderListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Component
public class RecordingLeaderListener implements LeaderListener {

    private final AtomicInteger onLeaderCalls = new AtomicInteger(0);
    private final AtomicInteger onLeaderLossCalls = new AtomicInteger(0);

    @Override
    public void onElected() {
        onLeaderCalls.incrementAndGet();
    }

    @Override
    public void onRevoked() {
        onLeaderLossCalls.incrementAndGet();
    }

    public int onLeaderCallCount() { return onLeaderCalls.get(); }
    public int onLeaderLossCallCount() { return onLeaderLossCalls.get(); }
}
