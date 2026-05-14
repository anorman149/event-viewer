package org.eventviewer.ingest;

import org.eventviewer.ingest.support.RecordingLeaderListener;
import org.eventviewer.leader.LeaderElectionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class LeaderElectionIT extends BaseTest {

    @Autowired
    LeaderElectionService leaderElectionService;

    @Autowired
    RecordingLeaderListener recordingLeaderListener;

    @Test
    void serviceBecomesLeaderOnStartup() {
        await().atMost(10, TimeUnit.SECONDS)
                .until(leaderElectionService::isLeader);

        assertThat(leaderElectionService.isLeader()).isTrue();
    }

    @Test
    void fencingTokenIsPositiveWhenLeader() {
        await().atMost(10, TimeUnit.SECONDS)
                .until(leaderElectionService::isLeader);

        assertThat(leaderElectionService.getFencingToken()).isGreaterThan(0L);
    }

    @Test
    void listenerNotifiedOnLeadership() {
        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> recordingLeaderListener.onLeaderCallCount() >= 1);

        assertThat(recordingLeaderListener.onLeaderCallCount()).isGreaterThanOrEqualTo(1);
    }
}
