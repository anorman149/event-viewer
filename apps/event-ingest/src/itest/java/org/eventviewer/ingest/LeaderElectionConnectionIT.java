package org.eventviewer.ingest;

import org.eventviewer.leader.LeaderElectionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@TestPropertySource(properties = {
        "leader-election.retry-interval-ms=200"
})
class LeaderElectionConnectionIT extends BaseTest {

    @Autowired
    LeaderElectionService leaderElectionService;

    @Test
    void eventuallyBecomesLeaderWithFastRetry() {
        await().atMost(10, TimeUnit.SECONDS)
                .until(leaderElectionService::isLeader);

        assertThat(leaderElectionService.isLeader()).isTrue();
        assertThat(leaderElectionService.getFencingToken()).isGreaterThan(0L);
    }
}
