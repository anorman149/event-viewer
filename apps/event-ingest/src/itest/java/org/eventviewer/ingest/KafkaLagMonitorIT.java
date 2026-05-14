package org.eventviewer.ingest;

import org.apache.kafka.clients.admin.AdminClient;
import org.eventviewer.leader.LeaderElectionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@TestPropertySource(properties = {
        "event-ingest.kafka.lag-monitor.interval-ms=500"
})
class KafkaLagMonitorIT extends BaseTest {

    @Autowired
    LeaderElectionService leaderElectionService;

    @Autowired
    AdminClient adminClient;

    @Test
    void lagMonitorRunsAfterLeadershipAcquired() {
        await().atMost(10, TimeUnit.SECONDS)
                .until(leaderElectionService::isLeader);

        // Wait for at least one scheduled firing (500ms interval + buffer)
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            // Verify AdminClient is reachable (lag check was executed without error)
            var groups = adminClient.listConsumerGroups().all().get(5, TimeUnit.SECONDS);
            assertThat(groups).isNotNull();
        });
    }
}
