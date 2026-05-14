package org.eventviewer.ingest;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaTopicProvisioningIT extends BaseTest {

    private static final List<String> MAIN_TOPICS = List.of(
            "event-raw-1", "event-raw-2", "event-raw-3", "event-raw-4");

    private static final List<String> DLT_TOPICS = List.of(
            "event-raw-1.DLT", "event-raw-2.DLT", "event-raw-3.DLT", "event-raw-4.DLT");

    @Test
    void allShardedTopicsExistWithCorrectPartitionCount() throws Exception {
        try (AdminClient adminClient = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"))) {

            Set<String> topics = adminClient.listTopics().names().get(10, TimeUnit.SECONDS);
            assertThat(topics).containsAll(MAIN_TOPICS);
            assertThat(topics).containsAll(DLT_TOPICS);

            List<String> allTopics = new java.util.ArrayList<>(MAIN_TOPICS);
            allTopics.addAll(DLT_TOPICS);

            Map<String, TopicDescription> descriptions = adminClient
                    .describeTopics(allTopics)
                    .allTopicNames()
                    .get(10, TimeUnit.SECONDS);

            for (String mainTopic : MAIN_TOPICS) {
                assertThat(descriptions.get(mainTopic).partitions())
                        .as("Topic %s should have 2 partitions", mainTopic)
                        .hasSize(2);
            }
        }
    }
}
