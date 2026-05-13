package org.eventviewer.ingest;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class KafkaTopicProvisioningIT {

    @Test
    void eventRawTopicExistsWith3Partitions() throws Exception {
        try (AdminClient adminClient = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"))) {

            Set<String> topics = adminClient.listTopics().names().get(10, TimeUnit.SECONDS);
            assertThat(topics).contains("event-raw", "event-raw-dlt");

            Map<String, TopicDescription> descriptions = adminClient
                    .describeTopics(List.of("event-raw", "event-raw-dlt"))
                    .allTopicNames()
                    .get(10, TimeUnit.SECONDS);

            assertThat(descriptions.get("event-raw").partitions()).hasSize(3);
            assertThat(descriptions.get("event-raw-dlt").partitions()).hasSize(1);
        }
    }
}
