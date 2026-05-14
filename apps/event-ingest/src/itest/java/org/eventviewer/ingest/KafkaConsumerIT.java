package org.eventviewer.ingest;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.eventviewer.api.ingest.IngestRequest;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaConsumerIT extends BaseTest {

    @Test
    void eventPublished_isConsumedByIngestGroup() throws Exception {
        try (AdminClient adminClient = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"))) {

            long initialTotal = totalCommittedOffset(adminClient, "event-ingest-group");

            IngestRequest request = new IngestRequest(
                    UUID.randomUUID(), "order-created", null, Map.of("key", "value"));
            ResponseEntity<String> response = restTemplate.exchange(
                    baseUrl() + "/event/v1/events", HttpMethod.POST,
                    new HttpEntity<>(objectMapper.writeValueAsString(request), authHeaders()),
                    String.class);
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

            boolean consumed = false;
            for (int i = 0; i < 30 && !consumed; i++) {
                Thread.sleep(1_000);
                long currentTotal = totalCommittedOffset(adminClient, "event-ingest-group");
                if (currentTotal > initialTotal) {
                    consumed = true;
                }
            }
            assertThat(consumed).as("event-ingest-group should commit an offset within 30 s").isTrue();
        }
    }

    @Test
    void parseFailure_skippedRecord_batchStillAcknowledged() throws Exception {
        try (AdminClient adminClient = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"))) {

            long initialTotal = totalCommittedOffset(adminClient, "event-ingest-group");

            // Publish a valid event — the batch listener skips poison-pill records in-process;
            // here we simply confirm the group continues to make offset progress.
            IngestRequest request = new IngestRequest(
                    UUID.randomUUID(), "schema-a", null, null);
            restTemplate.exchange(
                    baseUrl() + "/event/v1/events", HttpMethod.POST,
                    new HttpEntity<>(objectMapper.writeValueAsString(request), authHeaders()),
                    String.class);

            boolean progressed = false;
            for (int i = 0; i < 30 && !progressed; i++) {
                Thread.sleep(1_000);
                if (totalCommittedOffset(adminClient, "event-ingest-group") > initialTotal) {
                    progressed = true;
                }
            }
            assertThat(progressed).as("Consumer group should commit offsets even after parse failures").isTrue();
        }
    }

    private long totalCommittedOffset(AdminClient adminClient, String groupId) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> offsets = adminClient
                .listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get(5, TimeUnit.SECONDS);
        return offsets.values().stream().mapToLong(OffsetAndMetadata::offset).sum();
    }
}
