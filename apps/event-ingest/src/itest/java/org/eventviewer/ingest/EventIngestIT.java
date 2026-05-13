package org.eventviewer.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eventviewer.api.ingest.IngestRequest;
import org.eventviewer.api.ingest.IngestResponse;
import org.eventviewer.api.ingest.KafkaEventMessage;
import org.eventviewer.ingest.support.TestJwtFactory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class EventIngestIT {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    ObjectMapper objectMapper;

    private HttpHeaders authHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(TestJwtFactory.generateJwt("test-user"));
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }

    private String url() {
        return "http://localhost:" + port + "/event/v1/events";
    }

    private KafkaConsumer<String, String> newConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    @Test
    void happyPath_returns202_andMessageLandsOnKafka() throws Exception {
        IngestRequest request = new IngestRequest(
                UUID.randomUUID(), "order-created", null, Map.of("order_id", "ORD-1", "amount", 49.99));

        ResponseEntity<IngestResponse> response = restTemplate.exchange(
                url(), HttpMethod.POST,
                new HttpEntity<>(objectMapper.writeValueAsString(request), authHeaders()),
                IngestResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody().eventId()).isEqualTo(request.eventId());
        assertThat(response.getBody().ingestTs()).isNotNull();

        try (KafkaConsumer<String, String> consumer = newConsumer("itest-happy-" + UUID.randomUUID())) {
            consumer.subscribe(List.of("event-raw"));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            assertThat(records).isNotEmpty();
            var record = records.iterator().next();
            assertThat(record.key()).isEqualTo(request.eventId().toString());

            KafkaEventMessage message = objectMapper.readValue(record.value(), KafkaEventMessage.class);
            assertThat(message.schemaType()).isEqualTo("order-created");
            assertThat(message.ingestTs()).isNotNull();
        }
    }

    @Test
    void missingTimestamp_defaultsToIngestTime() throws Exception {
        IngestRequest request = new IngestRequest(UUID.randomUUID(), "ping", null, null);
        Instant before = Instant.now();

        ResponseEntity<IngestResponse> response = restTemplate.exchange(
                url(), HttpMethod.POST,
                new HttpEntity<>(objectMapper.writeValueAsString(request), authHeaders()),
                IngestResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody().ingestTs()).isBetween(before, before.plusSeconds(2));
    }

    @Test
    void unauthenticatedRequest_returns401() throws Exception {
        IngestRequest request = new IngestRequest(UUID.randomUUID(), "test", null, null);
        HttpHeaders noAuth = new HttpHeaders();
        noAuth.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<String> response = restTemplate.exchange(
                url(), HttpMethod.POST,
                new HttpEntity<>(objectMapper.writeValueAsString(request), noAuth),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }
}
