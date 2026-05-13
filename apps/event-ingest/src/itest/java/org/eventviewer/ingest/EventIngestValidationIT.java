package org.eventviewer.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eventviewer.api.ingest.IngestRequest;
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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class EventIngestValidationIT {

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

    private ResponseEntity<String> post(Object body) throws Exception {
        return restTemplate.exchange(
                "http://localhost:" + port + "/event/v1/events",
                HttpMethod.POST,
                new HttpEntity<>(objectMapper.writeValueAsString(body), authHeaders()),
                String.class);
    }

    @Test
    void missingEventId_returns400() throws Exception {
        // Serialize IngestRequest with null eventId — non_null config omits the null field from JSON
        IngestRequest request = new IngestRequest(null, "order-created", null, null);
        assertThat(post(request).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void missingSchemaType_returns400() throws Exception {
        // Serialize IngestRequest with null schemaType — omitted from JSON
        IngestRequest request = new IngestRequest(UUID.randomUUID(), null, null, null);
        assertThat(post(request).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void nonUuidEventId_returns400() throws Exception {
        // Use a plain map so we can supply a non-UUID string for event_id
        Map<String, Object> body = new HashMap<>();
        body.put("event_id", "not-a-uuid");
        body.put("schema_type", "test");
        assertThat(post(body).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void emptyBody_returns400() throws Exception {
        assertThat(post(Map.of()).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
