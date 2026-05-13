package org.eventviewer.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eventviewer.ingest.support.TestJwtFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class BaseTest {
    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    ObjectMapper objectMapper;

    protected HttpHeaders authHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(TestJwtFactory.generateJwt("test-user"));
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }

    protected String baseUrl() {
        return "http://localhost:" + port;
    }
}
