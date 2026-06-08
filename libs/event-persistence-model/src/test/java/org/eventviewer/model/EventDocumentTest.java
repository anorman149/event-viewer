package org.eventviewer.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EventDocumentTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Test
    void nullRuleResults_normalizesToEmptyList() {
        EventDocument doc = new EventDocument(
                "evt-1", 42, Instant.now(), "550e8400.zst", "local-pod", 0L, 100, null);
        assertThat(doc.ruleResults()).isNotNull();
        assertThat(doc.ruleResults()).isEmpty();
    }

    @Test
    void emptyRuleResults_absentFromJson() throws Exception {
        EventDocument doc = new EventDocument(
                "evt-1", 42, Instant.now(), "550e8400.zst", "local-pod", 0L, 100, List.of());

        JsonNode json = objectMapper.readTree(objectMapper.writeValueAsString(doc));

        assertThat(json.has("ruleResults")).isFalse();
    }

    @Test
    void nonEmptyRuleResults_presentInJson() throws Exception {
        EventDocument doc = new EventDocument(
                "evt-1", 42, Instant.now(), "550e8400.zst", "local-pod", 0L, 100,
                List.of("rule-abc_1"));

        JsonNode json = objectMapper.readTree(objectMapper.writeValueAsString(doc));

        assertThat(json.has("ruleResults")).isTrue();
        assertThat(json.get("ruleResults").get(0).asText()).isEqualTo("rule-abc_1");
    }
}
