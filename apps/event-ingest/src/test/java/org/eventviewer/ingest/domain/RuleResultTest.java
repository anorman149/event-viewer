package org.eventviewer.ingest.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RuleResultTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Test
    void serialize_success_producesCompositeKeyword() throws Exception {
        RuleResult result = new RuleResult("rule-abc", RuleStatus.SUCCESS);
        String json = objectMapper.writeValueAsString(result);
        assertThat(json).isEqualTo("\"rule-abc_1\"");
    }

    @Test
    void serialize_unknown_producesCompositeKeyword() throws Exception {
        RuleResult result = new RuleResult("rule-xyz", RuleStatus.UNKNOWN);
        String json = objectMapper.writeValueAsString(result);
        assertThat(json).isEqualTo("\"rule-xyz_0\"");
    }

    @Test
    void serialize_failure_producesCompositeKeyword() throws Exception {
        RuleResult result = new RuleResult("rule-def", RuleStatus.FAILURE);
        String json = objectMapper.writeValueAsString(result);
        assertThat(json).isEqualTo("\"rule-def_2\"");
    }

    @Test
    void ruleStatus_codes_areCorrect() {
        assertThat(RuleStatus.UNKNOWN.getCode()).isEqualTo(0);
        assertThat(RuleStatus.SUCCESS.getCode()).isEqualTo(1);
        assertThat(RuleStatus.FAILURE.getCode()).isEqualTo(2);
    }
}
