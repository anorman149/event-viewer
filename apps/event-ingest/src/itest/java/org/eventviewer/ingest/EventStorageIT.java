package org.eventviewer.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import org.eventviewer.api.ingest.KafkaEventMessage;
import org.eventviewer.ingest.domain.EventDocument;
import org.eventviewer.ingest.domain.RuleResult;
import org.eventviewer.ingest.domain.RuleStatus;
import org.eventviewer.ingest.service.IngestPipelineService;
import org.eventviewer.opensearch.OsAdminClient;
import org.eventviewer.opensearch.OsDocumentClient;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.generic.Requests;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class EventStorageIT extends BaseTest {

    @Autowired
    private IngestPipelineService ingestPipelineService;

    @Autowired
    private OsAdminClient osAdminClient;

    @Autowired
    private OsDocumentClient osDocumentClient;

    @Autowired
    private OpenSearchClient openSearchClient;

    @Test
    void fieldMappings_areCorrect() throws Exception {
        List<KafkaEventMessage> messages = buildMessages("order-created", 10);
        ingestPipelineService.process(messages);
        osAdminClient.refresh(EventDocument.class);

        JsonNode mappings = getJson("events_write/_mapping");
        JsonNode props = mappings
                .fields().next().getValue()
                .path("mappings").path("properties");

        assertThat(props.path("eventId").path("type").asText()).isEqualTo("keyword");
        assertThat(props.path("schemaType").path("type").asText()).isEqualTo("integer");
        assertThat(props.path("timestamp").path("type").asText()).isEqualTo("date");
        assertThat(props.path("s3FileName").path("type").asText()).isEqualTo("keyword");
        assertThat(props.path("podId").path("type").asText()).isEqualTo("keyword");
        assertThat(props.path("batchOffset").path("type").asText()).isEqualTo("long");
        assertThat(props.path("batchLength").path("type").asText()).isEqualTo("long");
        assertThat(props.path("ruleResults").path("type").asText()).isEqualTo("keyword");
    }

    @Test
    void writeAlias_routesToActiveIndex() throws Exception {
        List<KafkaEventMessage> messages = buildMessages("payment-processed", 5);
        ingestPipelineService.process(messages);
        osAdminClient.refresh(EventDocument.class);

        JsonNode aliases = getJson("_cat/aliases?format=json");
        boolean writeAliasFound = false;
        for (JsonNode alias : aliases) {
            if ("events_write".equals(alias.path("alias").asText())) {
                writeAliasFound = true;
                assertThat(alias.path("index").asText()).startsWith("events-");
                break;
            }
        }
        assertThat(writeAliasFound).as("events_write alias should exist").isTrue();
    }

    @Test
    void readAlias_returnsIndexedDocuments() throws Exception {
        List<KafkaEventMessage> messages = buildMessages("user-signup", 5);
        ingestPipelineService.process(messages);
        osAdminClient.refresh(EventDocument.class);

        JsonNode aliases = getJson("_cat/aliases?format=json");
        String writeIndex = null;
        String readIndex = null;
        for (JsonNode alias : aliases) {
            String aliasName = alias.path("alias").asText();
            if ("events_write".equals(aliasName)) writeIndex = alias.path("index").asText();
            if ("events_read".equals(aliasName))  readIndex  = alias.path("index").asText();
        }
        assertThat(writeIndex).as("events_write alias must exist").isNotNull();
        assertThat(readIndex).as("events_read alias must exist").isNotNull();
        assertThat(writeIndex).isEqualTo(readIndex);

        JsonNode searchResult = getJson("events_read/_search");
        assertThat(searchResult.path("hits").path("total").path("value").asInt()).isGreaterThan(0);
    }

    @Test
    void ilmPolicy_isRegistered() throws Exception {
        JsonNode policy = getJson("_ilm/policy/events-ilm-policy");
        assertThat(policy.has("events-ilm-policy")).isTrue();

        JsonNode phases = policy.path("events-ilm-policy").path("policy").path("phases");
        JsonNode hotRollover = phases.path("hot").path("actions").path("rollover");
        assertThat(hotRollover.path("max_size").asText()).isEqualTo("130gb");
        assertThat(hotRollover.path("max_age").asText()).isEqualTo("12h");
        assertThat(phases.has("warm")).isTrue();
        assertThat(phases.has("delete")).isTrue();
        assertThat(phases.path("delete").path("min_age").asText()).isEqualTo("4d");
    }

    @Test
    void ilmPolicy_isAttachedToIndex() throws Exception {
        JsonNode settings = getJson("events_write/_settings");
        JsonNode indexSettings = settings.fields().next().getValue()
                .path("settings").path("index");
        assertThat(indexSettings.path("lifecycle").path("name").asText()).isEqualTo("events-ilm-policy");
    }

    @Test
    void ruleResults_roundTrip_storedAsKeywordStrings() throws Exception {
        String eventId = UUID.randomUUID().toString();
        EventDocument doc = new EventDocument(
                eventId,
                "test-schema",
                Instant.now(),
                "550e8400-e29b-41d4-a716-446655440000.zst",
                "local-pod",
                0L,
                100,
                List.of(
                        new RuleResult("rule-abc", RuleStatus.SUCCESS),
                        new RuleResult("rule-xyz", RuleStatus.UNKNOWN)
                )
        );
        osDocumentClient.save(List.of(doc));
        osAdminClient.refresh(EventDocument.class);

        JsonNode hit = getJson("events_read/_doc/" + eventId);
        assertThat(hit.path("found").asBoolean()).isTrue();

        JsonNode ruleResults = hit.path("_source").path("ruleResults");
        assertThat(ruleResults.isArray()).isTrue();
        List<String> values = new ArrayList<>();
        ruleResults.forEach(n -> values.add(n.asText()));
        assertThat(values).containsExactlyInAnyOrder("rule-abc_1", "rule-xyz_0");
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private List<KafkaEventMessage> buildMessages(String schemaType, int count) {
        List<KafkaEventMessage> messages = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            messages.add(new KafkaEventMessage(
                    UUID.randomUUID(),
                    schemaType,
                    Instant.now(),
                    Instant.now(),
                    Map.of("index", i)));
        }
        return messages;
    }

    private JsonNode getJson(String path) throws Exception {
        try (var response = openSearchClient.generic().execute(
                Requests.builder()
                        .method("GET")
                        .endpoint("/" + path)
                        .build())) {
            String body = response.getBody()
                    .map(b -> {
                        try { return new String(b.body().readAllBytes()); }
                        catch (Exception e) { throw new RuntimeException(e); }
                    })
                    .orElse("{}");
            return objectMapper.readTree(body);
        }
    }
}
