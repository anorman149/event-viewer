package org.eventviewer.ingest.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eventviewer.api.ingest.KafkaEventMessage;
import org.eventviewer.ingest.service.IngestPipelineService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EventBatchListenerTest {

    @Mock
    IngestPipelineService ingestPipelineService;

    @Mock
    Acknowledgment acknowledgment;

    private ObjectMapper objectMapper;
    private SimpleMeterRegistry meterRegistry;
    private EventBatchListener listener;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        meterRegistry = new SimpleMeterRegistry();
        listener = new EventBatchListener(objectMapper, ingestPipelineService, meterRegistry);
    }

    @Test
    void validBatch_parsesAllAndDelegatesAsList() throws Exception {
        KafkaEventMessage msg1 = new KafkaEventMessage(UUID.randomUUID(), "order-created", Instant.now(), Instant.now(), null);
        KafkaEventMessage msg2 = new KafkaEventMessage(UUID.randomUUID(), "order-shipped", Instant.now(), Instant.now(), null);

        listener.onMessage(
                List.of(record(objectMapper.writeValueAsString(msg1)),
                        record(objectMapper.writeValueAsString(msg2))),
                acknowledgment);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<KafkaEventMessage>> captor = ArgumentCaptor.forClass(List.class);
        verify(ingestPipelineService).process(captor.capture());
        assertThat(captor.getValue()).hasSize(2);
        verify(acknowledgment).acknowledge();
        assertThat(parseFailures()).isZero();
    }

    @Test
    void invalidJson_metersParseFailure_skipsRecord_acknowledges() {
        listener.onMessage(List.of(record("not-valid-json")), acknowledgment);

        verifyNoInteractions(ingestPipelineService);
        verify(acknowledgment).acknowledge();
        assertThat(parseFailures()).isEqualTo(1.0);
    }

    @Test
    void mixedBatch_sendOnlyValidRecordsToService_acknowledgesOnce() throws Exception {
        KafkaEventMessage msg = new KafkaEventMessage(UUID.randomUUID(), "order-created", Instant.now(), Instant.now(), null);

        listener.onMessage(
                List.of(record(objectMapper.writeValueAsString(msg)), record("bad-json")),
                acknowledgment);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<KafkaEventMessage>> captor = ArgumentCaptor.forClass(List.class);
        verify(ingestPipelineService).process(captor.capture());
        assertThat(captor.getValue()).hasSize(1);
        verify(acknowledgment, times(1)).acknowledge();
        assertThat(parseFailures()).isEqualTo(1.0);
    }

    @Test
    void allInvalidBatch_doesNotCallService_acknowledges() {
        listener.onMessage(
                List.of(record("bad1"), record("bad2")),
                acknowledgment);

        verify(ingestPipelineService, never()).process(anyList());
        verify(acknowledgment).acknowledge();
        assertThat(parseFailures()).isEqualTo(2.0);
    }

    private ConsumerRecord<String, String> record(String value) {
        return new ConsumerRecord<>("event-raw-1", 0, 0L, "key", value);
    }

    private double parseFailures() {
        return meterRegistry.counter("kafka.consumer.parse.failures").count();
    }
}
