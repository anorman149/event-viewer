package org.eventviewer.ingest.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eventviewer.api.ingest.KafkaEventMessage;
import org.eventviewer.ingest.kafka.AbstractBatchMessageListener;
import org.eventviewer.ingest.service.IngestPipelineService;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class DltBatchMessageListener extends AbstractBatchMessageListener {
    private final IngestPipelineService ingestPipelineService;

    public DltBatchMessageListener(ObjectMapper objectMapper,
                                    IngestPipelineService ingestPipelineService,
                                    MeterRegistry meterRegistry) {
        super(objectMapper, meterRegistry, "dlt.parse.failures");
        this.ingestPipelineService = ingestPipelineService;
    }

    @Override
    public void onMessage(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        List<KafkaEventMessage> messages = parseRecords(records);
        if (!messages.isEmpty()) {
            ingestPipelineService.process(messages);
        }
        acknowledgment.acknowledge();
    }
}
