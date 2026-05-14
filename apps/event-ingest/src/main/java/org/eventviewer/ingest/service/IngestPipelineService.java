package org.eventviewer.ingest.service;

import io.micrometer.core.annotation.Timed;
import org.eventviewer.api.ingest.KafkaEventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class IngestPipelineService {

    private static final Logger log = LoggerFactory.getLogger(IngestPipelineService.class);

    @Timed(value = "event.ingest.pipeline.process",
           description = "Time to process a batch of events through the ingest pipeline",
           histogram = true)
    public void process(List<KafkaEventMessage> messages) {
        // Stub: Phases 5 and 6 will add S3 write and OpenSearch indexing
        log.debug("Processing batch of {} events", messages.size());
    }
}
