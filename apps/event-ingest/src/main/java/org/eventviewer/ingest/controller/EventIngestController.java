package org.eventviewer.ingest.controller;

import io.micrometer.core.annotation.Timed;
import org.eventviewer.api.ingest.IngestRequest;
import org.eventviewer.api.ingest.IngestResponse;
import org.eventviewer.ingest.service.EventIngestService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/event/v1")
public class EventIngestController {

    private final EventIngestService eventIngestService;

    public EventIngestController(EventIngestService eventIngestService) {
        this.eventIngestService = eventIngestService;
    }

    @PostMapping("/events")
    @Timed(value = "event.ingest.controller.ingest", description = "Time to handle POST /event/v1/events", histogram = true)
    public ResponseEntity<IngestResponse> ingest(@Validated @RequestBody IngestRequest request) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(eventIngestService.ingest(request));
    }
}
