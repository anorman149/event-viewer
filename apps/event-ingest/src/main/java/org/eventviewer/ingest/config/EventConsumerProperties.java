package org.eventviewer.ingest.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "event-ingest.consumer")
public record EventConsumerProperties(
        String podName,
        int podCount
) {
    public EventConsumerProperties {
        if (podName == null || podName.isBlank()) podName = "local-pod";
        if (podCount <= 0) podCount = 1;
    }
}
