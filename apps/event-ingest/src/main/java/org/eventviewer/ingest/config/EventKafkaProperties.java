package org.eventviewer.ingest.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "event-ingest.kafka")
public record EventKafkaProperties(List<TopicDefinition> topics, LagMonitor lagMonitor) {

    public EventKafkaProperties {
        if (topics == null) topics = List.of();
        if (lagMonitor == null) lagMonitor = new LagMonitor(false, 60_000L, List.of());
    }

    public record TopicDefinition(
            String name,
            int partitions,
            int replicationFactor,
            DeadLetter deadLetter
    ) {
        public record DeadLetter(String name, int partitions, int replicationFactor) {}
    }

    public record LagMonitor(boolean enabled, long intervalMs, List<String> consumerGroupIds) {
        public LagMonitor {
            if (consumerGroupIds == null) consumerGroupIds = List.of();
            if (intervalMs <= 0) intervalMs = 60_000L;
        }
    }
}
